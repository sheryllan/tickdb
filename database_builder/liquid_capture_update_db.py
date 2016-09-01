#!/usr/bin/env python3.5

import time,os,argparse,sys,json,re,subprocess,csv,fnmatch,pandas,lzma,re,io
import datetime
from joblib import Parallel, delayed
import pwd,grp

""" This program generates and updates the TickDB from the data captured by the Liquid servers
The design has been simplified at the cost of reading directories recursively. It is however
more resilient than the previous version and can fix more problems when the data are corrupted
so that the user doesn't have to intervene (and the user is me.... !)

So I traded off speed for simplicity. But given the speed of the server itself, it doesn't make
a bit difference in CPU and workload.
"""

def ensure_i_am_alone():
    """ prevent running 2 instances of the update_db program """
    pid = str(os.getpid())
    pidfile = "/tmp/mypid.pid"
    while os.path.exists(pidfile):
        time.sleep(20)
    pidfile = open("/tmp/mypid.pid",'w')
    pidfile.write(pid)

def run_compression(f): # run compression job on file f
    os.system('xz -9 '+f)

def compression_job(config_file):
    """ Wait for all new files to be copied, then compress them with xz """
    def find_files(rawdata_dir): # find all files in the raw data repository
        raw_files=[]
        for root,dirs,files in os.walk(rawdata_dir):
            for f in files:
                if f.endswith(".csv"): # Reactor writes uncompressed csv files
                    raw_files.append(os.path.join(root,f))
        return raw_files

    # Dummy way to see if the files can be compressed: check if their size is not growing anymore
    # for x seconds. The writer always finishes its job at some point. When all the files are
    # ready the function returns
    def monitor_raw_files(raw_files,delay=5):
        fsize={}
        no_growing_files = 0
        for f in raw_files: # get initial sizes
            fsize[f] = os.stat(f).st_size

        while no_growing_files < len(raw_files):
            time.sleep(delay)
            for f in raw_files:
                s = os.stat(f).st_size
                if fsize[f] != s:
                    fsize[f] = s
                else:
                    no_growing_files = no_growing_files + 1

    # run compression job
    if not os.path.isfile(config_file):
        return False

    with open(config_file) as cfg_file:
        cfg=json.load(cfg_file) # get json config file
        rawdata_dir = cfg['liquid_capture']['src_dir']
        new_files = find_files(rawdata_dir) # get list of .csv files
        monitor_raw_files(new_files,8) # check all files are uploaded
        print("Compressing ",len(new_files)," files")
        results = Parallel(n_jobs=-1)(delayed(run_compression)(f) for f in new_files)# run // jobs

def copy_liquid_capture_to_db(config_file):
    """ Find new files from the raw data of liquid_capture and process them into the DB """
    def find_compressed_files(rawdata_dir):
        """ get all the raw data compressed files """
        raw_files=[]
        for root,dirs,files in os.walk(rawdata_dir):
            for f in files:
                if f.endswith(".csv.xz") and 'Reference' not in f:
                    raw_files.append(os.path.join(root,f))
        return raw_files

    def find_db_files(dbdir):
        """ get all the files from the database """
        db_files = []
        for root,dirs,files in os.walk(dbdir):
            for f in files:
                if f.endswith(".csv.bz2"):
                    db_files.append(os.path.join(root,f))
        return db_files

    def create_dict_of_files(raw_files,db_files,prefix,dbdir):
        """ create a dictionary with key = target db file and value =
        list of raw data files to process """
        D = {} # build dict of new files to process
        for f in raw_files:
            x=os.path.basename(f).split('.')[0] # remove extensions
            for p in prefix: # remove any "exchange" prefix
                x=x.replace(p,'')
            ts=x.split('-')[-1]
            month= str(int(x.split('-')[-2])//100) # get month
            new_file=dbdir+'/'+month+'/'+x
            new_file=new_file.replace('-'+ts,'.csv.bz2')
            if new_file not in db_files: # check in file exists in db
                if new_file not in D: # if not, add the raw file to the list
                    D[new_file] = [f] # of files to process (because the same
                else: # instrument can have many files)
                    D[new_file].append(f)

        for k in D: # sort each sub list to be sure we concat files chronologically
            D[k].sort()

        return D

    def create_recompress_job_list(D):
        """ make a list of bash command to recompress everything """
        cmd=[]
        for k in D:
            s="( xz -cd "+D[k].pop(0)+";" # first file of the list
            for x in D[k]:
                s=s+"xz -cd "+x+"|tail -n +2;" # remove first line of other files
            s=s+") | bzip2 -9c > " + k
            cmd.append(s)

        return cmd

    def create_missing_dirs(D,uid,gid):
        """ create missing directories for target files """
        for d in list(set([os.path.dirname(k) for k in D])): # extract unique target dir names
            if not os.path.exists(d):
                os.makedirs(d,mode=0o750) # create dir with mode 750
                os.chown(d,uid,gid) # change owner and group

    # main program of this function
    with open(config_file) as cfg_file:
        cfg=json.load(cfg_file) # get json config file
        rawdata_dir = cfg['liquid_capture']['src_dir']
        dbdir =       cfg['liquid_capture']['dbdir']
        prefix =      cfg['liquid_capture']['prefix']
        owner =       cfg['liquid_capture']['owner']
        group =       cfg['liquid_capture']['group']

        uid = pwd.getpwnam(owner).pw_uid
        gid = grp.getgrnam(group).gr_gid

        raw_files = find_compressed_files(rawdata_dir) # find all files in raw data
        db_files = find_db_files(dbdir) # find all files in DB
        D = create_dict_of_files(raw_files,db_files,prefix,dbdir) # make a diff a generate new files to process
        job = create_recompress_job_list(D) # make a shell script
        create_missing_dirs(D,uid,gid)
        print("Running ",len(job)," jobs")
        results = Parallel(n_jobs=-1)(delayed(os.system)(f) for f in job)# run // jobs
        for k in D: # finally change owners of each new file
            os.chown(k,uid,gid)

def generate_reference_data(config_file):
    """ generate reference data main file from all the reference files """
    def find_files(rawdata_dir): # find all files in the raw data repository
        raw_files=[]
        for root,dirs,files in os.walk(rawdata_dir):
            for f in files:
                if fnmatch.fnmatch(f,'*Ref*'): # look for Reference data files
                    raw_files.append(os.path.join(root,f))
        return raw_files

    def load_and_merge_ref_files(raw_files, target_file):
        header=['ProductID','Product','Type','Exchange','Currency','Underlying','ExpiryDate',
                'Strike','PutOrCall','ExerciseStyle','MinPriceIncrement','MinPriceIncrementAmount',
                'SecurityDesc','PremiumDecimalPlace','SecurityID','UnderlyingSecurityID',
                'MarketSegmentID','MarketSegment','DestinationExchange']
        decopt = re.compile("([0-9]),([0-9]+\\.[CP])")
        ref = [] # resulting data.frame (as a list so far)
        for f in raw_files:
            # decompress the file first
            if f.endswith('.xz'):
                the_file = lzma.open(f,'rt',encoding='ISO-8859-1')
            else:
                the_file = open(f,'rt',encoding='ISO-8859-1')

            # read data and filter out problems
            # filter out all sort of problems
            l = [line.replace('\n','') for line in the_file] # read and remove all CR
            l = [line.rstrip(',')         for line in l] # remove comma at the end of each lines
            l = [re.sub(',",', ',,',line) for line in l] # remove a double-quote alone in between comma

            for c in ['\x00','\x01','\x02','\x04','\x0b','\x0c','\x10',
                    '\x14','\x15','\x16','\x17','\x18','\x1d','\x1e','\x1f',
                    '\xfe','`',u'\u0097',u'\u0098']:
                l = [line.replace(c,'') for line in l] # remove bad characters
            l = [line.replace(',&,',',,') for line in l]# remove & 
            l = [line.replace(',NA,',',,') for line in l]# remove NA to be more generic
            l[0] = l[0].replace(' ','') # remove spaces in header

            # check for product name with a , in place of the decimal point for the strike
            # if they're not surrounded by quotes, then we add them
            for i in range(len(l)):
                if decopt.search(l[i]):
                    l[i]=decopt.sub('\\1_\\2',l[i])

            # make a long string again and read it into a data.frame
            x = pandas.read_csv(io.StringIO('\n'.join(l)))
            # add empty missing_col
            for missing_col in [w for w in header if w not in x.columns]:
                x[missing_col]=pandas.Series('')
            # reduce x to the columns we want (in header) and concat with ref
            ref.append(x[header])
        ref=pandas.concat(ref) # make a big data.frame
        ref=ref.drop_duplicates() # remove duplicate lines
        ref.to_csv(target_file,na_rep='NA',columns=header,header=header,index=False)

    # main program
    with open(config_file) as cfg_file:
        cfg=json.load(cfg_file) # get json config file
        rawdata_dir = cfg['liquid_capture']['src_dir']
        dbdir =       cfg['liquid_capture']['dbdir']
        target_file = cfg['liquid_capture']['instdb']
        raw_files = find_files(rawdata_dir) # find all files in raw data
        load_and_merge_ref_files(raw_files,target_file) # create a single data.frame of all the ref data files

if __name__=="__main__":
    # Get arguments or print usage
    if(len(sys.argv)<2):
        print(sys.argv[0]," <config file>")
        sys.exit(1)
    config_file = sys.argv[1]
    # Test json conf file first
    if not os.path.isfile(config_file):
        print("Error: json config file",config_file," does not exist",file=sys.stderr)
        sys.exit(1)

    ensure_i_am_alone() # wait for previous scripts to finish
    #compression_job(config_file) # Run compression jobs
    copy_liquid_capture_to_db(config_file) # update the db with the new files
    #generate_reference_data(config_file) # update the reference data files

    os.unlink("/tmp/mypid.pid")
