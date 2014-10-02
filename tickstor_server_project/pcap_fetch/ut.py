# Helper functions, logging, etc...
# vim: expandtab ts=4 ai

__version__ = (0,0,2)

import subprocess as sp
from time import time,sleep
import sys, logging, os, ctypes
from config import LOGFILE,raw_pcap_path
import multiprocessing as mp
from math import floor

class massZip:
    def __init__(self,logginginstance):
        self.errorQ = mp.Queue()
        self.out = logginginstance
    
    def bz(self,x,cmd,overwrite=False):
        self.out.pout("Proc Spawn >>> " + x)
        unzipped_file = x.rstrip(".bz2")
        if x == None: 
            self.pwarn("WARNING: Got Nonetype value when calling b(un)zip function. Command: %s" % cmd)
            return 1
        if os.path.exists(unzipped_file):
            if overwrite == False:
                self.out.pwarn("File exists and overwrite set to False, skipping file %s"  % x)
                return 1
            else:
                self.out.pwarn("File exists and overwrite set to True, deleting %s and continuing" % unzipped_file)
                os.unlink(unzipped_file) #unlink old pcap file, continuing
                
        rc = os.system("/usr/bin/bunzip2 %s" % x)
        if rc != 0:
            self.out.perr("Could not unzip file: %s, got return code %s" % (x,rc))
            self.errorQ.put("bunzip ERROR: %s" % x)

    def execute(self,directory,ziptype,overwrite=False):
        ''' does what it says on the tin. Given a dir it walks it and bunzips it all in parallel '''
        if ziptype == "bunzip":
            files = os.popen("find %s -type f -name \"*.bz2\"" % directory).readlines()
            self.out.pout("We are batch bunzipping %d files" % len(files) )
            cmd = "/usr/bin/bunzip2" 
        elif ziptype == "bzip":
            files = os.popen("find %s -type f -not -name \"*.bz2\"" % path).readlines()
            self.out.pout("We are batch bunzipping %d files" % len(files))
            cmd = "/usr/bin/bzip2"
        else:
            self.out.perr("Sorry, type '%s' unrecognized, failing" % ziptype)
            raise AttributeError("ziptype '%s' not recognised" % ziptype)

        files = map( lambda x: x.strip(), files)

        failures = []
        running_procs = []

        while len(files) != 0:
            while (len(running_procs ) < mp.cpu_count() ):
                try: p = mp.Process(target=self.bz, args=(files.pop(),cmd,overwrite))
                except IndexError: break
                p.start()
                running_procs.append(p)

            sleep(0.5)
            running_procs = filter(lambda x: x.is_alive() == True, running_procs) #cleanup
        for p in running_procs:
            p.join() #Wait for all to finish

        while self.errorQ.empty() == False:
            failures.append(self.errorQ.get())
        #serial write to log file (expensive, due to mass syscalls, but we are not caring here)
        if len(failures) != 0:
            map(lambda x: self.out.perr(x), failures)
        return failures

    def bunzipdir(self,x,overwrite=False):
        return self.execute(x,"bunzip",overwrite)

    def bzipdir(self,directory,overwrite=False):
        return self.execute(x,"bzip",overwrite)


class output:
    def __init__(self,logfile=None,loglevel=logging.DEBUG):
        self.logfile = logfile
        self.mt = "%s: %s"
        if logfile != None:
            logging.basicConfig(filename=LOGFILE, level=loglevel)

    def pout(self,text):
        ''' Write text to stdout. Use instead of print (as print is a function in python3)  '''     
        text = self.mt % (time(),text)
        sys.stdout.write(text+"\n")
        logging.info(text)

    def pwarn(self,text):
        text = self.mt % (time(),text)
        sys.stderr.write(text+"\n")
        logging.warning(text)

    def perr(self,text):
        ''' Write text to stderr, if log != None, append to log. Not multiprocess safe! '''
        text = self.mt % (time(),text)
        sys.stderr.write(text+"\n")
        logging.error(text)

def sync():
    libc = ctypes.CDLL("libc.so.6")
    return libc.sync()

def fetchrawPCAPhosts():
    return os.listdir(raw_pcap_path)

def fetchPCAPdaySlots(host):
    return os.listdir( os.path.join(raw_pcap_path,host) )

def call(cmd, *args):
    # Execute command specified, and return stdout
    cmd = [cmd]
    if type(args) == list or type(args) == tuple:
        cmd.extend(args)
    else:
        cmd.extend([str(args)])
    pipe = sp.Popen(  cmd ,  shell=False, bufsize=65535, stdout=sp.PIPE, close_fds=True)
    return pipe.stdout.readlines()

def system(cmd, *args):
    cmd = [cmd]
    if type(args) == list or type(args) == tuple:
        cmd.extend(args)
    else:
        cmd.extend([ ' '.join(args)])

    rc = sp.call( cmd, shell=False )
    if rc != 0:
        print "Error, got return code %d" % rc
        print "Ran the following command:\n%s" % cmd
    # According to POSIX, return codes are unsigned 8 bit. Python seems to use 16-bit return codes.
    # Therefore,  a return code which is modulo 256 will cause overflow,
    # and as such  a non-zero return code will be passed back as a 0. The below
    # floor() makes sure that any error > 256 is returned as 255. We lose some error info, but better than
    # returning 0 inadvertantly. This may be because of how uint was defined on the 64bit system cPython was built on.
    if rc > 255: rc=255
    return rc


class flexidict(dict):
    def __init__(self,*args):
        dict.__init__(self,args)
    def __setitem__(self,key,val):
        try:dict.__setitem__(self,key,val)
        except KeyError:dict.update(self,{key:value})

class pflexidict(flexidict):
    def __init__(self,*args):
        flexidict.__init__(self,*args)
    def __setitem__(self,key,val):
        try:
            if type( flexidict.__getitem__(self,key)) == list:
                items = flexidict.__getitem__(self,key)
                items.append(val)
                val = items
            else:
                val = []
            flexidict.__setitem__(self,key,val)
        except KeyError:
            flexidict.__setitem__(self,key,[val])
            
            

