# -*- coding: utf-8 -*-
import string
import datetime
import os
import sys
import argparse
import logging
import xml.etree.ElementTree as ET
import Datamole_2_EOBI as EOBI
import Datamole_2_ETI as ETI
import cPickle as pickle
import multiprocessing

def setlog(log_level):
    log = logging.getLogger()
    log.setLevel(log_level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log
    
def parseTemplates(path):
    id_to_template={}
    template_to_columns={}
    interfaces = ET.parse(os.path.join(path,'interfaces.xml'))
    templates = ET.parse(os.path.join(path,'templates.xml'))
    #Get all of the teplate ids
    for child in interfaces.getroot().iter('dataflow'):
        id_to_template[child.attrib['id']]= child.attrib['decode']
     
    #get all of the elements of the template 
    for child in templates.getroot().iter('template'):
        if child.attrib['id'] not in template_to_columns:
            template_to_columns[child.attrib['id']]=[]
        for detail in child.findall("detail"):
            template_to_columns[child.attrib['id']].append(detail.attrib['field'])
    return id_to_template,template_to_columns

class Datamole:
    def __init__(self,line,id_to_template,template_to_columns,log=None):
        if log is None:
            log=setlog()
        self.__log=log
        self.name=""
        self.timestamp=""
        self.interface=-1
        self.id=-1
        self.values={}
        self.id_to_template=id_to_template
        self.template_to_columns=template_to_columns
        self.__makeTick(line)
        
    def __makeTick(self,line):
        try:
            sl=line.strip('\n').split(',')
            self.interface=sl[0]
            self.id=sl[1]
            #tick.seqnum=sl[2]
            self.timestamp=sl[3].replace('.','') #arista timestamp
            self.name=self.id_to_template[self.id]
            for i,key in enumerate( self.template_to_columns[self.name]):
                self.values[key]=sl[4+i]
        except:
            self.__log.fatal(line)
                   
def run_product(files,path, eobi_path, uid_to_run,log_level):
    log=setlog(log_level)
    id_to_template,template_to_columns=parseTemplates(path)
    eobi=pickle.load(open(os.path.join(eobi_path,'eobi.pickle'),'r'))
    for file in files:
        with open(os.path.join(eobi_path,file)) as o:
            EOBI_ticks=[]
            for line in o:
                tick=Datamole(line,id_to_template,template_to_columns,log)
                if tick.values.has_key('secid'):
                    uid=tick.values['secid']
                    if uid==uid_to_run:
                        EOBI_ticks.append(tick)
            date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
            log.info("{0} {1}: {2}, {3}".format("Parse files for book build for product", uid_to_run,datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
            eobi.calcEOBI(date_string,EOBI_ticks,uid_to_run,log)    
    eobi.bookData.output_to_HDF5()        

def runEOBIdata(path,eobi_path,eobi_snap_path,log_level):        
    # Set up logging
    log=setlog(log_level)        
    #Parse XML Templates        
    id_to_template,template_to_columns=parseTemplates(path)
            
        
    #for each file on a given date   # or is there only 1 file?  This keeps changing.
    #Also this was coded because the file contained EMDI/EOBI/ETI... this may have changed as well.
    # What about A and B?  Do I need to merge 2 files?
    ##############################
    # EOBI Files
    ##############################
    files=os.listdir(eobi_path)
    snap_files=os.listdir(eobi_snap_path)
    files.sort()
    snap_files.sort()
    #we only want .csv files
    for file in files:
        if file[-4:]!=".csv":
            files.remove(file)
    for file in snap_files:
        if file[-4:]!=".csv":
            snap_files.remove(file)

    ########################################################################
    #    HOW TO PARSE.....
    #    1) Cycle through and find the first msgseq num for each product in the incremental
    #    2) Run through the data and find all of the sequence gaps
    #    3) Cycle through the snapshots and find the snapshot that is >= first msgseq num for each product and generate an initial book for each product  if we start a 0, then there is no need for a snapshot
    #    4) Cycle through the incrementals and generate the rest of the book.
    #######################################################################

        
    eobi=None
    # Step 1
    for file in files:
        with open(os.path.join(eobi_path,file)) as o:
            EOBI_ticks=[]
            for line in o:
                tick=Datamole(line,id_to_template,template_to_columns,log)
                EOBI_ticks.append(tick)
            date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
            log.info("{0}: {1}, {2}".format("Parse files for incremental starts",datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
            if eobi==None:
                eobi=EOBI.EOBI(5,date_string,log)
            eobi.findFirstMsgseqnum(date_string,EOBI_ticks)
            
    #print eobi.bookData.product_sequence_numbers       
    #pickle.dump(eobi,open(os.path.join(eobi_path,'eobi.pickle'),'w'))
        
    # Step 2
    for file in files:
        with open(os.path.join(eobi_path,file)) as o:
            EOBI_ticks=[]
            for line in o:
                tick=Datamole(line,id_to_template,template_to_columns,log)
                EOBI_ticks.append(tick)
            date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
            log.info("{0}: {1}, {2}".format("Parse files for sequence gaps",datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
            if eobi==None:
                eobi=EOBI.EOBI(5,date_string,log)
            eobi.parseForMissingSeqnum(date_string,EOBI_ticks)
            
    #pickle.dump(eobi,open(os.path.join(eobi_path,'eobi.pickle'),'w'))        
    # Step 3... this could run in parallel with Step 2       
    for file in snap_files:
        with open(os.path.join(eobi_snap_path,file)) as o:
            EOBI_ticks=[]
            for line in o:
                tick=Datamole(line,id_to_template,template_to_columns,log)
                EOBI_ticks.append(tick)
            date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
            log.info("{0}: {1}, {2}".format("Parse files for building snapshots",datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
            eobi.computeSnapshotData(date_string,EOBI_ticks)  

    #print eobi.bookData.product_sequence_numbers
    #print eobi.snapshot_products 
           
    #Step 4  Run in parallel
    pickle.dump(eobi,open(os.path.join(eobi_path,'eobi.pickle'),'w'))
        
    procs=[]
    for uid in eobi.snapshot_products.keys():
        p = multiprocessing.Process(target=run_product, args=(files,path,eobi_path,uid,log_level))
        p.start()
        procs.append(p)
        
    while len(procs)>0:
        if procs[0].is_alive():
            procs[0].join()
        procs.pop(0)
        
    os.remove(os.path.join(eobi_path,'eobi.pickle')) 

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Parse a Datamole csv file.')
    parser.add_argument('workingDirectory', help='The directory of csvs to parse')
    args = parser.parse_args()

    os.chdir(args.workingDirectory)
    path=os.path.realpath(os.path.curdir)
    eobi_path=os.path.join(path,'eobi')
    eobi_snap_path=os.path.join(path,'eobi')
    #log_level=logging.DEBUG
    log_level=logging.INFO
    #log_level=logging.WARN
    runEOBIdata(path,eobi_path,eobi_snap_path,log_level) 
 
