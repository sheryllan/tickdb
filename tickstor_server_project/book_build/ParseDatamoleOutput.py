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

             
class Datamole:
  def __init__(self):
        self.name=""
        self.timestamp=""
        self.interface=-1
        self.id=-1
        self.values={}           

# Set up logging
log = logging.getLogger()
log.setLevel(log_level)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch) 
        
#Parse XML Templates        
interfaces = ET.parse(os.path.join(path,'interfaces.xml'))
templates = ET.parse(os.path.join(path,'templates.xml'))

id_to_template={}
template_to_columns={}

#Get all of the teplate ids
for child in interfaces.getroot().iter('dataflow'):
    id_to_template[child.attrib['id']]= child.attrib['decode']
 
#get all of the elements of the template 
for child in templates.getroot().iter('template'):
    if child.attrib['id'] not in template_to_columns:
        template_to_columns[child.attrib['id']]=[]
    for detail in child.findall("detail"):
        template_to_columns[child.attrib['id']].append(detail.attrib['field'])
        
    

#for each file on a given date   # or is there only 1 file?  This keeps changing.
#Also this was coded because the file contained EMDI/EOBI/ETI... this may have changed as well.
# What about A and B?  Do I need to merge 2 files?

##############################
# EOBI
##############################
files=os.listdir(eobi_path)
snap_files=os.listdir(eobi_snap_path)
files.sort()
snap_files.sort()
bookData=None

########################################################################
#    HOW TO PARSE.....
#    1) Cycle through and find the first msgseq num for each product in the incremental
#    2) Run through the data and find all of the sequence gaps
#    3) Cycle through the snapshots and find the snapshot that is >= first msgseq num for each product and generate an initial book for each product  if we start a 0, then there is no need for a snapshot
#    4) Cycle through the incrementals and generate the rest of the book.
########################################################################

def makeTick(line):
    sl=line.strip('\n').split(',')
    tick=Datamole()
    tick.interface=sl[0]
    tick.id=sl[1]
    #tick.seqnum=sl[2]
    tick.timestamp=sl[3].replace('.','') #arista timestamp
    tick.name=id_to_template[tick.id]
    for i,key in enumerate( template_to_columns[tick.name]):
        tick.values[key]=sl[4+i]
    return tick

eobi=None
# Step 1
for file in files:
    with open(os.path.join(eobi_path,file)) as o:
        EOBI_ticks=[]
        for line in o:
            tick=makeTick(line)
            EOBI_ticks.append(tick)
        date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
        log.info("{0}: {1}, {2}".format("Parse files for incremental starts",datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
        if eobi==None:
            eobi=EOBI.EOBI(5,date_string,log)
        eobi.findFirstMsgseqnum(date_string,EOBI_ticks)
        
print eobi.bookData.product_sequence_numbers       
# Step 2
for file in files:
    with open(os.path.join(eobi_path,file)) as o:
        EOBI_ticks=[]
        for line in o:
            tick=makeTick(line)
            EOBI_ticks.append(tick)
        date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
        log.info("{0}: {1}, {2}".format("Parse files for sequence gaps",datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
        if eobi==None:
            eobi=EOBI.EOBI(5,date_string,log)
        eobi.parseForMissingSeqnum(date_string,EOBI_ticks)
# Step 3... this could run in parallel with Step 2       
for file in snap_files:
    with open(os.path.join(eobi_snap_path,file)) as o:
        EOBI_ticks=[]
        for line in o:
            tick=makeTick(line)
            EOBI_ticks.append(tick)
        date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
        log.info("{0}: {1}, {2}".format("Parse files for building snapshots",datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
        eobi.computeSnapshotData(date_string,EOBI_ticks)  

print eobi.bookData.product_sequence_numbers
print eobi.snapshot_products        
#Step 4            
for file in files:
    with open(os.path.join(eobi_path,file)) as o:
        EOBI_ticks=[]
        for line in o:
            tick=makeTick(line)
            EOBI_ticks.append(tick)
        date_string=datetime.datetime.fromtimestamp(float(file.split('-')[0])).date().strftime("%Y%m%d")
        log.info("{0}: {1}, {2}".format("Parse files for book build",datetime.datetime.fromtimestamp(float(file.split('-')[0])), file))
        eobi.calcEOBI(date_string,EOBI_ticks)


eobi.bookData.output_to_HDF5()

#ETI.ETI(date_string).calcETI(ETI_ticks)
 
#now i have 2 choices.... split by eobi/eti/emdi and then by security id... or vice-versa.

#talk to Julien and David and Pual/driscoll.  
  
 #since i have them split i then need to call the code to build the books..  get what david has for eobi...incrementals?
 
 
 