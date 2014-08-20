# -*- coding: utf-8 -*-
import string
import datetime
import os
import argparse
import xml.etree.ElementTree as ET
import Datamole_2_EOBI as EOBI
import Datamole_2_ETI as ETI

parser = argparse.ArgumentParser(description='Parse a Datamole csv file.')
parser.add_argument('workingDirectory', help='The directory of csvs to parse')
args = parser.parse_args()

os.chdir(args.workingDirectory)
             
class Datamole:
  def __init__(self):
        self.name=""
        self.timestamp=""
        self.interface=-1
        self.id=-1
        self.values={}           
 
interfaces = ET.parse('interfaces.xml')
templates = ET.parse('templates.xml')

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
        
    
EOBI_ticks=[]        #TODO... split by interface or by symbol?
EMDI_ticks=[]        #TODO... split by interface or by symbol?
ETI_ticks=[]        #TODO... split by interface or by symbol?
EOBI_test=[]

#for each file on a given date   # or is there only 1 file?  This keeps changing.
#Also this was coded because the file contained EMDI/EOBI/ETI... this may have changed as well.
# What about A and B?  Do I need to merge 2 files?
with open("1404198000-1404198300.csv") as o:
    for line in o:
        sl=line.split(',')
        tick=Datamole()
        tick.interface=sl[0]
        tick.id=sl[1]
        #tick.seqnum=sl[2]
        tick.timestamp=sl[3].replace('.','') #arista timestamp
        tick.name=id_to_template[tick.id]
        for i,key in enumerate( template_to_columns[tick.name]):
            tick.values[key]=sl[4+i]
        #TEST:  TODO Remove
        if tick.name=='eobi_13100_ord_add':
            EOBI_test.append(tick)
            
        if tick.name[0:4]=='eobi':
            EOBI_ticks.append(tick)
        elif tick.name[0:3]=='eti':
            ETI_ticks.append(tick)
        elif tick.name=='Eurex-EMDI':
            EMDI_ticks.append(tick)

date_string=datetime.datetime.fromtimestamp(float('1404198000')).date().strftime("%Y%m%d")
bookData = EOBI.EOBI(5,date_string).calcEOBI(EOBI_ticks)
#end for
bookData.output_to_HDF5()

ETI.ETI(date_string).calcETI(ETI_ticks)
 
#now i have 2 choices.... split by eobi/eti/emdi and then by security id... or vice-versa.

#talk to Julien and David and Pual/driscoll.  
  
 #since i have them split i then need to call the code to build the books..  get what david has for eobi...incrementals?
 
 
 