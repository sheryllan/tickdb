#!/usr/bin/env python

# -*- coding: utf-8 -*-
import string, datetime, os, sys, argparse, logging, multiprocessing
import xml.etree.ElementTree as ET
import Datamole_2_EOBI as EOBI
import Datamole_2_ETI as ETI
import cPickle as pickle
import mimetypes as mim


def setlog(log_level):
	log = logging.getLogger()
	log.setLevel(log_level)
	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(log_level)
	formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
	ch.setFormatter(formatter)
	log.addHandler(ch)
	return log

# Read template parameters to help decoding lines
# interfaces.xml has id to templates values
# example, field 1 (from 0) has value 33, the <dataflow> tag tells us it's an EOBI 13100 order add message
# templates.xml gives signification of fields depending on their id
# example <template id="eobi_13100_ord_add"> has each field in a <detail> tag in the right order
# This function returns id_to_template which does the first transformation (using interfaces.xml) and template_to_columns, which does the 2nd transformation (using templates.xml)
def parseTemplates(path):
	id_to_template={}
	template_to_columns={}
	interfaces = ET.parse(os.path.join(path,'interfaces.xml'))
	templates = ET.parse(os.path.join(path,'templates.xml'))
	# Get all of the template ids
	for child in interfaces.getroot().iter('dataflow'):
		id_to_template[child.attrib['id']]= child.attrib['decode']
	 
	# get all of the elements of the template 
	for child in templates.getroot().iter('template'):
	# XXX next line should always be true if the XML file is good and
	# XXX the template id should never appear twice in the XML file. Strange!
		if child.attrib['id'] not in template_to_columns:
			template_to_columns[child.attrib['id']]=[]
		for detail in child.findall("detail"):
			template_to_columns[child.attrib['id']].append(detail.attrib['field'])
	return id_to_template,template_to_columns

# ====================
#	Line decode
# ====================
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
		# Values are supposed to appear in the CSV file in the same
		# order as the <detail> tags in the XML file
			for i,key in enumerate( self.template_to_columns[self.name]):
				self.values[key]=sl[4+i]
		except:
			self.__log.fatal(line)
				   
def runEOBIdata(path,eobi_path,eobi_snap_path,log_level,levels):		
	# Set up logging
	log=setlog(log_level)		
	#Parse XML Templates		
	id_to_template,template_to_columns=parseTemplates(path)
		
	# Get EOBI files list
	files=os.listdir(eobi_path)
	snap_files=os.listdir(eobi_snap_path)
	# we only want .csv files
	files = [x for x in sorted(files) if mim.guess_type(x)[0]=='text/csv']
	snap_files = [x for x in sorted(snap_files) if mim.guess_type(x)[0]=='text/csv']
	dir_date=os.path.split(os.path.split(eobi_path)[0])[1] # date we want to process
	log.info("Reading {0} files and {1} snap files".format(len(files),len(snap_files)))

	# Read all the files in one pass
	EOBI_ticks=[] # ticks after "Datamole" parsing
	lines=[] # ticks as strings from their respective files
	for file in files:
		# check the file date is the same as the dir date
		file_date=datetime.datetime.fromtimestamp(
				float(file.split('-')[0])).date().strftime("%Y%m%d")
		# read and store all the file
		if file_date==dir_date:
			with open(os.path.join(eobi_path,file)) as o:
				lines.extend(o.readlines())
	log.info("Day has {0} ticks to process".format(len(lines)))

	# Parse string ticks into Datamole ticks
	i=0
	for line in lines:
		EOBI_ticks.append(Datamole(line,id_to_template,template_to_columns,log))
		i+=1
		if i%100000==0:
			log.info("processed {0} lines".format(i))
	del lines # free up memory
	log.info("Datamole decoded {0} lines into ticks".format(len(EOBI_ticks)))
	
	# Create EOBI object
	eobi = EOBI.EOBI(levels,log)

	# step 1: find first message sequence number for all symbols
	eobi.findFirstMsgseqnum(EOBI_ticks)
	log.info("Done with first msg seq num")

	# step 2: find missing message sequence number
	eobi.parseForMissingSeqnum(EOBI_ticks)
	log.info("Done with missing seq num")

	# step 3: find first snapshot with msgseq >= first msgseq
	snap_lines=[]
	snap_EOBI_ticks=[]
	for file in snap_files:
		snap_date=datetime.datetime.fromtimestamp(
				float(file.split('-')[0])).date().strftime("%Y%m%d")
		if snap_date==dir_date:
			with open(os.path.join(eobi_snap_path,file)) as o:
				snap_lines.extend(o.readlines())
	log.info("Snap day has {0} ticks to process".format(len(snap_lines)))

	i=0
	for line in snap_lines:
		snap_EOBI_ticks.append(Datamole(line,id_to_template,template_to_columns,log))
		i+=1
		if i%100000==0:
			log.info("processed {0} lines from snap file".format(i))
	del snap_lines
	eobi.computeSnapshotData(EOBI_ticks)
	sys.exit("Bye")

	# step 4:generate books for each product
	for tick in EOBI_ticks:
		if 'secid' in tick.values:
			uid = tick.values['secid']
			eobi.calcEOBI(EOBI_ticks,uid,log)

	# Finally, write outputs to HDF5 files
	eobi.bookData.output_to_HDF5()


# ====================
#	 Main program 
# ====================
def main():
	parser = argparse.ArgumentParser(description="Parse a Datamole csv file.")
	parser.add_argument("--levels","-l",help="max Order Book levels",type=int,default=5)
#	parser.add_argument("--odir","-o",help="output directory",type=str,default=".")
	parser.add_argument("workingDirectory",help="The directory of csvs to parse",type=str)
	args = parser.parse_args()

	# Logs
	#log_level=logging.DEBUG
	log_level=logging.INFO
	#log_level=logging.WARN

	# Run decoding
	path=os.path.realpath(args.workingDirectory)
	eobi_path=os.path.join(path,'eobi')
	eobi_snap_path=os.path.join(path,'eobi')
	runEOBIdata(path,eobi_path,eobi_snap_path,log_level,args.levels) 
 
if __name__=="__main__":
	print(sys.version)
	main()
