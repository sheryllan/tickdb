#!/usr/bin/env python3

import string, datetime, os, sys, argparse, logging, multiprocessing, pickle
import xml.etree.ElementTree as ET
import mimetypes as mim
import Datamole_2_EOBI as EOBI
import Datamole_2_ETI as ETI
import numpy as np
import pandas as pd

template_to_columns = {}
id_to_template = {}

# =============================
#	Datamole  decoder
# =============================
class Datamole:
	def __init__(self,line):
		self.name=""
		self.timestamp=""
		self.interface=-1
		self.id=-1
		self.values={}
		
		try:
			sl=line.strip('\n').split(',')
			self.interface=sl[0] 
			self.id=sl[1]
			#tick.seqnum=sl[2]
			self.timestamp=sl[3].replace('.','') #arista timestamp
			self.name=id_to_template[self.id]
			# Values are supposed to appear in the CSV file in the same
			# order as the <detail> tags in the XML file
			for i,key in enumerate(template_to_columns[self.name]):
				self.values[key]=sl[4+i]
			self.valid = True
		except:
			self.valid = False
	
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
# This function returns id_to_template which does the first transformation (using interfaces.xml) and template_to_columns,
# which does the 2nd transformation (using templates.xml)
# 
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
		if child.attrib['id'] not in template_to_columns:
			template_to_columns[child.attrib['id']]=[]
		for detail in child.findall("detail"):
			template_to_columns[child.attrib['id']].append(detail.attrib['field'])
	return id_to_template,template_to_columns

# ====================
#     Main program 
# ====================
def main():
	# Parse command line arguments
	parser = argparse.ArgumentParser(description="Parse a Datamole csv file.")
	parser.add_argument("--levels","-l",help="max Order Book levels",type=int,default=5)
	parser.add_argument("--odir","-o",help="output directory",type=str,default="")
	parser.add_argument("workingDirectory",help="The directory of csvs to parse",type=str)
	args = parser.parse_args()

	# Set up logging
	log_level=logging.INFO
	log=setlog(log_level)		

	# Run decoding
	path=os.path.realpath(args.workingDirectory)
	eobi_path=os.path.join(path,'eobi')
	levels = args.levels

	#Parse XML Templates		
	global id_to_template, template_to_columns
	id_to_template,template_to_columns=parseTemplates(path)
		
	# Get EOBI files list
	files=os.listdir(eobi_path)
	# we only want .csv files
	files = [x for x in sorted(files) if mim.guess_type(x)[0]=='text/csv']
	# dates we want to process
	dir_date=os.path.split(os.path.split(eobi_path)[0])[1]

	# Read all the files in one pass
	log.info("Reading {0} files".format(len(files)))
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
	log.info("Parsing strings into ticks")
	eobi_ticks = [ Datamole(line,id_to_template,template_to_columns,log) for line in lines]
	log.info("Datamole decoded {0} lines into ticks".format(len(eobi_ticks)))
	del lines # free up memory
	sys.exit("Bye")

	# Create EOBI object
	eobi = EOBI.EOBI(levels,log)

	# Run throught the data and generate order books
	eobi.calcEOBI(eobi_ticks,log)

	# Generate HDF5 files	
	eobi.bookData.output_to_HDF5()


if __name__=="__main__":
	print(sys.version)
	main()
