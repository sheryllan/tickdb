import numpy as np
import pandas as pd


class MessageSequenceTracker:
	def __init__(self):
		self.product_sequence_numbers={} # map of uid and the integer sequence number
		self.product_stored_data={} # map of uid and a list of sequence numbers not processed yet.
		
############################################################
#
#   KEY:
#	A:  Order Add
#	C:  Book Cleared
#	D:  Order Delete
#	M:  Order Modify no priority change
#	R:  Order Modify priority change
#	T:  Trade
#
############################################################
class ByOrderBookData(MessageSequenceTracker):
	def __init__(self,levels):
		MessageSequenceTracker.__init__(self)
		self.obooks = {} # contains books
		self.__results = {} # map of uid and columns of results
		self.__levels=levels
	
	def delete_book(self,uid):
		if uid in self.__results: self.__results.pop(uid)
		if uid in self.product_sequence_numbers: self.product_sequence_numbers.pop(uid)
		if uid in self.product_stored_data: self.product_stored_data.pop(uid)
		if uid in self.obooks: self.obooks.pop(uid)
		
	def init_book(self,uid,msgseqnum):
		self.obooks[uid] = ({}, {}) # l[1]: symbol id. tuple of 2 dicts (bid, ask)
		self.product_sequence_numbers[uid]=msgseqnum
		if uid not in self.__results:
			self.__results[uid]={}

	# 3 lists for each data type for each uid
		for item in ['otype','exch','recv']:
			self.__results[uid][item]=[]
	# and 4N lists, for each level of the book
		for i in range(1,int(self.__levels)+1):
			self.__results[uid]["bid{0}".format(i)]=[]
			self.__results[uid]["bidv{0}".format(i)]=[]
		for i in range(1,int(self.__levels)+1):
			self.__results[uid]["ask{0}".format(i)]=[]
			self.__results[uid]["askv{0}".format(i)]=[]	
			
	############################################################
	#
	#   Print Functions   // Change to using Pandas
	#
	############################################################

	def printBook(self,otype, uid, texch, trecv):
		#output the data
		self.__results[uid]['otype'].append(otype)
		self.__results[uid]['exch'].append(texch)
		self.__results[uid]['recv'].append(trecv)
		for side in [0,1]:
			side_str="bid"
			if side==1:
				side_str="ask"
			book = self.obooks[uid][side]
			prices = sorted(book.keys(), reverse = side==0)
			count = 1

			for price in prices:
				if count <= self.__levels:
					sum = 0
					for o in book[price]:
						sum += o[1]
					self.__results[uid]["{0}{1}".format(side_str,count)].append(price)
					self.__results[uid]["{0}v{1}".format(side_str,count)].append(sum)
					count += 1
			if count<=self.__levels:
				# fill in missing levels with NaN
				for i in range(count,int(self.__levels)+1):
					self.__results[uid]["{0}{1}".format(side_str,i)].append(np.nan)
					self.__results[uid]["{0}v{1}".format(side_str,i)].append(0)

	def printTrade(self,uid,exch,recv,price,qty):			
		# output the data
		self.__results[uid]['otype'].append("T")
		self.__results[uid]['exch'].append(exch)
		self.__results[uid]['recv'].append(recv)
		self.__results[uid]['bid1'].append(price)
		self.__results[uid]['bidv1'].append(qty)
		# Fill in the price levels with NaN as this is a trade
		for i in range(1,int(self.__levels)+1):
			for side in ["bid","ask"]:
				if not (side=="bid" and i==1):
					self.__results[uid]["{0}{1}".format(side,i)].append(np.nan)
					self.__results[uid]["{0}v{1}".format(side,i)].append(0)
					
	def output_to_HDF5(self):
		for uid in self.__results:
			dataframe=pd.DataFrame(self.__results[uid],index=range(len(self.__results[uid]['recv'])))
			dataframe.to_hdf("{0}_{1}.h5".format(self.date, uid),'table',append=False)
		
class IncrementalBookData(MessageSequenceTracker):
	def __init__(self):
		MessageSequenceTracker.__init__(self)
		self.__results = {}   #map of uid and columns of results
#!/usr/bin/env python3

# TODO list
# 1- add exception to Datamole constructor is id of the message is not EOBI, i.e. sl[0]!=2

# -*- coding: utf-8 -*-
import string, datetime, os, sys, argparse, logging, multiprocessing, pickle
import xml.etree.ElementTree as ET
import mimetypes as mim
import Datamole_2_EOBI as EOBI
import Datamole_2_ETI as ETI
import numpy as np
import pandas as pd

# --------------------




class MessageSequenceTracker:
	def __init__(self):
		self.product_sequence_numbers={} # map of uid and the integer sequence number
		self.product_stored_data={} # map of uid and a list of sequence numbers not processed yet.
		
############################################################
#
#   KEY:
#	A:  Order Add
#	C:  Book Cleared
#	D:  Order Delete
#	M:  Order Modify no priority change
#	R:  Order Modify priority change
#	T:  Trade
#
############################################################
class ByOrderBookData(MessageSequenceTracker):
	def __init__(self,levels):
		MessageSequenceTracker.__init__(self)
		self.obooks = {} # contains books
		self.__results = {} # map of uid and columns of results
		self.__levels=levels
	
	def delete_book(self,uid):
		if uid in self.__results: self.__results.pop(uid)
		if uid in self.product_sequence_numbers: self.product_sequence_numbers.pop(uid)
		if uid in self.product_stored_data: self.product_stored_data.pop(uid)
		if uid in self.obooks: self.obooks.pop(uid)
		
	def init_book(self,uid,msgseqnum):
		self.obooks[uid] = ({}, {}) # l[1]: symbol id. tuple of 2 dicts (bid, ask)
		self.product_sequence_numbers[uid]=msgseqnum
		if uid not in self.__results:
			self.__results[uid]={}

	# 3 lists for each data type for each uid
		for item in ['otype','exch','recv']:
			self.__results[uid][item]=[]
	# and 4N lists, for each level of the book
		for i in range(1,int(self.__levels)+1):
			self.__results[uid]["bid{0}".format(i)]=[]
			self.__results[uid]["bidv{0}".format(i)]=[]
		for i in range(1,int(self.__levels)+1):
			self.__results[uid]["ask{0}".format(i)]=[]
			self.__results[uid]["askv{0}".format(i)]=[]	
			
	############################################################
	#
	#   Print Functions   // Change to using Pandas
	#
	############################################################

	def printBook(self,otype, uid, texch, trecv):
		#output the data
		self.__results[uid]['otype'].append(otype)
		self.__results[uid]['exch'].append(texch)
		self.__results[uid]['recv'].append(trecv)
		for side in [0,1]:
			side_str="bid"
			if side==1:
				side_str="ask"
			book = self.obooks[uid][side]
			prices = sorted(book.keys(), reverse = side==0)
			count = 1

			for price in prices:
				if count <= self.__levels:
					sum = 0
					for o in book[price]:
						sum += o[1]
					self.__results[uid]["{0}{1}".format(side_str,count)].append(price)
					self.__results[uid]["{0}v{1}".format(side_str,count)].append(sum)
					count += 1
			if count<=self.__levels:
				# fill in missing levels with NaN
				for i in range(count,int(self.__levels)+1):
					self.__results[uid]["{0}{1}".format(side_str,i)].append(np.nan)
					self.__results[uid]["{0}v{1}".format(side_str,i)].append(0)

	def printTrade(self,uid,exch,recv,price,qty):			
		# output the data
		self.__results[uid]['otype'].append("T")
		self.__results[uid]['exch'].append(exch)
		self.__results[uid]['recv'].append(recv)
		self.__results[uid]['bid1'].append(price)
		self.__results[uid]['bidv1'].append(qty)
		# Fill in the price levels with NaN as this is a trade
		for i in range(1,int(self.__levels)+1):
			for side in ["bid","ask"]:
				if not (side=="bid" and i==1):
					self.__results[uid]["{0}{1}".format(side,i)].append(np.nan)
					self.__results[uid]["{0}v{1}".format(side,i)].append(0)
					
	def output_to_HDF5(self):
		for uid in self.__results:
			dataframe=pd.DataFrame(self.__results[uid],index=range(len(self.__results[uid]['recv'])))
			dataframe.to_hdf("{0}_{1}.h5".format(self.date, uid),'table',append=False)
		
class IncrementalBookData(MessageSequenceTracker):
	def __init__(self):
		MessageSequenceTracker.__init__(self)
		self.__results = {}   #map of uid and columns of results


# ----------------











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
			for i,key in enumerate(self.template_to_columns[self.name]):
				self.values[key]=sl[4+i]
		except:
			self.__log.fatal(line)
				   
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
	id_to_template,template_to_columns=parseTemplates(path)
		
	# Get EOBI files list
	files=os.listdir(eobi_path)
	# we only want .csv files
	files = [x for x in sorted(files) if mim.guess_type(x)[0]=='text/csv']
	dir_date=os.path.split(os.path.split(eobi_path)[0])[1] # date we want to process
	log.info("Reading {0} files".format(len(files)))

	# Read all the files in one pass
	eobi_ticks=[] # ticks after "Datamole" parsing
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
	i=0
	for line in lines:
		eobi_ticks.append(Datamole(line,id_to_template,template_to_columns,log))
		i+=1
		if i%1000000==0:
			log.info("processed {0} lines".format(i))
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
