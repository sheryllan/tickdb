#!/usr/bin/env python3

import string, datetime, os, sys, argparse, logging, multiprocessing, pickle
import xml.etree.ElementTree as ET
import mimetypes as mim
import Datamole_2_EOBI as EOBI
import Datamole_2_ETI as ETI
import numpy as np
import pandas as pd
from Book import *

# By keeping them global with optimize on a class that otherwise would store these 2 dictionnaries dozens of millions of time for nothing
template_to_columns = {}
id_to_template = {}

# ===================================================================================
#      Decoder for UDP datagram on EOBI channels represented as list of messages
# ===================================================================================
class EOBI_decoder:
	def __init__(self,levels,date):
		# number of levels to output per book
		self.levels = levels
		# date being processed
		self.date = date

		# == original data ==
		# Datamole list
		self.eobi_ticks=[]

		# == UDP sequences management
		# Reconstruction of UDP datagrams
		self.udplist = []
		# latest udp seq number processed by IP:port
		self.udpseqnum = {}
		self.udpwaitlist=[]

		# == EOBI messages sequences management ==
		# order books by instruments
		self.books = {}
		self.seqnum = {}
		self.msid = -1

	# Recreate a list of UDP datagrams from P2D lines
	# The list is supposed to simulate was happened on the market
	# in term of UDP datagrams reaching a network card
	def convert_to_UDP_list(self):
		datagram = []
		while self.eobi_ticks:
			tick = self.eobi_ticks.pop(0) # read message
			datagram.append(tick)
			if tick.name == "eobi_header": # start reading UDP datagram
				# read each message in the UDP datagram
				while self.eobi_ticks 
				and self.eobi_ticks[0].name!="eobi_header":
					datagram.append(self.eobi_ticks.pop(0))
				self.udplist.append(datagram)
				datagram = []

	def main_loop(self):
		self.convert_to_UDP_list()
		self.udp_main_loop()

	def udp_main_loop(self):
		while self.udplist:
			datagram = self.udplist.pop(0) # read latest UDP datagram
			seqnum = datagram[0].values["applseqnum"] # get its sequence number ...
			ipport = datagram[0].values["ip_dst"]+":"+datagram[0].values["udp_dst"] # ... and ip:port
			if ipport not in self.udpseqnum or seqnum == self.udpseqnum[ipport]-1: # check it's the next one
				self.udpwaitlist.insert(0,datagram) # insert at the beginning of the list
			else:
				self.udpwaitlist.append(datagram) # put at the end (natural order)

			keep_processing=True
			while keep_processing: # Fixed-point to process all waiting datagrams in order
				i=0
				while self.udpwaitlist[i][0].values["applseqnum"] !=
					self.udpseqnum[self.udpwaitlist[i][0].values["ip_dst"]+":"+self.udpwaitlist[i][0].values["udp_dst"]]+1:
					i=i+1
				if i < len(self.udpwaitlist): # means we found a matching datagram
					self.process_udp_seq(self.udpwaitlist[i]) # process messages
					self.udpseqnum[self.udpwaitlist[i][0].values["ip_dst"]+":"+self.udpwaitlist[i][0].values["udp_dst"]]=
					self.udpwaitlist[i][0].values["applseqnum"] # update seqnum
					del self.udpwaitlist[i]
					keep_processing = True
				else
					keep_processing = False
	
	def process_udp_seq(self,udpwaitlist):
		while udpwaitlist:
			datagram = udpwaitlist.pop(0)
			while datagram:
				msg = datagram.pop(0)
				if ! process_msg(msg):
					msgwaitlist.append(msg)
			newmsgwaitlist = []
			while msgwaitlist:
				msg = datagram.pop(0)
				if ! process_msg(msg):
					newmsgwaitlist.append(msg)
			msgwaitlist = newmsgwaitlist

	def process_msg(self,msg):
		if   msg.name == "eobi_header":
			self.msid = msg.values["MarketSegmentID"]
			return True

		elif msg.name == "eobi_13100_ord_add":
			msgseqnum = msg.values["msgseqnum"]
			ts = msg.values["trdregtstimein"]
			uid = msg.values["secid"]
			oid = msg.values["trdregTStimepriority"]
			qty = msg.values["qty"]
			side= msg.values["side"]
			price=msg.values["price"]

			if uid in books:
				if !books[uid].not_valid:
					if msgseqnum=seqnum[msid]+1: # good, use it
						books[uid].add_order(side,price,oid,qty,ts,ts)
						seqnum[msid] = msgseqnum
						return True
					elif msgseqnum <= seqnum[msid]: # too old, ignore
						return True
					elif msgseqnum > seqnum[msid]+1: # missed msg
						books[uid].not_valid = True
						return True
				else: # book not ready, keep message for later
					return False
			else: # book not ready, keep message for later
				return False
			
		elif msg.name == "eobi_13101_order_mod":
			msgseqnum = msg.values["msgseqnum"]
			ts = msg.values["trdregtstimein"]
			prevoid = msg.values["TrdRegTSPrevTime-Pri"]
			prevprice = msg.values["PrevPrice"]
			prevqty = msg.values["PrevDisplayQty"]
			uid = msg.values["secid"]
			oid = msg.values["trdregTStimepriority"]
			qty = msg.values["DisplayQty"]
			side= msg.values["side"]
			price=msg.values["price"]

			if uid in books:
				if !books[uid].not_valid:
					if msgseqnum=seqnum[msid]+1:
						if prevoid not in books[uid][side][prevprice] or # loss of priority
						prevprice != price or
						prevqty < qty:
							books[uid].modify_order(side,price,oid,qty,ts,ts,prevoid,prevprice,prevqty,change=True)
						else:
							books[uid].modify_order(side,price,oid,qty,ts,ts,prevoid,prevprice,prevqty,change=True)
						seqnum[msid] = msgseqnum
						return True
					elif msgseqnum <= seqnum[msid]: # too old, ignore
						return True
					elif msgseqnum > seqnum[msid]+1: # missed msg
						books[uid].not_valid = True
						return True
				else:
					return False
			else:
				return False

		elif msg.name == "eobi_13102_order_del":
			msgseqnum = msg.values["msgseqnum"]
			ts = msg.values["trdregtstimein"]
			uid = msg.values["secid"]
			oid = msg.values["trdregTStimepriority"]
			side= msg.values["side"]
			price=msg.values["price"]
			qty = msg.values["qty"]

			if uid in books:
				if !books[uid].not_valid:
					if msgseqnum=seqnum[msid]+1:
						books[uid].delete_order(side,price,oid,ts,ts)
						seqnum[msid]=msgseqnum
						return True
					elif msgseqnum <= seqnum[msid]:
						return True
					elif msgseqnum > seqnum[msid]+1:
						books[uid].not_valid = True
						return True
				else:
					return False
			else:
				return False

		elif msg.name == "eobi_13103_order_mass_del":
			msgseqnum = msg.values["msgseqnum"]
			uid = msg.values["secid"]

			if uid in books
				if !books[uid].not_valid:
					if msgseqnum=seqnum[msid]+1:
						books[uid].clear()
						seqnum[msid]=msgseqnum
						return True
					elif msgseqnum <= seqnum[msid]:
						return True
					elif msgseqnum > seqnum[msid]+1:
						books[uid].not_valid = True
						return True
				else:
					return False
			else:
				return False

		elif msg.name == "eobi_13106_order_mod_same_priority":
			msgseqnum = msg.values["msgseqnum"]
			ts = msg.values["trdregtstimein"]
			uid = msg.values["secid"]
			oid = msg.values["trdregTStimepriority"]
			qty = msg.values["qty"]
			side = msg.values["side"]
			price = msg.values["price"]

			if uid in books:
				if !books[uid].not_valid:
					if msgseqnum=seqnum[msid]+1:
						books[uid].modify_order(side,price,oid,qty,ts,ts,oid,price,qty,change=False)
						seqnum[msid] = msgseqnum
						return True
					elif msgseqnum <= seqnum[msid]: # too old, ignore
						return True
					elif msgseqnum > seqnum[msid]+1: # missed msg
						books[uid].not_valid = True
						return True
				else:
					return False
			else:
				return False



			return True

		elif msg.name == "eobi_13104_13105_order_exec":
			msgseqnum = msg.values["msgseqnum"]
			side = msg.values["side"]
			price = msg.values["price"]
			oid = msg.values["trdregTStimepriority"]
			uid = msg.values["secid"]
			tradematch = msg.values["tradematch"]
			qty = msg.values["qty"]
			return True

		elif msg.name == "eobi_13202_exec_summary":
			msgseqnum = msg.values["msgseqnum"]
			uid = msg.values["secid"]
			ts = msg.values["aggressortime"]
			execid = msg.values["execid"]
			qty = msg.values["qty"]
			side = msg.values["side"]
			price = msg.values["price"]
			return True

# SNAPSHOT CYCLE
	elif msg.name == "eobi_13600_product_summary":
			msgseqnum = msg.values["msgseqnum"]
			lastseq = msg.values["lastmegseqnumprocessed"]
			tid = msg.values["tradingsessionid"]
			tsid = msg.values["tradingsessionsubid"]
			status = msg.values["tradsesstatus"]
			fmind = msg.values["fastmktindicator"]
			return True

     elif msg.name == "eobi_13602_snapshot_order":
			msgseqnum = msg.values["msgseqnum"]
			oid = msg.values["trdregTStimepriority"]
			qty = msg.values["displayqty"]
			side = msg.values["side"]
			price = msg.values["price"]
			status = msg.values["tradsesstatus"]
			return True

     elif msg.name == "eobi_13601_instrument_summary_header_body":
			msgseqnum = msg.values["msgseqnum"]
			uid = msg.values["securityid"]
			 = msg.values["lastupdatetime"]
			 = msg.values["trdregtsexecutiontime"]
			 = msg.values["totnoorders"]
			 = msg.values["securitystatus"]
			 = msg.values["securitytradingstatus"]
			 = msg.values["fastmktindicator"]
			 = msg.values["nomdentries"]

			 = msg.values["E1mdentrytype"]
			 = msg.values["E1pad3"]
			 = msg.values["E1mdentrypx"]
			 = msg.values["E1mdentrysize"]

			 = msg.values["E2mdentrytype"]
			 = msg.values["E2pad3"]
			 = msg.values["E2mdentrypx"]
			 = msg.values["E2mdentrysize"]
			return True

# =============================
#	Datamole  decoder
# =============================
class Datamole:
	def __init__(self,line):
		self.interface = -1
		self.line_type = -1
		self.seqnume   = -1

		self.name=""
		self.timestamp=""
		self.values={}
		self.valid = False
		
		try:
			# split lines into tokens
			sl=line.strip('\n').split(',')

			# parse line
			self.interface=sl[0] 
			self.line_type=sl[1]
			self.seqnum=sl[2]
			self.timestamp=sl[3].replace('.','') #arista timestamp

			# transform numeric type in string
			self.name=id_to_template[self.line_type]

			# Values are supposed to appear in the CSV file in the same
			# order as the <detail> tags in the XML file
			for i,key in enumerate(template_to_columns[self.name]):
				self.values[key]=sl[4+i]
			self.valid = True
		except:
			self.valid = False
	
# =========================================
#      Main program and main functions
# =========================================

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
	eobi_ticks = [ Datamole(line) for line in lines]
	log.info("Datamole decoded {0} lines into ticks".format(len(eobi_ticks)))
	del lines # free up memory

	# Create EOBI object
	eobi = EOBI.EOBI(levels,log)

	# Run throught the data and generate order books
	eobi.calcEOBI(eobi_ticks,log)

	# Generate HDF5 files	
	eobi.bookData.output_to_HDF5()


if __name__=="__main__":
	print(sys.version)
	main()
