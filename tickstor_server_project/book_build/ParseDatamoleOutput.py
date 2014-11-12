#!/usr/bin/env python3

import string, datetime, os, sys, argparse, logging, multiprocessing, pickle
import xml.etree.ElementTree as ET
import mimetypes as mim
#import Datamole_2_EOBI as EOBI
#import Datamole_2_ETI as ETI
import numpy as np
import pandas as pd
import gc
from Book import *

# By keeping them global with optimize on a class that otherwise would store these 2 dictionnaries dozens of millions of time for nothing
template_to_columns = {}
id_to_template = {}

# ===================================================================================
#      Decoder for UDP datagram on EOBI channels represented as list of messages
# ===================================================================================
class EOBI_decoder:
	def __init__(self,eobi_ticks,levels,date):
		# number of levels to output per book
		self.levels = levels
		# date being processed
		self.date = date

		# == original data ==
		# Datamole list
		self.eobi_ticks=eobi_ticks

		# == UDP sequences management
		# Reconstruction of UDP datagrams
		self.udplist = []

		# == EOBI messages sequences management ==
		# order books by instruments
		self.books = {}
		self.mseqnum = {}
		self.msid = -1
		self.packet_header_ts = -1
		self.seqnum_big_gap = 5 # if we miss more than 10 datagrams, we reset the flow

		# Snapshot cycle management
		self.current_snapuid = -1
		self.lastts = -1
		self.nborder = -1

	# Recreate a list of UDP datagrams from P2D lines
	# The list is supposed to simulate was happened on the market
	# in term of UDP datagrams reaching a network card
	def convert_to_UDP_list(self):
		logging.info("Converting Datamole list back to UDP list of datagrams")
		datagram=[]
		for x in self.eobi_ticks:
			if x.name=="eobi_header":
				if datagram:
					self.udplist.append(datagram)
					datagram=[]
			datagram.append(x)
		if datagram:
			self.udplist.append(datagram)
		logging.info("inserted {0} datagrams".format(len(self.udplist)))

	def main_loop(self):
		self.convert_to_UDP_list()
		seqnum = {} # UDP sequence number
		WL={} # UDP waiting list

		logging.info("Running main loop on {0} datagrams".format(len(self.udplist)))
		for datagram in self.udplist:
			print("-----------:new datagram:------------")
			seqn=datagram[0].values["applseqnum"]
			ipp = datagram[0].values["ip_dst"]+":"+datagram[0].values["udp_dst"] # get ip:port
			print("len=",len(datagram), "seqn=",seqn,"ipp=",ipp)
			if ipp not in seqnum: # first packet on this IP/port
				seqnum[ipp] = seqn
				WL[ipp] = [datagram]
				compl = datagram[0].values["CompletionIndicator"]
				print("added new ipp={0} compl={1}".format(ipp,compl))
			else:
				# if new datagram is too far in seq num, remove the waiting sequence (if any)
				if seqn - seqnum[ipp] > self.seqnum_big_gap:
					print("forward big gap detected: seqn={0} seqnum[ipp]={1}".format(seqn,seqnum[ipp]))
					seqnum[ipp]=seqn
					WL[ipp] = [datagram]
					compl = datagram[0].values["CompletionIndicator"]
				# if new datagram is too old, ignore it
				elif seqnum[ipp] - seqn > self.seqnum_big_gap:
					print("ignoring old datagram: seqn={0} seqnum[ipp]={1}".format(seqn,seqnum[ipp]))
					pass
				# else the datagram is not too far away from the previous datagrams
				else:
					print("lenWLavant=",len(WL[ipp]))
					# if we receive a new datagram after incomplete completion
					# of the previous one we discard the old one
					if len(WL[ipp]) and WL[ipp][-1][0].values["CompletionIndicator"]==1 and seqn>WL[ipp][-1][0].values["applseqnum"]:
						print("incomplete sequence is discarded")
						WL[ipp] = []

					# insert the new datagram by order of seqnum in WL[ipp]
					i=0
					while i<len(WL[ipp]):
						if WL[ipp][i][0].values["applseqnum"]>seqn:
							break
						else:
							i+=1
					WL[ipp].insert(i,datagram)
					# update the seqnum with the latest in the sequence
					seqnum[ipp] = WL[ipp][-1][0].values["applseqnum"]
					print("lenWLapres=",len(WL[ipp]))

					# check sequence has been completed
					print("COMPLETION: ",WL[ipp][-1][0].values["CompletionIndicator"])

					if WL[ipp][-1][0].values["CompletionIndicator"]==1:
						s = [x[0].values["applseqnum"] for x in WL[ipp]] # get all seqnum of WL[ipp]
						print("s=",s)
						compl = all([x==1 for x in (s[i+1]-s[i] for i in range(len(s)-1))]) # check they are contiguous
						print("allcompl=",compl)
					else:
						compl = False
						s = [x[0].values["applseqnum"] for x in WL[ipp]] # get all seqnum of WL[ipp]
						print("s=",s)
						print("no CI, no processing")

			if compl:
				# process all messages
				print("processing ",len(WL[ipp]))
				for msg in sum(WL[ipp],[]):
					self.process_msg(msg)
				print("before proc s=",[x[0].values["applseqnum"] for x in WL[ipp]])
				WL[ipp] = []
			print("-------------------------------")
		print("processing done: books=",len(self.books))

	def process_msg(self,msg):
		print("processing ",msg.name)
		if   msg.name == "eobi_header":
			self.msid = msg.values["MarketSegmentID"]
			self.packet_header_ts = msg.values["transacttime"]

		elif msg.name == "eobi_13100_ord_add":
			msgseqnum = msg.values["msgseqnum"]
			ts = msg.values["trdregtstimein"]
			uid = msg.values["secid"]
			oid = msg.values["trdregTStimepriority"]
			qty = msg.values["qty"]
			side= msg.values["side"]
			price=msg.values["price"]

			if uid in self.books and not self.books[uid].not_valid:
				if msgseqnum==self.mseqnum[self.msid]+1: # good, use it
					print(oid)
					self.books[uid].add_order(side,price,oid,qty,ts,ts)
					self.mseqnum[self.msid] = msgseqnum
					print("msg accepted")
				elif msgseqnum <= self.mseqnum[self.msid]: # too old, ignore
					print("msg too old")
					pass
				elif msgseqnum > self.mseqnum[self.msid]+1: # missed msg
					print("missed msg, book invalidated ",uid,msgseqnum,self.mseqnum[self.msid])
					self.books[uid].not_valid = True
			
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

			if uid in self.books and not self.books[uid].not_valid:
				if msgseqnum==self.mseqnum[self.msid]+1: # good, use it
					print(self.books[uid].book[side].keys())
					print(prevoid,prevprice,prevqty,"      ",uid,"    ",oid,qty,side,price)
					if prevprice in self.books[uid].book[side] and prevoid in self.books[uid].book[side][prevprice]: # check if order exists
						if prevqty<qty or prevprice!=price or oid not in self.books[uid].book[side][prevprice]: # loss of priority
							self.books[uid].modify_order(side,price,oid,qty,ts,ts,prevoid,prevprice,prevqty,change=True)
						else: # no loss of priority
							self.books[uid].modify_order(side,price,oid,qty,ts,ts,prevoid,prevprice,prevqty,change=False)
						print("msg accepted")
						self.mseqnum[self.msid] = msgseqnum
					else:
						print("msg not accepted: missing oid or price level")
				elif msgseqnum <= self.mseqnum[self.msid]: # too old, ignore
					print("msg too old")
					pass
				elif msgseqnum > self.mseqnum[self.msid]+1: # missed msg
					print("missed msg, book invalidated ",uid,msgseqnum,self.mseqnum[self.msid])
					self.books[uid].not_valid = True

		elif msg.name == "eobi_13102_order_del":
			msgseqnum = msg.values["msgseqnum"]
			ts = msg.values["trdregtstimein"]
			uid = msg.values["secid"]
			oid = msg.values["trdregTStimepriority"]
			side= msg.values["side"]
			price=msg.values["price"]

			if uid in self.books and not self.books[uid].not_valid: # good, use it
				if msgseqnum==self.mseqnum[self.msid]+1:
					self.books[uid].delete_order(side,price,oid,ts,ts)
					self.mseqnum[self.msid]=msgseqnum
					print("msg accepted")
				elif msgseqnum <= self.mseqnum[self.msid]: # too old, ignore
					print("msg too old")
					pass
				elif msgseqnum > self.mseqnum[self.msid]+1: # missed msg
					print("missed msg, book invalidated ",uid,msgseqnum,self.mseqnum[self.msid])
					self.books[uid].not_valid = True

		elif msg.name == "eobi_13103_order_mass_del":
			msgseqnum = msg.values["msgseqnum"]
			uid = msg.values["secid"]

			if uid in self.books and not self.books[uid].not_valid:
				if msgseqnum==self.mseqnum[self.msid]+1:
					self.books[uid].clear()
					self.mseqnum[self.msid]=msgseqnum
					print("msg accepted")
				elif msgseqnum <= self.mseqnum[self.msid]:
					print("msg too old")
					pass
				elif msgseqnum > self.mseqnum[self.msid]+1:
					print("missed msg, book invalidated ",uid,msgseqnum,self.mseqnum[self.msid])
					self.books[uid].not_valid = True

		elif msg.name == "eobi_13106_order_mod_same_priority":
			msgseqnum = msg.values["msgseqnum"]
			ts = msg.values["trdregtstimein"]
			uid = msg.values["secid"]
			oid = msg.values["trdregTStimepriority"]
			qty = msg.values["qty"]
			side = msg.values["side"]
			price = msg.values["price"]

			if uid in self.books and not self.books[uid].not_valid:
				if msgseqnum==self.mseqnum[self.msid]+1: # good, use it
					self.books[uid].modify_order(side,price,oid,qty,ts,ts,oid,price,qty,change=False)
					self.mseqnum[self.msid] = msgseqnum
					print("msg accepted")
				elif msgseqnum <= self.mseqnum[self.msid]: # too old, ignore
					print("msg too old")
					pass
				elif msgseqnum > self.mseqnum[self.msid]+1: # missed msg
					print("missed msg, book invalidated ",uid,msgseqnum,self.mseqnum[self.msid])
					self.books[uid].not_valid = True

		elif msg.name == "eobi_13104_13105_order_exec":
			msgseqnum = msg.values["msgseqnum"]
			side = msg.values["side"]
			price = msg.values["price"]
			oid = msg.values["trdregTStimepriority"]
			uid = msg.values["secid"]
			tradematch = msg.values["tradematch"]
			qty = msg.values["qty"]

			if uid in self.books and not self.books[uid].not_valid:
				if msgseqnum==self.mseqnum[self.msid]+1: # good, use it
					self.books[uid].report_trade(price,qty,self.packet_header_ts,self.packet_header_ts)
					self.mseqnum[self.msid] = msgseqnum
					print("msg accepted")
				elif msgseqnum<=self.mseqnum[self.msid]: # too old, ignore
					print("msg too old")
					pass	
				elif msgseqnum > self.mseqnum[self.msid]+1: # missed msg
					print("missed msg, no trade report")
					self.mseqnum[self.msid] = msgseqnum
					# this msg is not important to miss
					# because it does not invalidate the book

		elif msg.name == "eobi_13202_exec_summary":
			msgseqnum = msg.values["msgseqnum"]
			uid = msg.values["secid"]
			ts = msg.values["aggressortime"]
			execid = msg.values["execid"]
			qty = msg.values["qty"]
			side = msg.values["side"]
			price = msg.values["price"]

			if uid in self.books and not self.books[uid].not_valid:
				if msgseqnum==self.mseqnum[self.msid]+1:
					self.books[uid].exec_summary(price,qty,self.packet_header_ts,self.packet_header_ts)
					self.mseqnum[self.msid] = msgseqnum
					print("msg accepted")
				elif msgseqnum<=self.mseqnum[self.msid]: # too old, ignore
					print("msg too old")
					pass
				elif msgseqnum > self.mseqnum[self.msid]+1: # missed msg
					print("missed msg, no trade report")
					self.mseqnum[self.msid] = msgseqnum

		# --------------
		# SNAPSHOT CYCLE
		# --------------
		elif msg.name == "eobi_13600_product_summary":
			lastseq = msg.values["lastmegseqnumprocessed"]
			# Snapshot message are always accepted.
			# So we just update the internal state
			self.mseqnum[self.msid] = lastseq
			print("msg accepted")

		elif msg.name == "eobi_13601_instrument_summary_header_body":
			uid = msg.values["securityid"]
			new_book=False
			if uid in self.books and self.books[uid].not_valid:
				self.books[uid].clear()
				new_book=True
			else:
				self.books[uid] = Book(uid,self.date, self.levels)
				new_book=True

			if new_book:
				self.current_snapuid = uid
				self.nborder = msg.values["totnoorders"]
				self.lastts = msg.values["lastupdatetime"]
				#self.lastexectime = msg.values["trdregtsexecutiontime"]
			else:
				self.current_snapuid = -1 # used to ignore the snapshot if we don't need it
				self.nborder = -1
			print("msg accepted")

		elif msg.name == "eobi_13602_snapshot_order":
			oid = msg.values["trdregTStimepriority"]
			qty = msg.values["displayqty"]
			side = msg.values["side"]
			price = msg.values["price"]

			# if current_snapuid != -1 then we accept snapshot orders for this instrument
			if self.current_snapuid!=-1:
				self.books[self.current_snapuid].add_order(side,price,oid,qty,self.lastts,self.lastts)
				self.nborder -= 1
				if self.nborder <= 0:
					self.books[self.current_snapuid].not_valid = False
					self.current_snapuid = -1
					self.lastts = -1
					self.nb_order = -1
			print("msg accepted")

		# unused message that still need to be processed
		elif msg.name in ["eobi_13504_top_of_book","eobi_13503_quote_request","eobi_13502_cross_request","eobi_13201_trade_report"]:
			msgseqnum = msg.values["msgseqnum"]
			uid = msg.values["secid"]

			if uid in self.books and not self.books[uid].not_valid:
				if msgseqnum==self.mseqnum[self.msid]+1: # good, use it
					self.mseqnum[self.msid] = msgseqnum
					print("msg accepted")
				elif msgseqnum <= self.mseqnum[self.msid]: # too old, ignore
					print("msg too old")
					pass
				elif msgseqnum > self.mseqnum[self.msid]+1: # missed msg
					print("missed msg, book invalidated ",uid,msgseqnum,self.mseqnum[self.msid])
					self.books[uid].not_valid = True

		else:
			pass
			# report unknow message


# =============================
#	Datamole  decoder
# =============================
class Datamole:
	def __init__(self,line):
		#self.interface = -1
		#self.line_type = -1
		self.seqnum   = -1
		self.name=""
		#self.timestamp=""
		self.values={}
		self.valid = False

		try:
       			# split lines into tokens
			sl=line.strip('\n').split(',')

       			# parse line
	       		#self.interface=sl[0] 
	       		#self.line_type=sl[1]
			line_type=sl[1]
			self.seqnum=int(sl[2])
       			#self.timestamp=sl[3].replace('.','') #arista timestamp

	       		# transform numeric type in string
	       		#self.name=id_to_template[self.line_type]
			self.name=id_to_template[line_type]

	       		# Values are supposed to appear in the CSV file in the same
       			# order as the <detail> tags in the XML file
			for i,key in enumerate(template_to_columns[self.name]):
				if key in ["TrdRegTSPrevTime-Pri", "lastmegseqnumprocessed", "lastupdatetime", "msgseqnum",
					"transacttime", "trdregTStimepriority", "trdregtsexecutiontime", "trdregtstimein",
					"aggressortime","applseqnum","DisplayQty", "PrevDisplayQty", "displayqty", "qty", "totnoorders",
					"CompletionIndicator","bodylen"]:
					self.values[key]=int(sl[4+i])
				elif key in ["side"]:
					self.values[key]=int(sl[4+i])-1 # side is used as an index
				elif key in ["price","PrevPrice"]:
					self.values[key]=float(sl[4+i])/1e8
				else:
					self.values[key]=sl[4+i]
			self.valid = True
		except:
			self.valid = False
	
# =========================================
#      Main program and main functions
# =========================================

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
			template_to_columns[child.attrib['id']].append(detail.attrib['id'])
			#print(child.attrib['id'],detail.attrib['id'])

	return id_to_template,template_to_columns

# ====================
#     Main program 
# ====================
def main():
	# Parse command line arguments
	parser = argparse.ArgumentParser(description="Parse a Datamole csv file.")
	parser.add_argument("--levels","-l",help="max Order Book levels",type=int,default=5)
	parser.add_argument("--odir","-o",help="output directory",type=str,default="./")
	parser.add_argument("--tdir","-t",help="template directory",type=str,default="./")
	parser.add_argument("data_dir",help="Directory containing the data to parser",type=str)
	args = parser.parse_args()
	eobi_path=os.path.join(os.path.realpath(args.data_dir), "eobi")

	# setup logging
	logging.basicConfig(filename=os.path.join(args.odir,"eobi.log"),level=logging.INFO)

	# Parse XML Templates		
	global id_to_template, template_to_columns
	id_to_template,template_to_columns=parseTemplates(args.tdir)
		
	# Get EOBI files list # we only want .csv files
	files = [x for x in sorted(os.listdir(eobi_path)) if mim.guess_type(x)[0]=='text/csv']
	# Dates we want to process
	dir_date=os.path.split(os.path.split(eobi_path)[0])[1]

	logging.info("levels={0} odir={1} tdir={2} data_dir={3} dir_date={4}".format(args.levels,args.odir,args.tdir,args.data_dir,dir_date))

	# Read all the files in one pass
	logging.info("Reading {0} files".format(len(files)))
	lines=[] # ticks as strings from their respective files
	for file in files:
		# check the file date is the same as the dir date
		file_date=datetime.fromtimestamp(
			float(file.split('-')[0])).date().strftime("%Y%m%d")
		# read and store all the file
		if file_date==dir_date:
			with open(os.path.join(eobi_path,file)) as o:
				lines.extend(o.readlines())
	logging.info("Day has {0} ticks to process".format(len(lines)))

	# Parse string ticks into Datamole ticks
	logging.info("Parsing into Datamole objects")
	eobi_ticks = [ Datamole(line) for line in lines]
	logging.info("Datamole decoded {0} lines into ticks".format(len(eobi_ticks)))
	del lines # free up memory

	# Create EOBI object
	eobi = EOBI_decoder(eobi_ticks, args.levels, dir_date)

	# Parse data
	eobi.main_loop()
	sys.exit(0)
	# Generate HDF5 files	
	for uid in eobi.books:
		# create HDF5 header
		headers = ["otype","h","min","sec","us","deltat","recv","exch"]
		headers.extend(sum([ ["bid{0}".format(i),"bidv{0}".format(i)] for i in range(5) ],[]))
		headers.extend(sum([ ["ask{0}".format(i),"askv{0}".format(i)] for i in range(5) ],[]))
		# write file
		output_dir = os.path.join(args.odir,"{0}_{1}.hdf".format(self.eobi.books[uid].date,uid))
		pd.DataFrame(eobi.books[uid].output, columns=headers).to_hdf(output_dir, 'table', append=False)
				

if __name__=="__main__":
	main()
