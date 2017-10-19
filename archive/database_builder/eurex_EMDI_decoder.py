#!/usr/bin/env python3
import getopt
import uuid
import os
import tarfile
import shutil
import sys
import time
import argparse
import datetime, dateutil.parser
from Book import *
import csv
from copy import deepcopy

# I was using Enum before for the following definitions.
# But Enum is surprisingly very slow and was taking up
# to 5% of the total time according to the profiler.
# For example Enum is heavily using a hash function !!!
# And Enum methods are also the most called functions of this code !!!
# Overall it costs 32 minutes of computation !!!!!!!!!
# Hence the following variables to replace Enum's
# State constant
state_init = 0

# RDD
state_snapshot_cycle = 1
state_product_cycle = 2

# EMDI
state_incremental = 3
state_snapshot = 4

DI = ['DepthIncremental',
		'ComplexInstrumentUpdate',
		'CrossRequest',
		'QuoteRequest',
		'InstrumentStateChange',
		'ProductStateChange',
		'MassInstrumentStateChange',
		'TopOfBookImplied']

# return val of key in list
# or NA if not found
def val(_list,key,start=0,default=float('nan'),typ=''):
	try:
		if typ=='':
			return _list[_list.index(key,start)+1]
		else:
			return typ(_list[_list.index(key,start)+1]) # convert to type 'typ'
	except:
		return default

# return index of key in list
# or 999999 if not found
def vali(_list,key,start=0):
	try:
		return _list.index(key,start)+1
	except:
		return 999999

#################################################
# Generic Decoder
# In particular, it implements the UDP datagram decoding
# and contiguity check
#################################################

class generic_decoder:
	def __init__(self):
		self.packetseqnum = {}
		self.queue = {}
		self.max_queue=5
		self.nb_packet=0

	# This function is called at the end of the decoding
	def output(self):
		print("# packets=",self.nb_packet)
	
	# This function is called on each FAST messages that is each line
	# of a UDP datagram except the first 2
	def process_messages(self,packet,found_gap):
		self.nb_packet+=1
		print("process_messages: ",packet[0][3].rstrip(),val(packet[1],"SenderCompID"),
				val(packet[1],"PacketSeqNum"))

	def search_new_psn(self,KEY):
		minpsn = sys.maxsize
		for p in self.queue[KEY]:
			psn = int(val(p[1],"PacketSeqNum")) # PacketSeqNum
			if psn < minpsn:
				minpsn = psn

		return minpsn-1

	# Check contiguity of UDP datagram
	def contiguity(self,KEY,new_psn):
		if KEY not in self.packetseqnum: # new KEY
			self.packetseqnum[KEY] = new_psn # create structures
			self.queue[KEY] = []
			return 0
		else:
			last_psn= self.packetseqnum[KEY]
			if new_psn == (last_psn+1):
				return 0 # on time
			elif new_psn > (last_psn+1):
				return 1 # too early
			else:
				return -1 # too late

	# Process a queue of UDP datagram in case of a out-of-order
	# packets
	def process_queue(self,KEY):
		processed=1
		while processed>0:
			processed=0
			i=0
			while i<len(self.queue[KEY]):
				p = self.queue[KEY][i]
				new_psn = int(val(p[1],"PacketSeqNum"))
				cont = self.contiguity(KEY,new_psn)
				if cont == 0:
					self.packetseqnum[KEY] = new_psn
					self.process_messages(p,False)
					del self.queue[KEY][i]
					processed+=1
				elif cont == -1: # too late
					del self.queue[KEY][i]
				else:
					i+=1
	
	# Process a UDP datagram and re-order them if needed
	# When in a child class, the process message of the child is called
	def process_udp_datagram(self,packet):
		ip = packet[0][2]
		dport = packet[0][3].rstrip()
		scid = val(packet[1],"SenderCompID")
		KEY = ip+'_'+dport+'_'+scid

		new_psn = int(val(packet[1],"PacketSeqNum"))
		
		# check datagram contiguity
		cont = self.contiguity(KEY,new_psn)
		if cont == 0: # on time
			print("generic: on time key=", KEY,'psn=',self.packetseqnum[KEY],
					'new_psn=',new_psn,'timestamp=', packet[0][1])
			self.packetseqnum[KEY] = new_psn
			self.process_messages(packet,False) # process packet
			if len(self.queue[KEY])>0: # process queue if any
				self.process_queue(KEY)
		elif cont == 1: # too early
			print("generic: too early key=", KEY,'psn=',self.packetseqnum[KEY],
					'new_psn=',new_psn,'timestamp=', packet[0][1])
			self.queue[KEY].append(deepcopy(packet)) # bufferise packet
		#else:
			print("generic: too late key=", KEY,'psn=',self.packetseqnum[KEY],
					'new_psn=',new_psn,'timestamp=', packet[0][1])

		# gap in data
		if len(self.queue[KEY])==self.max_queue:
			print("reach max queue: processing with gap")
			min_psn = sys.maxsize # find packet with smallest psn
			min_i = 0
			for i in range(0,len(self.queue[KEY])):
				psn = int(val(self.queue[KEY][i][1],"PacketSeqNum"))
				if psn < min_psn:
					min_psn=psn
					min_i=i
			# and restart from there
			print("restart at min_psn=",min_psn)
			self.packetseqnum[KEY] = min_psn
			self.process_messages(self.queue[KEY][min_i],True)
			del self.queue[KEY][min_i]
			if len(self.queue[KEY])>0:
				self.process_queue(KEY)

#################################################################
# RDD DECODER
# The result is a daily database of products sent by the exchange
#################################################################

def product_complex(it):
	it = int(it)
	if it==0:
		return 'Simple Instrument'
	elif it==1:
		return 'Standard Option Strategy'
	elif it==2:
		return 'Non-Standard Option Strategy'
	elif it==3:
		return 'Volatility Strategy'
	elif it==4:
		return 'Future Spread'
	elif it==5:
		return 'Inter-Product Spread'
	elif it==6:
		return 'Standard Futures Strategy'
	elif it==7:
		return 'Pack and Bundle'
	elif it==8:
		return 'Strip'

class rdd_decoder(generic_decoder):
	def __init__(self,output_dir, date):
		generic_decoder.__init__(self)
		self.state = state_init
		self.msgseqnum = -1
		self.product = {}
		self.inst={}
		self.current_pid = ''
		self.product_fields = ['MarketID','MarketSegmentID','MarketSegment',
			'MarketSegmentDesc','MarketSegmentSymbol','Currency',
			'MarketSegmentStatus','PartitionID',
			'UnderlyingSecurityExchange','UnderlyingSymbol','UnderlyingSecurityID']
		self.inst_fields=['product', 'SecurityID','SecurityType', 
			'ProductComplex', 'MaturityDate', 'MaturityMonthYear',
			'StrikePrice', 'PricePrecision', 'ContractMultiplier',
			'PutOrCall', 'ExerciseStyle', 'SettlMethod', 'SecurityDesc',
			'MinPriceIncrement', 'MinPriceIncrementAmount','SecurityStatus']
		self.ofname = output_dir+"/"+"Eurex_products_db_"+date+".csv"

	def create_product(self,msg):
		pid=val(msg,'MarketSegmentID')
		if pid not in self.product:
			self.product[pid]={}
			for key in self.product_fields:
				self.product[pid][key] = val(msg,key)
			self.current_pid = pid

	def create_inst(self,msg):
		uid = val(msg,'SecurityID')
		if uid not in self.inst:
			self.inst[uid] = {}
			self.inst[uid]['product'] = self.current_pid
			# Retrieve instrument info
			self.inst[uid]['SecurityID'] = uid
			st = val(msg,'SecurityType')
			if st=='0':
				self.inst[uid]['SecurityType']= 'O' # options
			elif st=='1':
				self.inst[uid]['SecurityType']= 'F' # futures
			else:
				self.inst[uid]['SecurityType']= 'C' # complex instrument
			self.inst[uid]['ProductComplex']=product_complex(val(msg,'ProductComplex'))
			self.inst[uid]['MaturityDate']=val(msg,'MaturityDate')
			self.inst[uid]['MaturityMonthYear']=val(msg,'MaturityMonthYear')
			self.inst[uid]['StrikePrice']=val(msg,'StrikePrice')
			self.inst[uid]['PricePrecision']=val(msg,'PricePrecision')
			self.inst[uid]['ContractMultiplier']=val(msg,'ContractMultiplier')
			poc = val(msg,'PutOrCall')
			if st=='0':
				if poc=='0':
					self.inst[uid]['PutOrCall']='P'
				elif poc=='1':
					self.inst[uid]['PutOrCall']='C'
			exs = val(msg,'ExerciseStyle')
			if exs=='0':
				self.inst[uid]['ExerciseStyle']='EU'
			elif exs=='1':
				self.inst[uid]['ExerciseStyle']='US'
			sem = val(msg,'SettlMethod')
			if sem=='0':
				self.inst[uid]['SettlMethod']='Cash'
			elif sem=='1':
				self.inst[uid]['SettlMethod']='Physical'
			self.inst[uid]['SecurityDesc']=val(msg,'SecurityDesc')
			self.inst[uid]['MinPriceIncrement']=val(msg,'MinPriceIncrement')
			self.inst[uid]['MinPriceIncrementAmount']=val(msg,'MinPriceIncrementAmount')
			self.inst[uid]['SecurityStatus']=val(msg,'SecurityStatus')

	def output(self):
		f = open(self.ofname,'w')
		print(','.join(self.product_fields),end='',file=f) # header product
		print(',',end='',file=f)
		print(','.join(self.inst_fields),file=f) # header instrument

		for i in self.inst:
			p = self.inst[i]['product']
			# print product data
			print(','.join(
				(str(self.product[p][x]) if x!='MarketSegmentDesc'
					else '"'+str(self.product[p][x])+'"')
				for x in self.product_fields),end='',file=f)
			print(',',end='',file=f)
			# print instrument data
			print(','.join(str(self.inst[i].get(x,'')) for x in self.inst_fields),file=f)

		f.close()

	def process_messages(self,packet,found_gap):
		for i in range(2,len(packet)): # useful messages start at packet[2]
			# Run FSA
			if self.state == state_init: # Wait for a snapshot to start
				if packet[i][0]=='MarketDataReport':
					if int(val(packet[i],'MDReportEvent'))==0:
						self.state = state_snapshot_cycle # enter snapshot
						print("rdd: init -> snapshot_cycle",packet[0][1])
			# Read all snapshot messages
			elif self.state == state_snapshot_cycle:
				if found_gap:
					self.state = state_init
				elif packet[i][0]=='MarketDataReport':
					if int(val(packet[i],'MDReportEvent'))==1:
						self.state = state_init # end of snapshot
						print("rdd: END snapshot_cycle -> init",packet[0][1])
				else:
					if packet[i][0]=='ProductSnapshot':
						self.create_product(packet[i]) # create new product
						print("rdd: create product","len=",len(self.product))
					elif packet[i][0]=='InstrumentSnapshot':
						self.create_inst(packet[i]) # add new instrument
						print("rdd: create inst len=",len(self.inst))

					#msgseqnum = int(val(packet[i],'MsgSeqNum'))
					#print("rdd: process msg",self.msgseqnum, msgseqnum,packet[i][0])
					## check contiguity
					#if self.msgseqnum==-1 or (self.msgseqnum+1)==msgseqnum:
					#	self.msgseqnum = msgseqnum
					#	if packet[i][0]=='ProductSnapshot':
					#		self.create_product(packet[i]) # create new product
					#		print("rdd: create product","len=",len(self.product))
					#	elif packet[i][0]=='InstrumentSnapshot':
					#		self.create_inst(packet[i]) # add new instrument
					#		print("rdd: create inst len=",len(self.inst))
					#elif self.msgseqnum >= msgseqnum: # too late=duplicate from channel B in general
					#	print("rdd: duplicate")
					#else: # gap
					#	self.state = state_init
					#	print("rdd: BUG snapshot_cycle -> init")

######################################################################################
# Product represents a product in the Eurex FAST sense, that is it is made of many
# instruments.
# It contains the main code to decode FAST messages of each instruments order book
# updates
######################################################################################

class Product:
	def __init__(self):
		self.state = state_init
		self.msn = -1
		self.queue = []
		self.inst = {}
		self.inst_init = set()
		self.first_inst = -1

	# reset a product when a gap has been detected
	def reset(self):
		self.state = state_init
		self.msn = -1
		self.inst_init = set()
		self.first_inst = -1

	# True if we received all initial book for this product
	def snapshot_complete(self):
		return len(self.inst)==len(self.inst_init)

	# Decode and initialize instruments' books
	def process_prod_snapshot(self,msg,recv):
		print("calling process_prod_snapshot")
		# all the i_<var> points to one field in the message
		# there are also used in the function vali to start searching
		# from them and not from the beginning to speed up the decoding
		i_lastmsn=vali(msg,'LastMsgSeqNumProcessed')
		self.msn = int(msg[i_lastmsn])
		i_uid=vali(msg,'SecurityID',i_lastmsn)
		uid = msg[i_uid]
		if msg[i_uid] not in self.inst: # ignore instruments which are not in the database
			return
		if msg[i_uid] not in self.inst_init: # check if uid already done
			self.inst_init.add(msg[i_uid])

		i_prodcomplex=vali(msg,'ProductComplex',i_uid)
		i_lastupdtime=vali(msg,'LastUpdateTime',i_prodcomplex)
		N = int(val(msg,'NoMDEntries',i_lastupdtime))
		I = i_lastupdtime # index to track position in msg

		for i in range(N): # process each entry for instrument uid
			i_entrytype = vali(msg,'MDEntryType',I)
			I = i_entrytype+1 # I points at the next key (...:key:value:next key:...) in the list
			i_next_entrytype = vali(msg,'MDEntryType',I)-1 # point to the next MDEntryType key
			if msg[i_entrytype]=='0' or msg[i_entrytype]=='1': # add bid or ask level
				i_mdbooktype = vali(msg,'MDBookType',I)
				if i_mdbooktype < i_next_entrytype and msg[i_mdbooktype]=='1': # price depth
					i_mdentrypx  =vali(msg,'MDEntryPx',I) # get values
					i_mdentrysize=vali(msg,'MDEntrySize',I)
					i_nborders   =vali(msg,'NumberOfOrders',I)
					i_mdpricelvl =vali(msg,'MDPriceLevel',I)
					i_mdentrytime=vali(msg,'MDEntryTime',I)
					# check they belong to the current MDSshGrp starting at i_entrytype
					# and ending right before i_next_entrytype
					side  = int(msg[i_entrytype])
					price = Decimal(msg[i_mdentrypx]) if i_mdentrypx<i_next_entrytype else float('nan')
					size  = int(msg[i_mdentrysize]) if i_mdentrysize<i_next_entrytype else float('nan')
					nbo   = int(msg[i_nborders])    if i_nborders<i_next_entrytype    else float('nan')
					level = int(msg[i_mdpricelvl])  if i_mdpricelvl<i_next_entrytype  else float('nan')
					ts    = int(msg[i_mdentrytime]) if i_mdentrytime<i_next_entrytype else float('nan')
					# And do an update of the order book
					self.inst[uid].add_level(level-1,side,size,price,nbo)
					self.inst[uid].store_update("Q",recv,ts)
			elif msg[i_entrytype]=='3': # clear book
				i_mdentrytime = vali(msg,'MDEntryTime',I)
				ts = int(msg[i_mdentrytime]) if i_mdentrytime<i_next_entrytype else float('nan')
				self.inst[uid].clear(recv,ts)

			I = i_next_entrytype # jump to the next MDEntryType

	# Process queue of Depth Incremental messages. It is used during when waiting for the
	# snapshot completion or if there is a gap
	def process_inc_queue(self,recv):
		print("calling process_inc_queue")
		processed=True
		# Fixed point algorithm to process each element in the queue
		while processed:
			processed=False
			i=0
			n = len(self.queue)
			while i < n:
				if self.queue[i][0] == 'DepthIncremental':
					status = self.process_inc(self.queue[i],recv)
				elif self.queue[i][0] in DI:
					status = self.process_non_inc(self.queue[i])
				else:
					status = False

				if status:
					del self.queue[i]
					n=n-1
					processed=True
				else:
					i=i+1

	# Core function to decode and process a FAST message
	# Return True when the packet has been processed
	# False when there is a gap. It assumes UDP are in order already
	# as given by generic_decoder. So a gap will cause the product to go
	# back in snapshot mode immediately
	def process_inc(self,msg,recv):
		print("calling process_inc")
		msn = int(val(msg,'MsgSeqNum'))
		if msn<=self.msn: # already processed
			print("too late")
			return True
		elif msn > self.msn+1: # too early
			print("wrong seq")
			return False
		print('process_inc: msn is good',self.msn,'->',msn)
		self.msn = msn # else msn==self.msn+1, process it
		try:
			N = int(val(msg,'NoMDEntries'))
		except ValueError:
			print(recv)
			print(msg)
			sys.exit(1)
		I=0
		print("mdentries=",N)
		for i in range(N):
			i_upd_act=vali(msg,'MDUpdateAction',I) # beginning of msg in msg
			I=i_upd_act+1 # where to start to search for following fields
			i_next_upd_act=vali(msg,'MDUpdateAction',i_upd_act) # where to end
			print("i=",i,"i_upd_act=",i_upd_act,"i_next_upd_act=",i_next_upd_act,"I=",I)
			uid=val(msg,'SecurityID',I)
			if uid not in self.inst: # do not process unused instruments
				continue

			entry_type=int(val(msg,'MDEntryType',I))
			print('uid=',uid,'entry_type=',entry_type,end=' ')
			qty = val(msg,'MDEntrySize',I,float('nan'),int)
			exch  = int(val(msg,'MDEntryTime',I))
			if entry_type < 3: # bid or ask order
				price = Decimal(val(msg,'MDEntryPx',I))
				nbo = val(msg,'NumberOfOrders',I,float('nan'),int)
				level = int(val(msg,'MDPriceLevel',I))-1 # Book.py takes levels from 0, Eurex from 1
				action  = int(msg[i_upd_act])
				print(price,nbo,level,'action=',action)
				if action==0: # new level
					self.inst[uid].add_level(level,entry_type,qty,price,nbo)
					self.inst[uid].store_update("A",recv,exch)
				elif action==1: # change volume in a level
					self.inst[uid].amend_level(level,entry_type,qty,price,nbo)
					self.inst[uid].store_update("V",recv,exch)
				elif action==2: # delete a price level
					self.inst[uid].delete_level(level,entry_type)
					self.inst[uid].store_update("D",recv,exch)
				elif action==3: # delete from 1 to level
					self.inst[uid].delete_to_level(level,entry_type)
					self.inst[uid].store_update("H",recv,exch)
				elif action==4: # delete from level to end
					self.inst[uid].delete_from_level(level,entry_type)
					self.inst[uid].store_update("F",recv,exch)
				elif action==5: # change price of a level
					self.inst[uid].amend_level(level,entry_type,qty,price,nbo)
					self.inst[uid].store_update("O",recv,exch)
			elif entry_type == 2: # trade
				print("trade",uid)
				self.inst[uid].report_trade(val(msg,'MDEntryPx',I,float('nan'),Decimal), qty, recv, exch,
					val(msg,'AggressorSide',I,float('nan'),int),
					val(msg,'NumberOfOrders',I,float('nan'),int),
					val(msg,'AggressorTimestamp',I,float('nan'),int),
					val(msg,'NumberOfBuyOrders',I,float('nan'),int),
					val(msg,'NumberOfSellOrders',I,float('nan'),int))
			elif entry_type == 3: # clear book
				print('clear book')
				self.inst[uid].clear(recv,exch)
				self.inst[uid].store_update("C",recv,exch)
		return True

	def process_non_inc(self,msg):
		print("calling process_non_inc")
		msn = int(val(msg,'MsgSeqNum'))
		if msn<=self.msn: # already processed
			return True
		elif msn > self.msn+1: # too early
			return False
		else: # msn==self.msn+1
			self.msn = msn
			return True

######################################################################################
# the EMDI decoder
######################################################################################

class emdi_decoder(generic_decoder):
	def __init__(self,instfile,date,outputdir='./'):
		generic_decoder.__init__(self)
		self.prod = {}


		# Read the products' database and instantiate all the
		# products and instruments
		emdi_date = dateutil.parser.parse("{}-{}-{}".format(date[0:4],date[4:6],date[6:8]))
		for row in csv.DictReader(open(instfile)):
			# take only active (==0) simple instruments (<2)
			st = row['SecurityType']
			if (st=='O' or st=='F') and int(row['SecurityStatus'])==0:
			# check date <= maturity
				maturity = dateutil.parser.parse("{}-{}-{}".format(
					row['MaturityDate'][0:4],
					row['MaturityDate'][4:6],
					row['MaturityDate'][6:8]))
				if emdi_date <= maturity:
					pid = row['MarketSegmentID']
					if pid not in self.prod: # Create product
						self.prod[pid] = Product()
					uid = row['SecurityID']
					# make file name
					filename =( outputdir+'/'+row['MarketSegment']+row['MaturityMonthYear']
							+'-'+row['StrikePrice']+'-'
							+ ('Put' if row['PutOrCall']=='P' else 'Call')
							+'_'+date+'.csv')
					# create Order Book for each instrument
					self.prod[pid].inst[uid] = Book(uid,date,5,"level_2",ofile=filename)

	def output(self):
		# output remaining data
		for p in self.prod:
			for u in self.prod[p].inst:
				self.prod[p].inst[u].write_output()

	def process_messages(self,packet,found_gap):
		recv = int(packet[0][1]) # UDP receive timestamps
		exch = int(val(packet[1],"SendingTime"))
		for i in range(2,len(packet)):
			pid = val(packet[i],'MarketSegmentID') # get product id 
			if pid not in self.prod: # filter out products
				continue
			prod = self.prod[pid]
			print("prod",pid,recv,exch,"len=",len(packet)-2)

			# Run FSA (states=init, snapshot, incremental)
			if prod.state == state_init:
				print("init : packet[0]=",packet[i][0],"=")
				if packet[i][0]=='DepthSnapshot': # start snapshot
					prod.process_prod_snapshot(packet[i],recv)
					prod.state = state_snapshot
					prod.first_inst = val(packet[i],'SecurityID')
					print("prod",pid,"init->snapshot",recv)
				elif packet[i][0] in DI: # save message in queue
					prod.queue.append(packet[i])
					print("prod",pid,"saving inc while in init",recv)
			elif prod.state == state_snapshot:
				if packet[i][0]=='DepthSnapshot': # continue snapshot
					if (len(prod.inst)==len(prod.inst_init) or # check for end of snapshot
							val(packet[i],'SecurityID')==prod.first_inst):
						print("end of snapshot",pid)
						print("len=",len(prod.inst),len(prod.inst_init))
						print(val(packet[i],'SecurityID'), prod.first_inst)
						prod.process_inc_queue(recv) # process queued msg
						prod.state=state_incremental # go to incremental
						prod.first_inst = -1
						print("prod",pid,"complete snapshot->incremental",recv)
					else:
						prod.process_prod_snapshot(packet[i],recv)
						print("we have",len(prod.inst_init),"instruments over",len(prod.inst))
						print("prod",pid,"process snapshot",recv)
				elif packet[i][0] in DI: # save msg in queue
					prod.queue.append(packet[i])
					print("prod",pid,"saving inc while in snapshot",recv)
			elif prod.state == state_incremental:
				if packet[i][0]=='DepthIncremental':
					print('found incremental')
					if not prod.process_inc(packet[i],recv):
						prod.reset()
						print("prod",pid,"wrong seq with DI incremental->init",recv)
					else:
						print("prod",pid,"in seq",prod.msn,recv)
				elif packet[i][0] in DI:
					if not prod.process_non_inc(packet[i]):
						prod.reset()
						print("prod",pid,"wrong seq with nonDI incremental->init",recv)
					else:
						print("prod",pid,"in seq",prod.msn,recv)

######################################################################################

class dummy_decoder:
	def __init__(self):
		self.nb_packets=0

	def process_udp_datagram(self,packet):
		self.nb_packets+=1

	def output(self):
		print("# packets=",self.nb_packets)

######################################################################################

def parse_text(namea,nameb,packet_decoder):
	""" Parse 2 files for channel A and B of FAST packet and stop when data
	are exhausted on both channels
	"""
	filea=open(namea,buffering=1<<27)
	fileb=open(nameb,buffering=1<<27)
	packeta=[]
	packetb=[]
	ia=0
	ib=0
	
	print("start at ",datetime.now().strftime("%H:%M:%S"),file=sys.stderr)
	read = True
	while read:
		read = False

		# channel A
		line = filea.readline()
		if line:
			read = True
			ia+=1
			if line[0:3]=='UDP' and len(packeta)>0: # check for new UDP datagram
				if len(packeta)>1: #some packets are empty !!!
					print("----- channel A -----")
					packet_decoder.process_udp_datagram(packeta)
					print("----- end of channel A -----")
				packeta.clear()
			# don't store empty lines 
			if line[0]!='\n' and line[0]!=' ' and line[0]!='':
				packeta.append(line.split(':'))

		# channel B
		line = fileb.readline()
		if line:
			read = True
			ib+=1
			if line[0:3]=='UDP' and len(packetb)>0: # check for new UDP datagram
				if len(packetb)>1: #some packets are empty !!!
					print("----- channel B -----")
					packet_decoder.process_udp_datagram(packetb)
					print("----- end of channel B -----")
				packetb.clear()
			# don't store empty lines 
			if line[0]!='\n' and line[0]!=' ' and line[0]!='':
				packetb.append(line.split(':'))

		if ia % 1000000 == 0:
			print("ia=",ia," ",datetime.now().strftime("%H:%M:%S"),file=sys.stderr)
		if ib % 1000000 == 0:
			print("ib=",ib," ",datetime.now().strftime("%H:%M:%S"),file=sys.stderr)

	# finish output
	packet_decoder.output()

######################################################################################

def usage():
	print("Usage: eurex_EMDI_decoder.py [options] <channel_a> <channel_b>",file=sys.stderr)
	print("-h,--help print help",file=sys.stderr)
	print("-d,--decoder either rdd or emdi",file=sys.stderr)
	print("-p,--product product file when using emdi decoder",file=sys.stderr)
	print("-o,--output outputdir",file=sys.stderr)
	print("-t,--date date of the files to process",file=sys.stderr)
	print("\nproduct is only required by the emdi decoder",file=sys.stderr)

def main(argv):
	# Get options
	try:
		opts, args = getopt.getopt(argv,"hd:p:o:t:", ["help","decoder=","product=","output=","date="])
	except getopt.GetoptError:
		usage()
		sys.exit(1)

	for opt, arg in opts:
		if opt in ("-h","--help"):
			usage()
			sys.exit(0)
		elif opt in ("-d","--decoder"):
			decoder = arg
		elif opt in ("-p","--product"):
			product = arg
		elif opt in ("-o","--output"):
			output_dir = arg
		elif opt in ("-t","--date"):
			date = arg

	# Test options
	if 'decoder' not in locals():
		print("decoder not specified",file=sys.stderr)
		usage()
		sys.exit(1)
	if 'output_dir' not in locals():
		print("output dir not specified",file=sys.stderr)
		usage()
		sys.exit(1)
	if 'date' not in locals():
		print("date not specified",file=sys.stderr)
		usage()
		sys.exit(1)

	# Run decoder
	if decoder == 'rdd':
		x = rdd_decoder(output_dir, date)
	elif decoder == 'emdi':
		x = emdi_decoder(product, date, output_dir)
	elif decoder == 'generic':
		x = generic_decoder()
	else:
		print("decoder",decoder,"is not valid",file=sys.stderr)
		sys.exit(1)

	parse_text(args[0],args[1], x)

if __name__=="__main__":
	main(sys.argv[1:])
