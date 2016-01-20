#!/usr/bin/env python3
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
state_reading_datagram=1
state_wait_for_snapshot=2

# RDD
state_snapshot_cycle = 3
state_product_cycle = 4

# EMDI
state_incremental = 5
state_snapshot = 6

# return val of key in list
# or NA if not found
def val(_list,key,start=0):
	try:
		return _list[_list.index(key,start)+1]
	except:
		return 'NA'

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
		self.max_queue=40
		self.nb_packet=0

	# This function is called at the end of the decoding
	def output(self):
		print("# packets=",self.nb_packet)
	
	# This function is called on each FAST messages that is each line
	# of a UDP datagram except the first 2
	def process_messages(self,packet,found_gap):
		self.nb_packet+=1
		print(packet[0][3],",",packet[1][2],",",packet[1][4])

	def search_new_psn(self,scid):
		minpsn = sys.maxsize
		for p in self.queue[scid]:
			psn = int(p[1][4]) # PacketSeqNum
			print(psn)
			sys.exit(1)
			if psn < minpsn:
				minpsn = psn

		return minpsn-1

	# Check contiguity of UDP datagram
	def contiguity(self,scid,new_psn):
		if scid not in self.packetseqnum: # new scid
			self.packetseqnum[scid] = new_psn # create structures
			self.queue[scid] = []
			return 0
		else:
			last_psn= self.packetseqnum[scid]
			if new_psn == (last_psn+1):
				return 0 # on time
			elif new_psn > (last_psn+1):
				return 1 # too early
			else:
				return -1 # too late

	# Process a queue of UDP datagram in case of a out-of-order
	# packets
	def process_queue(self,scid):
		processed=1
		while processed>0:
			processed=0
			i=0
			while i<len(self.queue[scid]):
				p = self.queue[scid][i]
				new_psn = int(p[1][4]) # PacketSeqNum
				cont = self.contiguity(scid,new_psn)
				if cont == 0:
					self.packetseqnum[scid] = new_psn
					self.process_messages(p,False)
					del self.queue[scid][i]
					processed+=1
				else:
					i+=1
	
	# Process a UDP datagram and re-order them if needed
	# When in a child class, the process message of the child is called
	def process_udp_datagram(self,packet):
		try:
			scid = packet[0][3] + packet[1][3] # dport + SenderCompID
		except:
			print(packet)
			sys.exit(1)
		new_psn = int(packet[1][4]) # PacketSeqNum
		
		# check datagram contiguity
		cont = self.contiguity(scid,new_psn)
		if cont == 0: # on time
			self.packetseqnum[scid] = new_psn
			self.process_messages(packet,False) # process packet
			if len(self.queue[scid])>0: # process queue if any
				self.process_queue(scid)
		elif cont== 1: # too early
			psn_in_queue= (int(x[1][4]) for x in self.queue[scid]) # x[1][4] PacketSeqNum
			if new_psn not in psn_in_queue:
				self.queue[scid].append(deepcopy(packet)) # bufferise packet

		# gap in data
		if len(self.queue[scid])==self.max_queue:
			min_psn = sys.maxsize # find packet with smallest psn
			min_i = 0
			for i in range(0,len(self.queue[scid])):
				psn = int(self.queue[scid][i][1][4]) # PacketSeqNum
				if psn < min_psn:
					min_psn=psn
					min_i=i
			# and restart from there
			self.packetseqnum[scid] = min_psn
			self.process_messages(self.queue[scid][min_i],True)
			del self.queue[scid][min_i]
			self.process_queue(scid)

#################################################################
# RDD DECODER
# The result is a daily database of products sent by the exchange
#################################################################

def product_complex(it):
	if it=='1':
		return 'Simple Instrument'
	elif it=='2':
		return 'Standard Option Strategy'
	elif it=='3':
		return 'Non-Standard Option Strategy'
	elif it=='4':
		return 'Volatility Strategy'
	elif it=='5':
		return 'Future Spread'
	elif it=='6':
		return 'Inter-Product Spread'
	elif it=='7':
		return 'Standard Futures Strategy'
	elif it=='8':
		return 'Pack and Bundle'
	elif it=='9':
		return 'Strip'

class rdd_decoder(generic_decoder):
	def __init__(self):
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
		self.inst_fields=['product', 'SecurityID','SecurityType', 'SecuritySubType', 
			'ProductComplex', 'MaturityDate', 'MaturityMonthYear',
			'StrikePrice', 'PricePrecision', 'ContractMultiplier',
			'PutOrCall', 'ExerciseStyle', 'SettlMethod', 'SecurityDesc',
			'MinPriceIncrement', 'MinPriceIncrementAmount','SecurityStatus']

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
			self.inst[uid]['SecurityType']=val(msg,'SecurityType')
			self.inst[uid]['SecuritySubType']=val(msg,'SecuritySubType')
			self.inst[uid]['ProductComplex']=product_complex(val(msg,'ProductComplex'))
			self.inst[uid]['MaturityDate']=val(msg,'MaturityDate')
			self.inst[uid]['MaturityMonthYear']=val(msg,'MaturityMonthYear')
			self.inst[uid]['StrikePrice']=val(msg,'StrikePrice')
			self.inst[uid]['PricePrecision']=val(msg,'PricePrecision')
			self.inst[uid]['ContractMultiplier']=val(msg,'ContractMultiplier')
			self.inst[uid]['PutOrCall']=val(msg,'PutOrCall')
			self.inst[uid]['ExerciseStyle']=val(msg,'ExerciseStyle')
			self.inst[uid]['SettlMethod']=val(msg,'SettlMethod')
			self.inst[uid]['SecurityDesc']=val(msg,'SecurityDesc')
			self.inst[uid]['MinPriceIncrement']=val(msg,'MinPriceIncrement')
			self.inst[uid]['MinPriceIncrementAmount']=val(msg,'MinPriceIncrementAmount')
			self.inst[uid]['SecurityStatus']=val(msg,'SecurityStatus')

	def output(self):
		print(','.join(self.product_fields),end='') # header product
		print(',',end='')
		print(','.join(self.inst_fields)) # header instrument

		for i in self.inst:
			p = self.inst[i]['product']
			# print product data
			print(','.join(
				(str(self.product[p][x]) if x!='MarketSegmentDesc'
					else '"'+str(self.product[p][x])+'"')
				for x in self.product_fields),end='')
			print(',',end='')
			# print instrument data
			print(','.join(str(self.inst[i][x]) for x in self.inst_fields))

	def process_messages(self,packet,found_gap):
		for i in range(2,len(packet)): # useful messages start at packet[2]
			# Run FSA
			if self.state == state_init: # Wait for a snapshot to start
				if packet[i][0]=='MarketDataReport':
					if val(packet[i],'MDReportEvent')=='1':
						self.state = state_snapshot_cycle
			# Read all snapshot messages
			elif self.state == state_snapshot_cycle:
				if found_gap:
					self.state = state_init
				elif packet[i][0]=='MarketDataReport':
					mdreportevent = val(packet[i],'MDReportEvent')
					if mdreportevent!='0' and mdreportevent!='1':
						self.state = state_init
				elif packet[i][0]=='ProductSnapshot':
					self.state = state_product_cycle
					msgseqnum = int(val(packet[i],'MsgSeqNum'))
					if self.msgseqnum==-1 or self.msgseqnum==(msgseqnum-1):
						self.msgseqnum = msgseqnum
						self.create_product(packet[i])
			# Read all snapshot messages for one product
			elif self.state == state_product_cycle:
				if found_gap:
					self.state = state_snapshot_cycle
				elif packet[i][0]=='InstrumentSnapshot':
					self.create_inst(packet[i])
				elif packet[i][0]=='ProductSnapshot':
					self.create_product(packet[i])

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

	# True if we received all initial book for this product
	def snapshot_complete(self):
		return len(self.inst)==len(self.inst_init)

	# Decode and initialize instruments' books
	def process_prod_snapshot(self,msg,recv):
		# all the i_<var> points to one field in the message
		# there are also used in the function vali to start searching
		# from them and not from the beginning to speed up the decoding
		i_lastmsn=vali(msg,'LastMsgSeqNumProcessed')
		self.msn = int(msg[i_lastmsn])
		i_uid=vali(msg,'SecurityID',i_lastmsn)
		if msg[i_uid] in self.inst_init: # check if uid already done
			return True
		else:
			self.inst_init.add(msg[i_uid])

		i_prodcomplex=vali(msg,'ProductComplex',i_uid)
		i_lastupdtime=vali(msg,'LastUpdateTime',i_prodcomplex)
		N = int(val(msg,'NoMDEntries',i_lastupdtime))
		I = i_lastupdtime # index to track position in msg

		for i in range(N): # process each entry for instrument uid
			i_entrytype = vali(msg,'MDEntryType',I)
			I = i_entrytype+1 # I points at the next key (...:key:value:...) in the list
			i_next_entrytype = vali(msg,'MDEntryType',I)-1 # point to the next MDEntryType key
			if msg[i_entrytype]=='0' or msg[i_entrytype]=='1': # add bid or ask level
				i_mdbooktype = vali(msg,'MDBookType',I)
				if i_mdbooktype < i_next_entrytype and msg[i_mdbooktype]=='2': # price depth
					i_mdentrypx  =vali(msg,'MDEntryPx',I) # get values
					i_mdentrysize=vali(msg,'MDEntrySize',I)
					i_nborders   =vali(msg,'NumberOfOrders',I)
					i_mdpricelvl =vali(msg,'MDPriceLevel',I)
					i_mdentrytime=vali(msg,'MDEntryTime',I)
					# check they belong to the current MDSshGrp starting at i_entrytype
					# and ending right before i_next_entrytype
					side  = msg[i_entrytype]
					price = msg[i_mdentrypx] if i_mdentrypx<i_next_entrytype else 'NA'
					size  = msg[i_mdentrysize] if i_mdentrysize<i_next_entrytype else 'NA'
					nbo   = msg[i_nborders] if i_nborders<i_next_entrytype else 'NA'
					level = msg[i_mdpricelvl] if i_mdpricelvl<i_next_entrytype else 'NA'
					ts    = msg[i_mdentrytime] if i_mdentrytime<i_next_entrytype else 'NA'
					# And do an update of the order book
					self.inst[uid].add_level(int(level),int(side),int(size),
							Decimal(price),int(nbo))
					self.inst[uid].store_update("A",recv,int(ts))
			elif msg[i_entrytype]=='J': # clear book
				i_mdentrytime=vali(msg,'MDEntryTime',I)
				ts    = msg[i_mdentrytime] if i_mdentrytime<i_next_entrytype else 'NA'
				inst[uid].clear(recv,int(ts))

			I = i_next_entrytype # jump to the next MDEntryType

		return True

	# Process queue of Depth Incremental messages. It is used during when waiting for the
	# snapshot completion or if there is a gap
	def process_inc_queue(self,recv):
		processed=True
		# Fixed point algorithm to process each element in the queue
		while processed:
			processed=False
			i=0
			n = len(self.queue)
			while i < n:
				if self.process_inc(self.queue[i],recv):
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
		msn = int(val(msg,'MsgSeqNum'))
		if msn<=self.msn: # already processed
			return True
		elif msn > self.msn+1: # too early
			return False
		# else msn==self.msn+1, process it
		N = int(val(msg,'NoMDEntries'))
		for i in range(N):
			i_upd_act=ival(msg,'MDUpdateAction')
			I=i_upd_act+1
			i_next_upd_act=ival(msg,'MDUpdateAction',i_upd_act)
			entry_type=val(msg,'MDEntryType',I)
			if int(entry_type) < 2: # bid or ask order
				uid=val(msg,'SecurityID',I)
				price = Decimal(val(msg,'MDEntryPx',I))
				qty   = int(val(msg,'MDEntrySize',I))
				nbo   = int(ival(msg,'NumberOfOrders',I))
				level = int(val(msg,'MDPriceLevel',I))
				exch  = int(ival(msg,'MDEntryTime',I))
				action  = int(msg[i_upd_act])
				if action==0: # new level
					self.inst[uid].add_level(level,int(entry_type),qty,price,nbo)
					self.inst[uid].store_update("A",recv,exch)
				elif action==1: # change volume in a level
					self.inst[uid].amend_level(level,int(entry_type),qty,price,nbo)
					self.inst[uid].store_update("V",recv,exch)
				elif action==2: # delete a price level
					self.inst[uid].delete_level(level,int(entry_type))
					self.inst[uid].store_update("D",recv,exch)
				elif action==3: # delete from 1 to level
					for i in range(1,level+1):
						self.inst[uid].delete_level(1,int(entry_type))
					self.inst[uid].store_update("H",recv,exch)
				elif action==4: # delete from level to end
					for i in range(level,self.inst[uid].levels+1):
						self.inst[uid].delete_level(level,int(entry_type))
					self.inst[uid].store_update("F",recv,exch)
				elif action==5: # change price of a level
					self.inst[uid].delete_level(level,int(entry_type))
					self.inst[uid].add_level(level,int(entry_type),qty,price,nbo)
					self.inst[uid].store_update("O",recv,exch)
			elif entry_type == '2': # trade
				self.inst[uid].report_trade(
					Decimal(val(msg,'MDEntryPx',I)),
					int(val(msg,'MDEntrySize',I)),
					recv, int(val(msg,'MDEntryTime',I)),
					int(val(msg,'AggressorSide',I)),
					int(val(msg,'NumberOfOrders',I)),
					int(val(msg,'AggressorTimestamp',I)),
					int(val(msg,'NumberOfBuyOrders',I)),
					int(val(msg,'NumberOfSellOrders',I)))
			elif entry_type == '3': # clear book
				self.inst[uid].clear(recv,int(val(msg,'MDEntryTime',I)))
				self.inst[uid].store_update("C",recv,exch)
		return True

	def process_non_inc(self,msg):
		msn = int(val(msg,'MsgSeqNum'))
		if msn<=self.msn: # already processed
			return True
		elif msn > self.msn+1: # too early
			return False
		else: # msn==self.msn+1
			return True

######################################################################################
# Implements the EMDI decoder
######################################################################################

class emdi_decoder(generic_decoder):
	def __init__(self,instfile,date,outputdir='./'):
		generic_decoder.__init__(self)

		emdi_date = dateutil.parser.parse("{}-{}-{}".format(date[0:4],date[4:6],date[6:8]))
		self.prod = {}
		self.DI = ['DepthIncremental',
					'ComplexInstrumentUpdate',
					'CrossRequest',
					'QuoteRequest',
					'InstrumentStateChange',
					'ProductStateChange',
					'MassInstrumentStateChange',
					'TopOfBookImplied']

		# Read the products' database and instantiate all the
		# products and instruments
		for row in csv.DictReader(open(instfile)):
			# take only active (==0) simple instruments (<2)
			if int(row['SecurityType'])<2 and int(row['SecurityStatus'])==0:
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
					filename =( outputdir+row['MarketSegment']+row['MaturityMonthYear']
							+'-'+row['StrikePrice']+'-'
							+ ('Put' if row['PutOrCall']=='0' else 'Call')
							+'_'+date+'.csv')
					# create Order Book for each instrument
					self.prod[pid].inst[uid] = Book(uid,date,5,"level_2",ofile=filename)

	def output(self):
		# output remaining data
		for p in self.prod:
			for u in self.prod[p].inst:
				self.prod[p].inst[u].write_output()

	def process_messages(self,packet,found_gap):
		recv = packet[0][1] # UDP receive timestamps
		exch = packet[1][8] # SendingTime
		for i in range(2,len(packet)):
			pid = val(packet[i],'MarketSegmentID') # get product id 
			if pid not in self.prod:
				continue
			print("prod ",pid," ",recv," ",exch, "# packets=",len(packet)-2)
			prod = self.prod[pid]
			# Run FSA (states=init, snapshot, incremental)
			if prod.state == state_init:
				if packet[i][0]=='DepthSnapshot': # start snapshot
					prod.process_prod_snapshot(packet[i],recv)
					prod.state = state_snapshot
					print("prod ",pid," init->snapshot")
				elif packet[i][0] in self.DI: # save message in queue
					prod.queue.append(packet[i])
					print("prod ",pid," saving inc while in init")
			elif prod.state == state_snapshot:
				if packet[i][0]=='DepthSnapshot': # continue snapshot
					if not prod.process_prod_snapshot(packet[i],recv):
						print("prod ",pid," wrong seq snapshot->init")
						prod.state = state_init
					elif prod.snapshot_complete(): # product complete ?
						prod.process_inc_queue(recv) # process queued msg
						prod.state=product_incremental # go to incremental
						print("prod ",pid," complete snapshot->incremental")
				elif packet[i][0] in self.DI: # save msg in queue
					prod.queue.append(packet[i])
					print("prod ",pid," saving inc while in snapshot")
			elif prod.state == state_incremental:
				if packet[i][0]=='DepthIncremental':
					if not prod.process_inc(packet[i],recv):
						prod.state = state_init
						print("prod ",pid," wrong seq with DI incremental->init")
				elif packet[i][0] in self.DI:
					if not prod.process_non_inc(packet[i]):
						prod.state = state_init
						print("prod ",pid," wrong seq with nonDI incremental->init")

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
					packet_decoder.process_udp_datagram(packeta)
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
					packet_decoder.process_udp_datagram(packetb)
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

if __name__=="__main__":
#	parser = argparse.ArgumentParser(__file__)
#	parser.add_argument("--verbose","-v", help="verbose mode", type=bool, default=False)
#	parser.add_argument("tarfile",help="input tar file name",type=str)
#	args = parser.parse_args()

#	x=rdd_decoder()
#	parse_text('/mnt/data/david/RDDa','/mnt/data/david/RDDb',x)

	x=emdi_decoder("prod.csv","20150924")
	parse_text('/mnt/data/david/EMDIa','/mnt/data/david/EMDIb',x)

#######################################################################################
#def line2msg(string):
#	l = string[:-1].split(':')
#	data = {}
#
#	for i in range(0,len(l),2):
#		key = l[i]
#		if key in data:
#			data[key].append(l[i+1])
#		else:
#			data[key] = [l[i+1]]
#
#	d1 = {k+'.'+str(i):data[k][i] for k in data if len(data[k])>1 for i in range(len(data[k]))}
#	d1.update({k:data[k][0] for k in data if len(data[k])==1})
#	return d1

def unpack_tar_file(tfile,tmpdir='/tmp'):
	if os.path.exists(tfile) and os.path.isfile(tfile) and tarfile.is_tarfile(tfile):
		# make a uniquely named temp directory
		uniqdir = tmpdir+'/'+str(uuid.uuid4())
		os.mkdir(uniqdir)

		print("unpacking tar file "+tfile+" in dir "+uniqdir)
		print("starting at ", datetime.now())

		# extract files
		t = tarfile.open(tfile)
		t.extractall(uniqdir)
		t.close()

		print("done at ", datetime.now())
	
		return uniqdir

def which(program):
	""" Find executable path """
	def is_exe(fpath):
		return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

	fpath, fname = os.path.split(program)
	if fpath:
		if is_exe(program):
			return program
	else:
		for path in os.environ["PATH"].split(os.pathsep):
			path = path.strip('"')
			exe_file = os.path.join(path, program)
			if is_exe(exe_file):
				return exe_file
	return None

def decode_emdi_pcap_data(tarfile,prefix,tmpdir='/tmp'):
	# unpack tar file to /tmp
	print(datetime.now(), " unpacking tar file ", tarfile)
	emdi_dir = unpack_tar_file(tarfile)
	#emdi_dir=tmpdir
	print(datetime.now(), " emdi dir: ",emdi_dir)

	# get pcap files list
	filesa=''
	filesb=''
	for f in sorted(os.listdir(emdi_dir+'/emdi-a')):
		filesa += ' ' + emdi_dir+'/emdi-a/'+f
	for f in sorted(os.listdir(emdi_dir+'/emdi-b')):
		filesb += ' ' + emdi_dir+'/emdi-b/'+f

	# result files
	outputa = tmpdir+'/'+prefix+'_a_'+uuid.uuid4().hex
	outputb = tmpdir+'/'+prefix+'_b_'+uuid.uuid4().hex

	print("Results are in "+outputa+" and "+outputb)

	# generate command
	nbcpu = str(os.cpu_count())
	parallela=(which("parallel") + " -j " + nbcpu
				+ ' --keep-order pcapread -f compact '+prefix+' {} ::: ' 
				+ filesa
				+ ' > ' + outputa)
	parallelb=(which("parallel") + " -j " + nbcpu
				+ ' --keep-order pcapread -f compact '+prefix+' {} ::: ' 
				+ filesb
				+ ' > ' + outputb)

	# run command
	print("start pcap on channel A decoding at ", datetime.now())
	os.system(parallela)
	print("start pcap on channel B decoding at ", datetime.now())
	os.system(parallelb)
	print("done at ",datetime.now())
	print("output files are ",outputa, " " ,outputb)

	# remove untar'red pcap files
	shutil.rmtree(emdi_dir,True)

	return (outputa,outputb)

