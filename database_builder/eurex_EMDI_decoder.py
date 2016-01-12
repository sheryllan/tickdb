#!/usr/bin/env python3
import uuid
import os
import tarfile
import shutil
import sys
import time
import argparse
from Book import *
import csv
from copy import deepcopy
from datetime import *
import line2msg

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
state_incremental_cycle = 5

# Channels
channel_A=1
channel_B=2

def keyname(s,i,N):
	return s if N==1 else s+"."+str(i)

class generic_decoder:
	def __init__(self,header_type):
		self.udp_state = { channel_A : state_init, channel_B:state_init}
		self.header_type = header_type
		self.packetseqnum = {}
		self.queue = {}
		self.max_queue=10

	def output():
		pass
	
	def process_messages(self,packet,found_gap):
		pass

	def search_new_psn(self,scid):
		minpsn = sys.maxsize
		for p in self.queue[scid]:
			psn = int(p[0]['PacketSeqNum'])
			if psn < minpsn:
				minpsn = psn

		return minpsn-1

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

	def process_queue(self,scid):
		processed=1
		while processed>0:
			processed=0
			i=0
			while i<len(self.queue[scid]):
				p = self.queue[scid][i]
				new_psn = int(p[0]['PacketSeqNum'])
				cont = self.contiguity(scid,new_psn)
				if cont == 0:
					self.packetseqnum[scid] = new_psn
					self.process_messages(p,False)
					del self.queue[scid][i]
					processed+=1
				else:
					i+=1
	
	def print_queue(self):
		for s in self.queue:
			for i in range(0,len(self.queue[s])):
				p = self.queue[s][i]
				psn = int(p[0]['PacketSeqNum'])

	def process_udp_datagram(self,packet):
		# due to a bug in pcapread I couldn't find so far
		# I have to test for dport.1. It seems there is a
		# problem at the end of each pcap file and an empty
		# packet is reported where there should be nothing
		if 'dport.1' in packet[0]: 
			scid = packet[0]['dport.1']+packet[0]['SenderCompID']
		else: 
			scid = packet[0]['dport']+packet[0]['SenderCompID']

		try:
			new_psn = int(packet[0]['PacketSeqNum'])
		except ValueError:
			try: # presumably invalid but try again
				print("problem with PacketSeqNum")
				new_psn = int(packet[0]['PacketSeqNum'],16)
			except:
				print("big problem with PacketSeqNum")
				new_psn = 0 # invalidate completely

		cont = self.contiguity(scid,new_psn)

		if cont == 0: # on time
			self.packetseqnum[scid] = new_psn
			self.process_messages(packet,False)
			if len(self.queue[scid])>0:
				self.process_queue(scid)
		elif cont== 1: # too early
			psn_in_queue= (int(x[0]['PacketSeqNum']) for x in self.queue[scid])
			if new_psn not in psn_in_queue:
				self.queue[scid].append(deepcopy(packet)) # bufferise packet

		if len(self.queue[scid])==self.max_queue:# gap in data
			min_psn = sys.maxsize
			min_i = 0
			for i in range(0,len(self.queue[scid])):
				psn = int(self.queue[scid][i][0]['PacketSeqNum'])
				if psn < min_psn:
					min_psn=psn
					min_i=i

			self.packetseqnum[scid] = min_psn
			self.process_messages(self.queue[scid][min_i],True)
			del self.queue[scid][min_i]
			self.process_queue(scid)

class rdd_decoder(generic_decoder):
	def __init__(self):
		generic_decoder.__init__(self,'RDPacketHeader')
		self.state = state_init
		self.scid = 0
		self.lastseqnum = 0
		self.msgseqnum = -1
		self.product = {}
		self.inst={}
		self.current_pid = ''
		self.product_fields = ['MarketID','MarketSegmentID','MarketSegment',
			'MarketSegmentDesc','MarketSegmentSymbol','Currency',
			'MarketSegmentStatus','PartitionID',
			'UnderlyingSecurityExchange','UnderlyingSymbol','UnderlyingSecurityID']

		self.inst_fields=['SecurityID','SecurityType','SecuritySubType',
				'ProductComplex','MaturityDate','MaturityMonthYear',
				'StrikePrice','PricePrecision','ContractMultiplier','PutOrCall',
				'ExerciseStyle','SettlMethod','SecurityDesc',
				'MinPriceIncrement','MinPriceIncrementAmount']

	def create_product(self,msg):
		pid=msg['MarketSegmentID']
		if pid in self.product:
			return False
		else:
			self.product[pid]={}
			for i in self.product_fields:
				self.product[pid][i] = msg.get(i,'')

			self.current_pid = pid
			return True

	def create_inst(self,msg):
		uid = msg['SecurityID']
		if uid in self.inst:
			return False
		else:
			self.inst[uid] = {}
			self.inst[uid]['product'] = self.current_pid
			for i in self.inst_fields:
				self.inst[uid][i] = msg.get(i,'')

			if 'ProductComplex' in self.inst[uid]:
				it=self.inst[uid]['ProductComplex']
				if   it==1:
					it = 'Simple Instrument'
				elif it==2:
					it = 'Standard Option Strategy'
				elif it==3:
					it = 'Non-Standard Option Strategy'
				elif it==4:
					it = 'Volatility Strategy'
				elif it==5:
					it = 'Future Spread'
				elif it==6:
					it = 'Inter-Product Spread'
				elif it==7:
					it = 'Standard Futures Strategy'
				elif it==8:
					it = 'Pack and Bundle'
				elif it==9:
					it = 'Strip'
				self.inst[uid]['ProductComplex'] = it
		
			return True

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
		exch = 0
		recv = 0
		nb_segments = 0
		nb_inst = 0
		for msg in packet:

			# Get time stamps
			if self.header_type in msg:
				if 'timestamp_arista' in msg:
					recv = msg['timestamp_arista']
				elif 'timestamp_pcap' in msg:
					recv = msg['timestamp_pcap']
				self.scid = int(msg['SenderCompID'])

			# Run FSA
			if self.state == state_init:
				if ('MarketDataReport' in msg) and (msg['MDReportEvent']=='1'):
					self.state = state_snapshot_cycle
					#print("init->snapshot_cycle")

			elif self.state == state_snapshot_cycle:
				if found_gap:
					self.state = state_init
					#print("snapshot_cycle->init GAP")
				elif 'MarketDataReport' in msg and msg['MDReportEvent']=='0':
					self.lastseqnum = int(msg['LastMsgSeqNumProcessed'])
					nb_segments = int(msg['TotNoMarketSegments'])
					nb_inst = int(msg['TotNoInstruments'])
				elif 'ProductSnapshot' in msg:
					#print("snapshot_cycle->product_cycle")
					self.state = state_product_cycle
					i = int(msg['MsgSeqNum'])
					if self.msgseqnum == -1 or (self.msgseqnum==(i-1)):
						self.msgseqnum = i
						self.create_product(msg)

			elif self.state == state_product_cycle:
				if found_gap:
					self.state = state_snapshot_cycle
					#print("product_cycle->snapshot_cycle GAP")
				elif 'InstrumentSnapshot' in msg:
					self.create_inst(msg)
					#print("create inst")
				elif 'ProductSnapshot' in msg:
					self.create_product(msg)
					#print("create prod")
				elif 'MarketDataReport' in msg and msg['MDReportEvent']=='2':
					self.state = state_init
					#print("product_cycle->init MDRE=2")

class Product:
	def __init__(self):
		self.scid = -1
		self.msn = -1
		self.state = state_init
		self.queue = []
		self.inst = {}
		self.first_uid = -1

class emdi_decoder(generic_decoder):
	def __init__(self,instfile,date='today',outputdir='./'):
		generic_decoder.__init__(self,'EMDPacketHeader')
		self.prod = {}
		self.DI = ['DepthIncremental',
					'ComplexInstrumentUpdate',
					'CrossRequest',
					'QuoteRequest',
					'InstrumentStateChange',
					'ProductStateChange',
					'MassInstrumentStateChange',
					'TopOfBookImplied']

		for row in csv.DictReader(open(instfile)):
			if int(row['SecurityType'])<2: # no complex instruments
				pid = row['MarketSegmentID']
				if pid not in self.prod:
					self.prod[pid] = Product()
				uid = row['SecurityID']
				# make file name and open output file
				poc = 'Put' if row['PutOrCall']=='0' else 'Call'
				filename =( outputdir+row['MarketSegment']+row['MaturityMonthYear']
						+'-'+row['StrikePrice']+'-'+poc+'_'+date+'.csv')
				of = open(filename,"at")
				# create Order Book for each instrument
				self.prod[pid].inst[uid] = Book(uid,date,5,"level_2",ofile=of)

	def output(self):
		# output remaining data
		for p in self.prod:
			for u in self.prod[p].inst:
				self.prod[p].inst[u].write_output()

	def update_book(self,pid,uid,action,level,entry,qty,price,nb_orders,recv,exch):
		# order book change
		if action=='0': # new level
			self.prod[pid].inst[uid].add_level(level,int(entry),qty,price,nb_orders)
			self.prod[pid].inst[uid].store_update("A",recv,exch)
		elif action=='1': # change volume in a level
			self.prod[pid].inst[uid].amend_level(level,int(entry),qty,price,nb_orders)
			self.prod[pid].inst[uid].store_update("V",recv,exch)
		elif action=='2': # delete a price level
			self.prod[pid].inst[uid].delete_level(level,int(entry))
			self.prod[pid].inst[uid].store_update("D",recv,exch)
		elif action=='3': # delete from 1 to level
			for i in range(1,level+1):
				self.prod[pid].inst[uid].delete_level(1,int(entry))
			self.prod[pid].inst[uid].store_update("H",recv,exch)
		elif action=='4': # delete from level to end
			for i in range(level,self.prod[pid].inst[uid].levels+1):
				self.prod[pid].inst[uid].delete_level(level,int(entry))
			self.prod[pid].inst[uid].store_update("F",recv,exch)
		elif action=='5': # change price of a level
			self.prod[pid].inst[uid].delete_level(level,int(entry))
			self.prod[pid].inst[uid].add_level(level,int(entry),qty,price,nb_orders)
			self.prod[pid].inst[uid].store_update("O",recv,exch)

	
	def process_snapshot(self,recv,pid,msg,msn):
		uid = msg['SecurityID']
		N = int(msg['NoMDEntries'])
		exch = int(msg['LastUpdateTime'])

		for i in range(0,N):
			entry = msg[keyname('MDEntryType',i,N)]
			if entry=='0' or entry=='1':
				if int(msg.get(keyname('MDBookType',i,N),-1))==2:
					action = '0' # snapshot => add level always
					price = Decimal(msg.get(keyname('MDEntryPx',i,N),-1))
					qty = int(msg.get(keyname('MDEntrySize',i,N),-1))
					nb_orders = int(msg.get(keyname('NumberOfOrders',i,N),-1))
					level = int(msg.get(keyname('MDPriceLevel',i,N),-1))
					self.update_book(pid,uid,action,level,entry,qty,price,nb_orders,recv,exch)
			elif entry=='J':
				self.prod[pid].inst[uid].clear(recv,exch)
				self.prod[pid].inst[uid].store_update("C",recv,exch)

	
	def process_inc(self,recv,pid,msg,msn):
		if 'DepthIncremental' in msg: # only process Incremental
			N = int(msg['NoMDEntries'])
			for i in range(0,N):
				uid = msg[keyname('SecurityID',i,N)]
				if uid not in self.prod[pid].inst:
					continue
				action = msg[keyname('MDUpdateAction',i,N)]
				entry = msg[keyname('MDEntryType',i,N)]
		
				# optional values
				price = Decimal(msg.get(keyname('MDEntryPx',i,N),-1))
				price = Decimal(msg.get(keyname('MDEntryPx',i,N),-1))
				qty = int(msg.get(keyname('MDEntrySize',i,N),-1))
				nb_orders = int(msg.get(keyname('NumberOfOrders',i,N),-1))
				level = int(msg.get(keyname('MDPriceLevel',i,N),-1))
				exch = int(msg.get(keyname('MDEntryTime',i,N),-1))
		
				if entry=='2': # trade
					aggrtime = int(msg.get(keyname('AggressorTimestamp',i,N),-1))
					aggrside = int(msg.get(keyname('AggressorSide',i,N),-1))
					nb_buy = int(msg.get(keyname('NumberOfBuyOrders',i,N),-1))
					nb_sell = int(msg.get(keyname('NumberOfSellOrders',i,N),-1))
					self.prod[pid].inst[uid].report_trade(price,qty,recv,exch,aggrside,
							nb_orders,aggrtime,nb_buy,nb_sell)
				elif entry=='0' or entry=='1': # book update
					self.update_book(pid,uid,action,level,entry,qty,price,nb_orders,recv,exch)
				elif entry=='3': # clear book
					self.prod[pid].inst[uid].clear(recv,exch)
					self.prod[pid].inst[uid].store_update("C",recv,exch)
		#else:
		#	print("found a non-Incremental message msn=",msn)


	
	def process_inc_queue(self,recv,pid):
		#print("### process queue ###")
		#print("lastmsg msn=",self.prod[pid].msn)
		self.prod[pid].queue.sort(key=lambda m : int(m['MsgSeqNum']))
		first_found = False
		for m in self.prod[pid].queue:
			msn = int(m['MsgSeqNum'])
			#print("trying queue msn=",msn)
			if msn>self.prod[pid].msn: # msg can be processed 
				if not first_found:
					#print("first found = ",msn)
					first_found = True
					self.process_inc(recv,pid,m,msn)
					self.prod[pid].msn = msn
				elif self.prod[pid].msn+1 == msn: # contiguous msg
					#print("processing queue msg msn=",msn)
					self.process_inc(recv,pid,m,msn)
					self.prod[pid].msn = msn
		self.prod[pid].queue = []

	
	def process_messages(self,packet,found_gap):
		#print("------------Process message-----------------", found_gap)
		recv = 0
		for msg in packet:
			if self.header_type in msg: # get time stamps
				if 'timestamp_arista' in msg:
					recv = msg['timestamp_arista']
				elif 'timestamp_pcap' in msg:
					recv = msg['timestamp_pcap']
				continue # first line in the packet is only used for timestamps
			elif 'Beacon' in msg: # ignore Beacon messages
				continue

			pid = msg['MarketSegmentID']
			if pid not in self.prod:
				continue

			scid = int(msg['SenderCompID'])
			msn = int(msg.get('MsgSeqNum'))

			if self.prod[pid].state == state_incremental_cycle:
				if any(i in msg for i in self.DI): # instrument message
					#print("processing inc message")
					if msn == self.prod[pid].msn+1:
						self.process_inc(recv,pid,msg,msn)
						#print("prod ",pid," DI incremental -> incremental msn=",msn)
						self.prod[pid].msn = msn # apply to all messages
					elif msn > self.prod[pid].msn+1:
						self.prod[pid].scid=-1
						self.prod[pid].msn = -1
						self.prod[pid].state = state_init
						self.prod[pid].queue = []
						self.prod[pid].inst_map = {}
						print("prod ",pid," found gap incremental -> init msn=",msn," was ",self.prod[pid].msn)
			elif self.prod[pid].state == state_snapshot_cycle:
				if any(i in msg for i in self.DI): # instrument message
					self.prod[pid].queue.append(msg)
					#print("prod ",pid," DI snapshot -> snapshot len=",len(self.prod[pid].queue))
				elif 'DepthSnapshot' in msg:
					uid = msg['SecurityID']
					if uid == self.prod[pid].first_uid: # end of snapshot
						self.prod[pid].state = state_incremental_cycle
						self.process_inc_queue(recv,pid)
						#print("prod ",pid," snapshot -> incremental first uid=",uid)
					else:
						self.process_snapshot(recv,pid,msg,msn)
						self.prod[pid].msn = int(msg['LastMsgSeqNumProcessed'])
						#print("prod ",pid," snapshot -> snapshot last msn=",msn)
			elif self.prod[pid].state == state_init:
				if any(i in msg for i in self.DI): # instrument message
					self.prod[pid].queue.append(msg)
					#print("prod {} DI -> storing DI. len={}".format(pid,len(self.prod[pid].queue)))
				elif 'DepthSnapshot' in msg:
					uid = msg['SecurityID']
					self.prod[pid].first_uid = uid
					self.process_snapshot(recv,pid,msg,msn)
					self.prod[pid].msn = int(msg['LastMsgSeqNumProcessed'])
					self.prod[pid].scid = scid
					self.prod[pid].state=state_snapshot_cycle
					#print("prod ",pid," DS init -> snapshot msn=",self.prod[pid].msn)

class dummy_decoder:
	def __init__(self):
		self.udp_state = { channel_A : state_init, channel_B:state_init}
		self.header_type = 'RDPacketHeader'

	def process_udp_datagram(self,packet):
		print("UDP datagram: ",len(packet)," packets")


def process_line(line,packet,packet_decoder, chan):
	""" Process one line of FAST text. If a UDP datagram is complete with this line
	then process the packet otherwise keeps accumulating lines
	"""
#	data=line2msg.line2msg(line)

	if packet_decoder.udp_state[chan] == state_init:
		if packet_decoder.header_type ==  line.split(':',1)[0]:
			packet_decoder.udp_state[chan] = state_reading_datagram
			packet.append(line)
	elif packet_decoder.udp_state[chan] == state_reading_datagram:
		if packet_decoder.header_type == line.split(':',1)[0]:
			packet_decoder.process_udp_datagram(packet)
			packet.clear() # this is needed to ensure the original object...
			packet.append(line) # is erased and the new data added
		else:
			packet.append(line)

def read_in_chunks(file_object, chunk_size=65536):
	while True:
		data = file_object.read(chunk_size)
		if not data:
			break
		yield data

def parse_text(namea,nameb,packet_decoder):
	""" Parse 2 files for channel A and B of FAST packet and stop when data
	are exhausted on both channels
	"""
	filea=open(namea,buffering=1<<27)
	fileb=open(nameb,buffering=1<<27)
	linea=filea.readline()
	lineb=fileb.readline()
	packeta=[]
	packetb=[]
	ia=0
	ib=0
	
	print("start at ",datetime.now().strftime("%H:%M:%S"),file=sys.stderr)
	while linea or lineb:
		# channel A
		if linea:
			process_line(linea,packeta,packet_decoder,channel_A)
			linea=filea.readline()
			ia+=1
			if ia % 1000000 == 0:
				print("ia=",ia," ",datetime.now().strftime("%H:%M:%S"),file=sys.stderr)

		# channel B
		if lineb:
			process_line(lineb,packetb,packet_decoder,channel_B)
			lineb=fileb.readline()
			ib+=1
			if ib % 1000000 == 0:
				print("ib=",ib," ",datetime.now().strftime("%H:%M:%S"),file=sys.stderr)

	packet_decoder.output()

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

if __name__=="__main__":
#	parser = argparse.ArgumentParser(__file__)
#	parser.add_argument("--verbose","-v", help="verbose mode", type=bool, default=False)
#	parser.add_argument("tarfile",help="input tar file name",type=str)
#	args = parser.parse_args()

#	x=rdd_decoder()
#	parse_text('/mnt/data/david/rdda','/mnt/data/david/rddb',x)

	x=emdi_decoder("/mnt/data/david/p1.csv")
	parse_text('/mnt/data/david/emdia','/mnt/data/david/emdib',x)



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
