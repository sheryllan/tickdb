import sys

class EOBI:
	def __init__(self,levels,logger):

		self.__log=logger
		if levels<1:
			raise Exception("Levels must be 1 or more")
		
		# fast set to check ticks type and process them or not
		self.__headers = frozenset(["eobi_header","eobi_13600_product_summary",
			"eobi_13601_instrument_summary_header_body","eobi_13602_snapshot_order"])
		self.__snap_headers = frozenset(["eobi_13600_product_summary",
			"eobi_13601_instrument_summary_header_body","eobi_13602_snapshot_order"])

		# The books
		self.bookData = ByOrderBookData(levels)

		# Snapshot flow management
		self.__snapshot_cycle=-1 # keeps track of where we are in the snapshot cycle
		self.__snapshot_queue=[] # for storing out of sequence snapshot cycle ticks
		self.__new_snapshot_cycle=-1 # this is to handle a snapshot cycle that comes before the last one ends.
		self.__new_snapshot_queue=[] # for storing out of sequence snapshot cycle ticks for the new cycle when the old one is not done.

		self.__last_header=None # this is the last eobi_header we use this to check for the end of the cycle
		self.__previous_tick=None #this is the last tick...    we use this to check for the end of the cycle
		self.__last_seqnum=0 # The last sequence number that the snapshot cycle is looking at.
		self.__uid=None #this is the current uid in the snapshot cycle
		self.snapshot_products={} #we use this to make sure we only snapshot each product once.		

		# ???
		self.__missing_seqnum={}
		self.__last_seqnum={}
	
	def __getstate__(self):
		d = dict(self.__dict__)
		print(d.keys())
		del d['_EOBI__log']#TODO: need to drop the logger because it won't pickle, but need to handle loading it better.
		del d['_EOBI__snapshot_queue']
		del d['_EOBI__last_header']
		del d['_EOBI__previous_tick']
		del d['_EOBI__uid']
		return d
		
	# Find the first sequence number for each product
	# ticks is a list of DataMole ticks
	def findFirstMsgseqnum(self,ticks):
		# read file line by line
		for tick in ticks:
			if tick.name not in self.__headers:
				uid=tick.values['secid']
				# As long as the message is not a snapshot message, this is 
				# the first incremental we can see, and hence when we subscribed
				# to the multicast channel.
				if uid not in self.bookData.product_sequence_numbers:
					msgseqnum=int(tick.values['msgseqnum'])
					self.bookData.init_book(uid,msgseqnum)
					self.__log.info("Found first product:  {0} {1} {2}".
							format(uid,tick.name,msgseqnum))
			# XXX all the subsequent messages will be ignored from here.
			# XXX this loop should stop here !
			
	# 
	def parseForMissingSeqnum(self,ticks):
		 # read file line by line
		for tick in ticks:
			if tick.name not in self.__headers:
				uid=tick.values['secid']
				msgseqnum=int(tick.values['msgseqnum'])
				#As long as the message is not a snapshot message, this is
				# the first incremental we can see, and hence when we subscribed
				# to the multicast channel.
				if uid not in self.__last_seqnum:
					self.__last_seqnum[uid]=self.bookData.product_sequence_numbers[uid]-1
				if uid not in self.__missing_seqnum:
					self.__missing_seqnum[uid]=[]
				# Save missing seq num if any
				if msgseqnum>self.__last_seqnum[uid]+1:
					#gap
					fill=self.__last_seqnum[uid]+1
					while fill<msgseqnum:
						self.__missing_seqnum[uid].append(fill)
						fill+=1
				# Remove missing seq num if a late message arrived
				if msgseqnum<=self.__last_seqnum[uid]:
					#out of sequence coming in
					self.__missing_seqnum[uid].remove(msgseqnum)
		
				# update last seq number with current number
				self.__last_seqnum[uid]=msgseqnum
		for uid in self.__missing_seqnum:
			self.__log.info("{0} has {1} missing ticks".format(uid,len(self.__missing_seqnum[uid])))
		
	def computeSnapshotData(self,ticks):
		for tick in ticks:
			# This will work on the snapshot channel only....
			if tick.name=='eobi_header':
				# Check if the previous snapshot message was a Complete message
				# indicating the end of the snapshot cycle.
				if self.__previous_tick is not None and self.__previous_tick.name in self.__snap_headers:
					if self.__last_header is not None and int(self.__last_header.values["CompletionIndicator"])==1:
						# End of snapshot cyle.  Clean up.
						self.__log.info("Clear End: " + tick.name)
						self.__snapshot_queue=[]
						self.__snapshot_cycle=-1
						self.__last_seqnum=0
				self.__last_header=tick 

			# This is the case where we get the start too early.
			count = 0
			if self.__snapshot_cycle==-1:
				while count<len(self.__new_snapshot_queue):
					self.__log.info("Running New Cycle")
					qtick = self.__new_snapshot_queue.pop(0)
					self.__readTickForSnapshot(qtick,True)
					count+=1
				self.__new_snapshot_queue=[]
				self.__new_snapshot_cycle=-1
			else:
				while count<len(self.__snapshot_queue):
					qtick= self.__snapshot_queue.pop(0)
					self.__readTickForSnapshot(qtick,True)
					count+=1
			self.__readTickForSnapshot(tick)
			self.__previous_tick=tick

	def __readTickForSnapshot(self,tick,recheck=False):
		if tick.name in self.__snap_headers:
			msgseqnum=int(tick.values['msgseqnum'])
			# If we are already in a snapshot cycle and get the start of
			# a new one without an end one, we have out of sequence packets.
			if self.__snapshot_cycle>=0 and msgseqnum==0 and tick.name=="eobi_13600_product_summary":
				self.__new_snapshot_queue.append(tick)
				self.__log.info("Too Early: " + str(msgseqnum) + " " + str(len(self.__new_snapshot_queue)))
			elif (self.__snapshot_cycle>=0 and msgseqnum==self.__new_snapshot_cycle+1 and
			     (tick.name=="eobi_13601_instrument_summary_header_body" or
				     tick.name=="eobi_13602_snapshot_order")):
				self.__new_snapshot_cycle=msgseqnum
				self.__new_snapshot_queue.append(tick)
				self.__log.info("Too Early: " + str(msgseqnum) +
						" " + str(len(self.__new_snapshot_queue)))
			# A product summary message with a sequence number of 0 starts the snapshot cycle
			elif self.__snapshot_cycle==-1 and msgseqnum==0 and tick.name=="eobi_13600_product_summary":
				self.__last_seqnum=int(tick.values['lastmegseqnumprocessed'])
				self.__snapshot_cycle=0
				#self.__log.info("Product Summary: " + str(self.__last_seqnum) +
				#		" " + str(len(self.__snapshot_queue)))
			# If we are in a snapshot cycle we can get Instrument Summary messages 
			# and then Order Messages to build the book
			elif (self.__snapshot_cycle>=0 and msgseqnum==self.__snapshot_cycle+1 and
			     (tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order")):
				self.__snapshot_cycle=msgseqnum	
				# A summary message is always before the order messages
				if (tick.name=="eobi_13601_instrument_summary_header_body"):
					self.__uid=None #clear the previous UID
					uid=tick.values['securityid']
					#self.__log.info("InstMsg: " + str(msgseqnum) + " Q: " +
					#		str(len(self.__snapshot_queue))+ " uid:" +
					#		str(uid)+ " last:" + str(self.__last_seqnum) +
					#		" incr: " + str(self.bookData.product_sequence_numbers[uid]))
					# We only want to consider the security if:
					# 1) We have not used the product before
					# 2) The sequence number is one that we can grow the incremental from 
					if(uid not in self.snapshot_products.keys() and
					   uid in self.bookData.product_sequence_numbers and
					   self.bookData.product_sequence_numbers[uid]<=self.__last_seqnum):
						self.__uid=uid
						self.snapshot_products[self.__uid]=self.__last_seqnum
						self.bookData.product_sequence_numbers[self.__uid]=self.__last_seqnum
						#self.__log.info("New Product: " + str(self.__uid) + " " + str(len(self.__snapshot_queue)))
				if (tick.name=="eobi_13602_snapshot_order"):
					#self.__log.info("OrdMsg: " + str(msgseqnum) + " " + str(len(self.__snapshot_queue)))
					# self.__uid is set by the Instrument Summary message
					if self.__uid is not None and self.__uid in self.snapshot_products.keys():
						oid=tick.values['trdregTStimepriority'] 
						side = int(tick.values['side'])-1
						qty= int(tick.values['displayqty'])
						#TODO: I am not sure if this is right to divide by 100000000
						price=float(tick.values["price"])/100000000.0
						self.__new_order(self.__uid,oid,side,price,qty,tick.timestamp)
						#self.__log.info("Order " + str(msgseqnum) + " " 
						#		+ str(oid)+ " " + str(side) + " " 
						#		+ str(price) + " " + str(qty ))
			# If we are in a snapshot cycle but the sequence nubmers do not add up,
			# then we have an out of sequence packet that we need to reorder
			elif(self.__snapshot_cycle>=0 and msgseqnum!=self.__snapshot_cycle+1 and
			     (tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order")):
				# TODO: There is some funny stuff going on with this where I am up to
				# packet 5000 and then I get packet 1200 and move up to and past 5000.  WTF IS THAT?
				self.__snapshot_queue.append(tick)
				#if not recheck:
					#self.__log.info("Out of sequence Snapshot: " + str(msgseqnum) +
					#" " + str(len(self.__snapshot_queue)))
			elif self.__snapshot_cycle>=0:
				pass #Something has gone wrong if we see this I think
				self.__log.warn(tick.name + " " + str(msgseqnum)+ " "  + str(self.__snapshot_cycle))
			else:
				#snapshot cycle has not started we jumped onto the multicast in the middle....
				# throw these away
				pass
					
	def calcEOBI(self,ticks,uid_to_run,logger):  #ticks is a list of DataMole ticks
		self.__log=logger
		# Remove other data because we are mulitprocessing and I only care about me
		for uid in self.snapshot_products.keys():
			if uid!=uid_to_run:
				self.snapshot_products.pop(uid)
				self.__missing_seqnum.pop(uid)
				self.bookData.delete_book(uid)
		# Now parse the data
		for tick in ticks:
			# pass over everything Snapshot
			if tick.name in self.__headers:
				continue
			uid=tick.values['secid']
			msgseqnum=int(tick.values['msgseqnum'])
			# cycle through the missing sequence numbers
			if len(self.__missing_seqnum[uid])>0:
				while len(self.__missing_seqnum[uid])>0 and msgseqnum>self.__missing_seqnum[uid][0]:
					seq=self.__missing_seqnum[uid].pop(0)
					self.bookData.product_sequence_numbers[uid]=seq
			# Go through stored data first to see if we are ready to 
			# parse data that was previously stored because we did not
			# have a snaphot or it had been out of sequence
			for suid in self.bookData.product_stored_data.keys():
				count=len(self.bookData.product_stored_data[suid])
				while count>0:
					st_tick=self.bookData.product_stored_data[suid].pop(0)
					if suid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[suid]>=msgseqnum:
						pass  #this is the A/B feed case
					elif suid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[suid]+1==msgseqnum:
						self.bookData.product_sequence_numbers[suid]+=1
						self.__parse_tick(st_tick)
					else:
						self.bookData.product_stored_data[suid].append(st_tick)
					count-=1
				if len(self.bookData.product_stored_data[suid])==0:
					self.bookData.product_stored_data.pop(suid)
			# After we have read all of the previously stored data we need to then process the next tick.
			if uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]>=msgseqnum:
				continue  #this is the A/B feed case or that we do not have the snapshot yet.
			elif uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]+1==msgseqnum:
				self.bookData.product_sequence_numbers[uid]+=1
				self.__parse_tick(tick)
			else:
				if uid not in self.bookData.product_stored_data:
					self.bookData.product_stored_data[uid]=[]
				self.bookData.product_stored_data[uid].append(tick)
				self.__log.info("OutOfSeq: {0} LastSeq: {1} CurrSeq: {2} Len: {3}".format(uid,
					self.bookData.product_sequence_numbers[uid],msgseqnum,
					len(self.bookData.product_stored_data[uid])))
				#TODO:  We need to check that we had a clean parse...
				# once we are completely done we need to check what is in
				# self.bookData.product_sequence_numbers
			
	############################################################
	#
	#   Parse Functions
	#
	############################################################		
	def __parse_tick(self,tick):
		uid=tick.values['secid']
		msgseqnum=int(tick.values['msgseqnum'])
		#we ignore  'eobi_13504_top_of_book'	
		if uid not in self.bookData.obooks:
			self.bookData.init_book(uid,msgseqnum)
		if tick.name=='eobi_13103_order_mass_del':
			self.bookData.obooks[uid] = ({}, {})
			# TODO: Should I output the clear?
			#self.bookData.printBook("C",uid,int(tick.timestamp),int(tick.timestamp))
		elif tick.name== 'eobi_13201_trade_report':
			pass  # Need to handle this to report the trades faster....
		elif tick.name=='eobi_13202_exec_summary':
			pass  # Need to handle this to report the trades faster....
		elif tick.name=='eobi_13502_cross_request':
			pass  # Need to output this for signals...
		elif tick.name=='eobi_13503_quote_request':
			pass  # Need to output this for signals...
		else:
			try:
				recv= int(tick.timestamp)
				side= int(tick.values["side"])-1
				oid = int(tick.values["trdregTStimepriority"])
				#TODO: I am not sure if this is right to divide by 100000000
				price=float(tick.values["price"])/100000000.0
				if tick.name=='eobi_13100_ord_add':
					exch= int(tick.values["trdregtstimein"])
					qty = int(tick.values["qty"])
					self.__new_order(uid,oid,side,price,qty,exch)
					self.bookData.printBook("A",uid,exch,recv)			   
				elif tick.name=='eobi_13101_order_mod':
					exch= int(tick.values["trdregtstimein"])
					qty = int(tick.values["DisplayQty"])
					prev = int(tick.values["PrevPrice"])
					old_oid = int(tick.values["TrdRegTSPrevTime-Pri"])
					if oid==old_oid:
						# Queue priority not lost, qty reduced
						self.__modify_order(uid, oid, side, price, qty, exch)
						self.bookData.printBook("M",uid,exch,recv)
					else:
						# Queue priority lost, qty increased or price changed
						self.__delete_order(uid, old_oid, side, prev)
						self.__new_order(uid,oid,side,price,qty,exch) 
						self.bookData.printBook("R",uid,exch,recv)			   
				elif tick.name=='eobi_13102_order_del':
					exch= int(tick.values["trdregtstimein"])
					self.__delete_order(uid, oid, side, 0)
					self.bookData.printBook("D",uid, exch,recv)				   
				elif tick.name=='eobi_13104_13105_order_exec':
					qty = int(tick.values["qty"])
					#TODO: this is not right... I need the TransactTime
					self.bookData.printTrade(uid,recv,recv,price,qty)			
				elif tick.name=='eobi_13106_order_mod_same_priority':
					exch= int(tick.values["trdregtstimein"])
					qty = int(tick.values["qty"])
					self.__modify_order(uid,oid,side,price,-qty,exch)
					self.bookData.printBook("M",uid,exch,recv)			   
				else:
					pass
			except:
				print(tick.name,tick.values.keys())
							


