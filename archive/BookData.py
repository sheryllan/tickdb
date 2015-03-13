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

class OrderBook
	def __init__(self,level):
		self.level=level
		
class ByOrderBookData(MessageSequenceTracker):
	def __init__(self,levels):
		MessageSequenceTracker.__init__(self)
		self.obooks = {} # contains books
		self.results = {} # map of uid and columns of results
		self.levels=levels
	
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

	# ======================
	#   Book Functions
	# ======================

	# find index of tuple in a list of tuples where the first value is the key
	# [ (key1, v1), (key2,v2), ... , (keyn,vn) ]
	def vindex(self,loftuples, key):
		i=0
		for v in loftuples:
			if v[0]==key: return i
			i += 1
		return -1

	def new_order(self,uid, oid, side, price, qty, exch):
		book = self.obooks[uid][side] # ref to symbol-side book

		if price not in book:
			book[price] = [] # create a new level
		if oid not in book[price]: # add new order if not exist
			book[price].append( (oid,qty,exch) ) # append order to price level list
		else:
			self.modify_order(uid, oid, side, price, qty, exch)

	def modify_order(self,uid, oid, side, price, delta_qty, exch):
		book = self.obooks[uid][side]

		if price not in book: # check if price exists first
			return
		else:
			indx = self.vindex(book[price], oid) # get index of orderid(oid) in price level list
			if indx>=0: # if oid is valid
				x=book[price][indx]
				if delta_qty > 0:  # move to the back if quantity increases
					del book[price][indx] # remove order first
					# put it last in priority
					book[price].append( (x[0], x[1]+delta_qty, exch))
				else:	 # qty decrease => priority doesn't change
					book[price][indx] = (x[0], x[1]+delta_qty, x[2])

	def delete_order(self,uid, oid, side, price):
		book = self.obooks[uid][side]
		indx=-1
		if price not in book and price != 0: # wrong price information, ignore order
			return
		if price != 0: # We can use price info
			indx = self.vindex(book[price], oid) # find index directly
		else: # EOBI - find price of particular orderID
			for price in book:
				indx = self.vindex(book[price],oid)
				if indx != -1: break # break if correc price is found
		if indx>=0: # finally remove the order and the level if it's empty
			del book[price][indx]
			if len(book[price])==0:
				del book[price]
			
	# ===============================================
	#   Print Functions   // Change to using Pandas
	# ===============================================
	def printBook(self,otype, uid, texch, trecv):
		#output the data
		self.results[uid]['otype'].append(otype)
		self.results[uid]['exch'].append(texch)
		self.results[uid]['recv'].append(trecv)
		for side in [0,1]:
			side_str="bid"
			if side==1:
				side_str="ask"
			book = self.obooks[uid][side]
			prices = sorted(book.keys(), reverse = side==0)
			count = 1

			for price in prices:
				if count <= self.levels:
					sum = 0
					for o in book[price]:
						sum += o[1]
					self.results[uid]["{0}{1}".format(side_str,count)].append(price)
					self.results[uid]["{0}v{1}".format(side_str,count)].append(sum)
					count += 1
			if count<=self.levels:
				# fill in missing levels with NaN
				for i in range(count,int(self.__levels)+1):
					self.results[uid]["{0}{1}".format(side_str,i)].append(np.nan)
					self.results[uid]["{0}v{1}".format(side_str,i)].append(0)

	def printTrade(self,uid,exch,recv,price,qty):			
		# output the data
		self.results[uid]['otype'].append("T")
		self.results[uid]['exch'].append(exch)
		self.results[uid]['recv'].append(recv)
		self.results[uid]['bid1'].append(price)
		self.results[uid]['bidv1'].append(qty)
		# Fill in the price levels with NaN as this is a trade
		for i in range(1,int(self.levels)+1):
			for side in ["bid","ask"]:
				if not (side=="bid" and i==1):
					self.results[uid]["{0}{1}".format(side,i)].append(np.nan)
					self.results[uid]["{0}v{1}".format(side,i)].append(0)
					
	def output_to_HDF5(self):
		for uid in self.results:
			dataframe=pd.DataFrame(self.results[uid],index=range(len(self.results[uid]['recv'])))
			dataframe.to_hdf("{0}_{1}.h5".format(self.date, uid),'table',append=False)
		
class IncrementalBookData(MessageSequenceTracker):
	def __init__(self):
		MessageSequenceTracker.__init__(self)
		self.results = {}   #map of uid and columns of results
