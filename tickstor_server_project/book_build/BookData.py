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
