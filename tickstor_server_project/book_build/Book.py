import string, datetime, os, sys, argparse, logging, multiprocessing, pickle
import numpy as np
import pandas as pd

# ====================
#      Order Book
# ====================

# keys in book output are
#	A: Order Add
#	D: Order Delete
#	M: Order Modify no priority change
#	L: Order Modify loss of priority
#	T: Trade
#	E: Execution summary (Eurex)
#	C: Book Cleared
#	Q: Quote

def tots(t):
	#epoch = datetime.utcfromtimestamp(0)
	d1 = datetime.fromtimestamp(t//1e6) # remove usec and convert to datetime
	d0 = datetime(d1.year,d1.month,d1.day,0,0,0) # beginning of the day
	ms = (t-(t//1e6)*1e6)
	ds = int((d1-d0).total_seconds()*1e6 + ms)
	# we report the date up to microsecond resolution, followed by 
	#the number of microsecond since the beginning of the day (= deltat)
	return (int(d1.hour),int(d1.minute),int(d1.second),int(ms),ds)
	#           0        1         2        3         4

# Output:
# A : add
# D : delete
# M : modify same priority
# L : modify loss of priority
# T : trade execution
# S : execution summary
# Q : quote (Kospi)

class Book:

	# Structure of a book
	# book[side][price][oid] = (recv,exch,qty)
	#       ^      ^     ^        ^
	#       |      |     |        |
	#      tuple  dict  dict     tuple

	def __init__(self, uid, date, level):
		self.level = level
		self.book = ({}, {})
		self.output = []
		self.uid = uid
		self.date = date
		self.not_valid = False

	# Clear the book
	def clear(self):
		self.book = ({}, {})
		self.not_valid = False

	# Add a new order
	def add_order(self,side,price,oid,qty,recv,exch):
		# if price level does not exist, then create it
		if price not in self.book[side]:
			self.book[side][price] = {}
			self.output("A",recv,exch)

		# check we're not processing twice the same order
		if oid not in self.book[side][price]:
			self.book[side][price][oid] = (recv,exch,qty)
			self.output("A",recv,exch)


	# Modify an order
	# priority is external to the book management and market dependent
	def modify_order(self,side,price,oid,qty,recv,exch,prevoid,prevprice,prevqty,change=False):
		# if order exists
		if prevoid in self.book[side][prevprice]:
			if change:
				del self.book[side][prevprice][prevoid]
				if price not in self.book[side]:
					self.book[side][price] = {}
			if oid not in self.book[side][price]: # this 2 lines work for change and no_change
				self.book[side][price][oid] = (recv,exch,qty)
			if change:
				self.output("L",recv,exch)
			else:
				self.output("M",recv,exch)
				
	# Delete an order
	def delete_order(self,side,price,oid,recv,exch):
		# if order exists and price is provided
		if price in self.book[side] and oid in self.book[side][price]:
			del self.book[side][pice][oid]
			self.output("D",recv,exch)
		# if order exists and price is not provided (price=-1 or side=-1)
		elif price==-1 or side==-1:
			done=False
			for s in [0,1]:
				if not done:
					for p in list(self.book[s]):
						if oid in self.book[side][price]
							del self.book[s][p][oid]
							done=True
							break
			self.output("D",recv,exch)

	def output_head(self,otype,recv,exch):
		return [otype] + list(tots(exch)) + [recv,exch]

	def report_trade(self,price,qty,recv,exch):
		self.output.append(
			self.output_head("T",recv,exch)
			+[price,qty]
			+([np.nan]*(4*self.level-2)))

	def exec_summary(self,price,qty,recv,exch):
		self.output.append(
			self.output_head("S",recv,exch)
			+ [price, qty]
			+ ([np.nan]*(4*self.level-2)))

	def output(self,otype,recv,exch):
		r = self.output_head(otype,recv,exch)
		for side in [0,1]:
			prices=sorted(list(self.book[side]), reverse=side==0)
			count = 1

			for price in prices:
				if count <= self.levels:
					sum=0
					for o in self.book[side][price]:
						sum+=o[1]
					r.append(price)
					r.append(sum)
					count += 1
			if count <= self.levels:
				for i in range(count,int(self.level)+1):
					r.append(np.nan)
					r.append(np.nan)
		self.output.append(r)

	def output_to_HDF5(self):
		headers = ["otype","h","min","sec","us","deltat","recv","exch"]
		headers.extend(sum([ ["bid{0}".format(i),"bidv{0}".format(i)] for i in range(5) ],[]))
		headers.extend(sum([ ["ask{0}".format(i),"askv{0}".format(i)] for i in range(5) ],[])
		df = pd.DataFrame(self.output, columns=headers)
		df.to_hdf("{0}_{1}.hdf".format(self.date,self.uid),'table',append=False)
