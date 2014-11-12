from datetime import datetime
from math import floor
import string
import numpy as np

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
	f = int(floor(t/1e9))
	dt=datetime.fromtimestamp(f)
	ns = t - f*1000000000
	delta=int((dt-datetime(dt.year,dt.month,dt.day,0,0,0)).total_seconds()*1e9) + ns
	# we report the date up to nanosecond resolution, followed by 
	# the number of nanoseconds since the beginning of the day
	return (dt.hour,dt.minute,dt.second,ns,delta)

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

	def __init__(self, uid, date, levels):
		self.levels = levels
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

		# check we're not processing twice the same order
		if oid not in self.book[side][price]:
			self.book[side][price][oid] = (recv,exch,qty)
			self.print_output("A",recv,exch)


	# Modify an order
	# priority is external to the book management and market dependent
	def modify_order(self,side,price,oid,qty,recv,exch,prevoid,prevprice,prevqty,change=False):
		# if order exists
		if prevprice in self.book[side] and prevoid in self.book[side][prevprice]:
			if change:
				del self.book[side][prevprice][prevoid]
				if price not in self.book[side]:
					self.book[side][price] = {}
			if oid not in self.book[side][price]: # this 2 lines work for change and no_change
				self.book[side][price][oid] = (recv,exch,qty)
			if change:
				self.print_output("L",recv,exch)
			else:
				self.print_output("M",recv,exch)
				
	# Delete an order
	def delete_order(self,side,price,oid,recv,exch):
		# if order exists and price is provided
		if price in self.book[side] and oid in self.book[side][price]:
			del self.book[side][price][oid]
			self.print_output("D",recv,exch)
		# if order exists and price is not provided (price=-1 or side=-1)
		elif price==-1 or side==-1:
			done=False
			for s in [0,1]:
				if not done:
					for p in list(self.book[s]):
						if oid in self.book[side][price]:
							del self.book[s][p][oid]
							done=True
							break
			self.print_output("D",recv,exch)

	def output_head(self,otype,recv,exch):
		return [otype] + list(tots(exch)) + [recv,exch]

	def report_trade(self,price,qty,recv,exch):
		self.output.append(
			self.output_head("T",recv,exch)
			+[price,qty]
			+([np.nan]*(4*self.levels-2)))

	def exec_summary(self,price,qty,recv,exch):
		self.output.append(
			self.output_head("S",recv,exch)
			+ [price, qty]
			+ ([np.nan]*(4*self.levels-2)))

	def print_output(self,otype,recv,exch):
		r = self.output_head(otype,recv,exch)
		for side in [0,1]:
			prices=sorted(list(self.book[side]), reverse=side==0)
			count = 1

			for price in prices:
				if count <= self.levels:
					sum=0
					for o in self.book[side][price]:
						sum+=self.book[side][price][o][2]
					r += [price,sum]
					count += 1
			if count <= self.levels:
				r += [np.nan]*(2*(self.levels+1-count))
		self.output.append(r)
