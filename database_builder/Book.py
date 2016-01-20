import string

import sys
import numpy as np
from datetime import datetime
from math import floor
from decimal import *
from pandas import *
from enum import IntEnum

# ====================
#      Order Book
# ====================

# keys in book output are
# level 3:
#	A: Order Add
#	D: Order Delete
#	M: Order Modify no priority change
#	L: Order Modify loss of priority
#	T: Trade
#	S: Execution summary (Eurex)
#	C: Book Cleared
#	Q: Quote
#
# Eurex EMDI level 2:
# A: add level
# V: change volume in a level
# D: delete level
# H: delete from level 1 to level l included
# F: delete from level l to the end
# O: overlay = change price of a level


class Precision(IntEnum):
	sec  =1e0
	milli=1e3
	micro=1e6
	nano =1e9
	pico =1e12
	femto=1e15
	atto=1e18
	zepto=1e21
	yocto=1e24

def tots(t,precision=Precision.micro):
	f = int(floor(t/precision))
	dt=datetime.fromtimestamp(f)
	aftervirgule = t - f*precision
	#delta=int((dt-datetime(dt.year,dt.month,dt.day,0,0,0)).total_seconds()*precision) + aftervirgule
	# we report the date up to nanosecond resolution, followed by 
	# the number of nanoseconds since the beginning of the day
	#return (dt.hour,dt.minute,dt.second,aftervirgule,delta)
	return (dt.hour,dt.minute,dt.second,aftervirgule)

class Order:
	def __init__(self,_uid,_oid,_side,_price,_recv,_exch,_qty):
		self.uid=_uid
		self.oid=_oid
		self.side=_side
		self.price=_price
		self.recv=_recv
		self.exch=_exch
		self.qty=_qty

class Book:

	# Structure of a book for level 3 data
	#
	# book[side][price][oid] = (recv,exch,qty)
	#       ^      ^     ^        ^
	#       |      |     |        |
	#      tuple  dict  dict     tuple
	#
	#
	#
	# Structure of a book for level 2 data
	#
	# book[side][level] = (price,qty,order_count)
	#       ^      ^                  ^
	#       |      |                  |
	#     tuple  list               # of orders
	#
	# price is a Decimal
	#
	# mode:
	#	level_3 : publish all the updates at the time of the update (ex.: Eurex EOBI)
	#	level_2 : oid is never used throughout the program, only price levels (ex.: CME, EMDI)
	#
	# channel_id is used for CME/CBOT only

	def __init__(self, uid, date, levels, mode='level_3', channel_id=-1, ofile=None, min_output_size=10000):
		self.uid = uid
		self.date = date
		self.levels = levels
		self.ll=3+self.levels*6

		if mode=='level_3':
			self.book = ({}, {})
		else:
			self.book = ([], [])
		#self.header = ["otype","h","min","sec","us","recv","exch"]
		self.header = ["otype","recv","exch"]
		bid =  ["bid{}".format(i)  for i in range(1,levels+1)]
		bidv = ["bidv{}".format(i) for i in range(1,levels+1)]
		nbid = ["nbid{}".format(i) for i in range(1,levels+1)]
		ask =  ["ask{}".format(i)  for i in range(1,levels+1)]
		askv = ["askv{}".format(i) for i in range(1,levels+1)]
		nask = ["nask{}".format(i) for i in range(1,levels+1)]
		self.header += bid+bidv+nbid+ask+askv+nask
		self.output = []
		self.valid = True
		if mode=="level_3":
			self.mode=3
		else:
			self.mode=2

		self.channel_id = channel_id
		self.ofile=ofile
		self.nb_write=0
		self.min_output_size = min_output_size

	# ===============
	# Level 3 methods
	# ===============

	# find an order using its oid
	def find_order(self,oid):
		for s in [0,1]:
			for p in self.book[s]: # for each price level
				if oid in self.book[s][p]:
					return Order(self.uid,oid,s,p,
						self.book[s][p][oid][0],
						self.book[s][p][oid][1],
						self.book[s][p][oid][2])
		return None

	# Clear the book
	def clear(self,recv,exch):
		if self.mode==2:
			self.book = ([],[])
		elif self.mode==3:
			self.book = ({},{})
			self.store_update("C",recv,exch)
		self.valid = True
		return True

	# Add a new order
	def add_order(self,side,price,oid,qty,recv,exch):
		# if price level does not exist, then create it
		if price not in self.book[side]:
			self.book[side][price] = {}

		# check we're not processing twice the same order
		if oid not in self.book[side][price]:
			self.book[side][price][oid] = (recv,exch,qty)
			if self.mode==3:
				self.store_update("A",recv,exch)
			return True
		else:
			return False

	# Replace an order by inserting a new one. By default priority is lost
	def replace_order(self,side,newprice,newoid,newqty,recv,exch,oldoid):
		order = self.find_order(oldoid)
		if order is None:
			return False
		oldprice = order.price
		# check if price level and order id exist
		if oldprice in self.book[side] and oldoid in self.book[side][oldprice]:
			del self.book[side][oldprice][oldoid]
			# check if price level is empty now
			if not self.book[side][oldprice]:
				del self.book[side][oldprice]
			# add the level if not exist
			if newprice not in self.book[side]:
				self.book[side][newprice] = {}
			# add the new order
			self.book[side][newprice][newoid] = (recv,exch,newqty)
			if self.mode==3:
				self.store_update("L",recv,exch)
			return True
		else:
			return False

	# Modify an existing order in place => only qty and timestamps can change
	def modify_inplace(self,side,oid,newqty,recv,exch):
		order = self.find_order(oid)
		if order is None:
			return False
		# check if price level and order id exist
		if order.price in self.book[side] and order.oid in self.book[side][order.price]:
			self.book[side][order.price][oid] = (recv,exch,newqty) 
			if self.mode==3:
				self.store_update("M",recv,exch)
			return True
		else:
			return False
			
	# Delete an order
	def delete_order(self,side,price,oid,recv,exch):
		# if order exists and price is provided
		if price in self.book[side] and oid in self.book[side][price]:
			del self.book[side][price][oid]
			# if price level is empty
			if not self.book[side][price]:
				del self.book[side][price]
			if self.mode==3:
				self.store_update("D",recv,exch)
			return True
		# if order exists and price is not provided
		elif price==-1: 
			for p in self.book[side]:
				if oid in self.book[side][p]:
					del self.book[side][p][oid]
					if not self.book[side][p]: # price level empty ?
						del self.book[side][p]
					if self.mode==3:
						self.store_update("D",exch,recv)
					return True
		return False # if we're here, it means no order has been deleted

	# ===============
	# Level 2 methods
	# ===============

	def replace_book(self,bid,ask,bidv,askv,nbid,nask,recv,exch):
		self.book = (list(zip(bid,bidv,nbid)),list(zip(ask,askv,nask)))
		self.valid=True
		self.store_update("Q",recv,exch)
		return True

	def add_level(self,level,side,qty,price,ord_cnt):
		if level<=len(self.book[side]):
			self.book[side].insert(level, (price,qty,ord_cnt) )
			return True
		else:
			return False

	def amend_level(self,level,side,qty,price,ord_cnt):
		if level < len(self.book[side]):
			self.book[side][level] = (price,qty,ord_cnt)
			return True
		else:
			return False

	def delete_level(self,level,side):
		if level < len(self.book[side]):
			self.book[side].pop(level)
			return True
		else:
			return False

	# =================
	# Reporting methods
	# =================

	def output_head(self,otype,recv,exch):
		#return [otype] + list(tots(exch)) + [recv,exch]
		return [otype,recv,exch]

	def report_trade(self,price,qty,recv,exch,side=0,nb_orders=0,aggrtime=0,nb_buy=0,nb_sell=0):
		self.output.append(
			self.output_head("T",recv,exch)
			+[price,qty,side,nb_orders,aggrtime,nb_buy,nb_sell]
			+([np.nan]*(4*self.levels-7)))
		return True

	def exec_summary(self,price,qty,recv,exch):
		self.output.append(
			self.output_head("S",recv,exch)
			+ [price, qty]
			+ ([np.nan]*(4*self.levels-2)))

	def store_update(self,otype,recv,exch):
		r = self.output_head(otype,recv,exch)
		do_update=True
		for s in [0,1]:
			# Eurex
			if self.mode==3:
				# count number of prices available on each side
				count = min(self.levels, len(self.book[s]))
				# get the ordered list of prices
				prices=sorted(list(self.book[s]), reverse=s==0)[0:count]
				# extract order, sum qty, count number of orders and make a list out of that
				list_qty = [sum([self.book[s][p][o][2] 
					for o in self.book[s][p]]) 
					for p in prices]
				list_len = [len(self.book[s][p]) for p in prices]
				r += prices + list_qty + list_len
			# CME/CBOT/Nymex, Kospi, Eurex EMDI
			elif self.mode==2:
				count = min(self.levels, len(self.book[s]))
				nanl = [np.nan]*(self.levels-count)
				r+= \
					[item[0] for item in self.book[s][0:count]]+nanl +\
					[item[1] for item in self.book[s][0:count]]+nanl +\
					[item[2] for item in self.book[s][0:count]]+nanl

		
		# do update only if there is a change in the top levels
		if len(self.output)==0 or (len(self.output) and 
				self.output[-1][3:self.ll]!=r[3:self.ll]):
			self.output.append(r)
			if self.ofile and len(self.output)==self.min_output_size:
				self.write_output()

	def write_output(self):
		x=DataFrame(self.output, columns=self.header)
		# write in CSV form
		# adding header only if it's the first time we write in the file
		f = open(self.ofile,'a')
		x.to_csv(f, na_rep="NA", index=False,index_label=False,
				header=self.nb_write==0)
		f.flush()
		f.close()
		self.nb_write += 1
		self.output = [] # clean up the big list to make it fast again
