import string

from datetime import datetime
from math import floor
from decimal import *
import numpy as np
import h5py as h
from enum import IntEnum

# ====================
#      Order Book
# ====================

# keys in book output are
#	A: Order Add
#	D: Order Delete
#	M: Order Modify no priority change
#	L: Order Modify loss of priority
#	T: Trade
#	S: Execution summary (Eurex)
#	C: Book Cleared
#	Q: Quote

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

# Output:
# A : add
# D : delete
# M : modify same priority
# L : modify loss of priority
# T : trade execution
# S : execution summary
# Q : quote (Kospi)
# C : clear book

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
	#     tuple  dict               # of orders
	#
	# price is a Decimal
	#
	# mode:
	#	level_3 : publish all the updates at the time of the update (ex.: Eurex EOBI)
	#	level_2 : oid is never used throughout the program, only price levels (ex.: CME, EMDI)

	def __init__(self, uid, date, levels, mode='level_3'):
		self.uid = uid
		self.date = date
		self.levels = levels

		self.book = ({}, {})
		self.header = ["otype","h","min","sec","us","recv","exch"]
		bid =  ["bid{}".format(i)  for i in range(1,levels+1)]
		bidv = ["bidv{}".format(i) for i in range(1,levels+1)]
		nbid = ["nbid{}".format(i) for i in range(1,levels+1)]
		ask =  ["ask{}".format(i)  for i in range(1,levels+1)]
		askv = ["askv{}".format(i) for i in range(1,levels+1)]
		nask = ["nask{}".format(i) for i in range(1,levels+1)]
		self.header += bid+bidv+nbid+ask+askv+nask
		self.output = []
		self.valid = True
		self.mode= mode

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
		self.book = ({}, {})
		self.valid = True
		if self.mode=='level_3':
			self.store_update("C",recv,exch)
		return True

	# Add a new order
	def add_order(self,side,price,oid,qty,recv,exch):
		# if price level does not exist, then create it
		if price not in self.book[side]:
			self.book[side][price] = {}

		# check we're not processing twice the same order
		if oid not in self.book[side][price]:
			self.book[side][price][oid] = (recv,exch,qty)
			if self.mode=='level_3':
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
			if self.mode=='level_3':
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
			if self.mode=='level_3':
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
			if self.mode=='level_3':
				self.store_update("D",recv,exch)
			return True
		# if order exists and price is not provided
		elif price==-1: 
			for p in self.book[side]:
				if oid in self.book[side][p]:
					del self.book[side][p][oid]
					if not self.book[side][p]: # price level empty ?
						del self.book[side][p]
					if self.mode=='level_3':
						self.store_update("D",exch,recv)
					return True
		return False # if we're here, it means no order has been deleted

	# ===============
	# Level 2 methods
	# ===============

	def add_level(self,level,side,qty,price,ord_cnt):
		#print("A",side,level,self.book[side])
		# if level exists then push all sub levels by one
		if level in self.book[side]:
			for i in range(max(self.book[side]),level-1,-1):
				if i in self.book[side]: # this should be true all the time
					self.book[side][i+1]=self.book[side][i]
				else:
					return False
		self.book[side][level] = (price,qty,ord_cnt)
		return True

	def amend_level(self,level,side,qty,price,ord_cnt):
		#print("M",side,level,self.book[side])
		if level not in self.book[side]:
			return False
		else:
			self.book[side][level] = (price,qty,ord_cnt)
			return True

	def delete_level(self,level,side):
		#print("D",side,level,self.book[side])
		if level not in self.book[side]:
			return False
		else:
			M = max(self.book[side])
			for i in range(level+1,M+1):
				self.book[side][i-1]=self.book[side][i]
			del self.book[side][M]
			return True

	# =================
	# Reporting methods
	# =================

	def output_head(self,otype,recv,exch):
		return [otype] + list(tots(exch)) + [recv,exch]

	def report_trade(self,price,qty,recv,exch,side=0):
		if self.mode=="level_3":
			self.output.append(
				self.output_head("T",recv,exch)
				+[price,qty]
				+([np.nan]*(4*self.levels-2)))
			return True
		elif self.mode=="level_2":
			self.output.append(
				self.output_head("T",recv,exch)
				+[price,qty,side]
				+([np.nan]*(4*self.levels-3)))
			return True
		else:
			return False

	def exec_summary(self,price,qty,recv,exch):
		self.output.append(
			self.output_head("S",recv,exch)
			+ [price, qty]
			+ ([np.nan]*(4*self.levels-2)))

	def store_update(self,otype,recv,exch):
		r = self.output_head(otype,recv,exch)
		for s in [0,1]:
			# Eurex
			if self.mode=="level_3":
				# count number of prices available on each side
				count = min(self.levels, len(self.book[s]))
				# get the ordered list of prices
				prices=sorted(list(self.book[s]), reverse=s==0)[0:count]
				# extract order, sum qty, count number of orders and make a list out of that
				list_qty = [sum([self.book[s][p][o][2] 
					for o in self.book[s][p]]) 
					for p in prices]
				list_len = [len(self.book[s][p]) for p in prices]
			# CME/CBOT
			elif self.mode=="level_2":
				count = min(self.levels, len(self.book[s]))
				# keys = sorted(self.book[s].keys())[0:count]
				prices   = [self.book[s][i][0] for i in range(count)]
				list_qty = [self.book[s][i][1] for i in range(count)]
				list_len = [self.book[s][i][2] for i in range(count)]

			# complete with nan if necessary
			if count < self.levels:
				prices += [np.nan]*(self.levels-count)
				list_qty += [np.nan]*(self.levels-count)
				list_len += [np.nan]*(self.levels-count)

			r += prices + list_qty + list_len

		self.output.append(r)
