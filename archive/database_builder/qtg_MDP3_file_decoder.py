import time,os,logging

from datetime import *
from decimal import *
from numpy import *
from pandas import *

import Book

def decode_qtg_MDP3(fi,levels,day):
	books = {}  # contains books
	symbols = {} # contains symbols' names

	_C = 0
	print(_C,datetime.now())
	_t = datetime.now()
	for line in fi: # read file line by line
		_C += 1
		# Timers for each 100 000 lines
		if _C%100000==0:
			print(_C,datetime.now()-_t)
			_t = datetime.now()

		l = line.split(':') # get fields from line
		msg = l[0] # type of message

		# Try to catch bad lines
		if(len(l)>1): # just to catch bad lines
			uid = int(l[1]) # get UID
		else:
			continue

		if msg=='SUBSCRIBE': # same as qtg EOBI
			sname=l[2].rstrip()
			if sname != 'N/A':
				symbols[uid] = l[2].rstrip()
				channel_id = int(l[3])
				books[uid] = Book.Book(uid,day,levels,"level_2", channel_id)

		elif msg=='PACKET_END':
			channel_id = int(l[1])
			exch = int(l[2])
			recv = int(l[3])
			for uid in books:
				if books[uid].valid and books[uid].channel_id == channel_id:
					books[uid].store_update("Q",recv,exch)
		elif uid in books:
			if not books[uid].valid:
				pass
			elif msg in ['UNSUBSCRIBE','FINAL']:
				pass
			else: # dated messages now with recv and exch in common
				if msg=='RESET':
					books[uid].clear(0,0)

				elif msg=='ADD':
					level = int(l[2])
					side  = int(l[3])
					qty   = int(l[4])
					price = Decimal(l[5])
					ord_cnt=int(l[6])

					if not books[uid].add_level(level,side,qty,price,ord_cnt):
						logging.error("Error with ADD_LEVEL at line {0} in {1}".format(_C,fi.name))

				elif msg=='DELETE':
					level = int(l[2])
					side  = int(l[3])
					if not books[uid].delete_level(level,side):
						logging.error("Error with DELETE_LEVEL at line {0} in {1}".format(_C,fi.name))

				elif msg=='AMEND':
					level = int(l[2])
					side  = int(l[3])
					qty   = int(l[4])
					price = Decimal(l[5])
					ord_cnt=int(l[6])

					if not books[uid].amend_level(level,side,qty,price,ord_cnt):
						logging.error("Error with AMEND_LEVEL at line {0} in {1}".format(_C,fi.name))

				elif msg=='TRADE_SUMMARY':
					price = Decimal(l[2])
					side  = int(l[3])
					qty   = int(l[4])
					ord_cnt=int(l[5])

					if not books[uid].report_trade(price,qty,recv,exch,side):
						logging.error("Error with TRADE_WITH_SIDE at line {0} in {1}".format(_C,fi.name))

				elif msg=='SNAP_FINAL':
					exch = int(l[2])
					recv = int(l[3])
					books[uid].store_update("Q",recv,exch)

				else:
					pass

	return books,symbols
