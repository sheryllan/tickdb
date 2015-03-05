import time,os,logging

from datetime import *
from decimal import *
from numpy import *
from pandas import *

import Book

def decode_qtg_CME(fi,levels,day):
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
				books[uid] = Book.Book(uid,day,levels,"level_2")
		elif uid in books:
			if msg in ['UNSUBSCRIBE','PACKET_DONE']:
				pass
			else: # dated messages now with recv and exch in common
				if msg=='RESET':
					books[uid].clear(0,0)

				elif msg=='ADD_LEVEL':
					level = int(l[2])
					side  = int(l[3])-1
					qty   = int(l[4])
					price = Decimal(l[5])
					ord_cnt=int(l[7])

					if not books[uid].add_level(level,side,qty,price,ord_cnt):
						logging.error("Error with ADD_LEVEL at line {}".format(_C))
						logging.error(line)

				elif msg=='DELETE_LEVEL':
					level = int(l[2])
					side  = int(l[3])-1
					if not books[uid].delete_level(level,side):
						logging.error("Error with DELETE_LEVEL at line {}".format(_C))
						logging,error(line)

				elif msg=='AMEND_LEVEL':
					level = int(l[2])
					side  = int(l[3])-1
					qty   = int(l[4])
					price = Decimal(l[5])
					ord_cnt=int(l[7])

					if not books[uid].amend_level(level,side,qty,price,ord_cnt):
						logging.error("Error with AMEND_LEVEL at line {}".format(_C))
						logging.error(line)

				elif msg=='TRADE_WITH_SIDE':
					qty   = int(l[4])
					price = Decimal(l[5])
					side  = int(l[6])-1
					#trade_volume = int(l[9])

					if not books[uid].report_trade(price,qty,recv,exch,side):
						logging.error("Error with TRADE_WITH_SIDE at line {}".format(_C))
						logging.error(line)

				elif msg=='FINAL':
					exch = int(l[2])
					recv = int(l[3])
					books[uid].store_update("Q",recv,exch)

				else:
					pass

	return books,symbols
