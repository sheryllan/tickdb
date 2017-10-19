import time,os,logging

from datetime import *
from decimal import *
from numpy import *
from pandas import *

import Book

def decode_qtg_KOSPI(fi,levels,day):
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
			if msg in ['UNSUBSCRIBE']:
				pass
			else: # dated messages now with recv and exch in common
				if msg=='QUOTE':
					bid = [ Decimal(l[i*6+4]) for i in range(0,5) ]
					bidv = [ int(l[i*6+5]) for i in range(0,5) ]
					nbid = [ int(l[i*6+6]) for i in range(0,5) ]
					ask = [ Decimal(l[i*6+7]) for i in range(0,5) ]
					askv = [ int(l[i*6+8]) for i in range(0,5) ]
					nask = [ int(l[i*6+9]) for i in range(0,5) ]
					exch = int(l[2])
					recv = int(l[3])

					if not books[uid].replace_book(bid,ask,bidv,askv,nbid,nask,recv,exch):
						logging.error("Error with QUOTE at line {0} in {1}".format(_C,fi.name))

				elif msg=='TRADE':
					exch = int(l[2])
					recv = int(l[3])
					qty = int(l[4])
					price= Decimal(l[5])

					bid = [ Decimal(l[i*6+6]) for i in range(0,5) ]
					bidv = [ int(l[i*6+7]) for i in range(0,5) ]
					nbid = [ int(l[i*6+8]) for i in range(0,5) ]
					ask = [ Decimal(l[i*6+9]) for i in range(0,5) ]
					askv = [ int(l[i*6+10]) for i in range(0,5) ]
					nask = [ int(l[i*6+11]) for i in range(0,5) ]

					if not books[uid].report_trade(price,qty,recv,exch,-1):
						logging.error("Error with TRADE at line {0} in {1}".format(_C,fi.name))
					if not books[uid].replace_book(bid,ask,bidv,askv,nbid,nask,recv,exch):
						logging.error("Error with TRADE at line {0} in {1}".format(_C,fi.name))
				elif msg=='PARTIAL_TRADE':
					exch = int(l[2])
					recv = int(l[3])
					qty = int(l[4])
					price=Decimal(l[5])

					if not books[uid].report_trade(price,qty,recv,exch,-1):
						logging.error("Error with PARTIAL_TRADE at line {0} in {1}".format(_C,fi.name))

				else:
					pass

	return books,symbols
