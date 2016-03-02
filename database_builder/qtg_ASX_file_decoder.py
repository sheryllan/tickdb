import time,os,logging

from datetime import *
from decimal import *
from numpy import *
from pandas import *
import Book

def decode_qtg_ASX(fi,levels,day):
	books = {}  # contains books
	symbols = {} # contains symbols' names

	_C = 0
	print(_C,datetime.now())
	_t = datetime.now()
	for line in fi: # read file line by line
		_C += 1
		if _C%100000==0:
			print(_C,datetime.now()-_t)
			_t = datetime.now()

		l = line.split(':') # get fields from line
		msg = l[0]

		# Try to catch bad lines
		if(len(l)>1): # just to catch bad lines
			uid = int(l[1])
		else:
			continue
		if msg=='SUBSCRIBE':
			sname=l[2].rstrip()
			if sname != 'N/A':
				symbols[uid] = l[2].rstrip()
				books[uid] = Book.Book(uid,day,levels)
		elif uid in books:
			if msg in ['UNSUBSCRIBE','END_RECV','END_PACKET','ORDERBOOK_STATE','REALTIME']:
				pass
			else: # dated messages now with recv and exch in common
				if msg=='CLEAR':
					exch = int(l[2])
					recv = exch
					books[uid].clear(recv,exch)
				elif msg=='ADD_ORDER':
					exch= int(l[2])
					recv= int(l[3])
					oid = l[4]
					side=int(l[5])-1
					price=Decimal(l[6])
					qty = int(l[7])
					if not books[uid].add_order(side,price,oid,qty,recv,exch):
						logging.error("Error with ADD_ORDER at line {0} in {1}".format(_C,fi.name))
				elif msg=='DEL_ORDER':
					exch= int(l[2])
					recv= int(l[3])
					oid = l[4]
					if not books[uid].delete_order(-1,-1,oid,recv,exch):
						logging.error("Error with DEL_ORDER at line {0} in {1}".format(_C,fi.name))
						#logging.error(line)
				elif msg=='REDUCE_ORDER':
					exch= int(l[2])
					recv= int(l[3])
					oid = l[4]
					qty = int(l[5])
					order = books[uid].find_order(oid)
					if order is None:
						logging.error("ERROR at line {0}, order does not exist in {1}".format(_C,fi.name))
						#logging.error("{} {}".format(l[4],l[5]))
						#logging.error("{} {}".format(oldoid,newoid))
						#logging.error(line)
					else:
						if not books[uid].modify_inplace(order.side,oid,qty,recv,exch):
							logging.error("Error with REDUCE_ORDER at line {0} in {1}".format(_C,fi.name))
							#logging.error(line)
				elif msg=='REPLACE_ORDER':
					exch= int(l[2])
					recv= int(l[3])
					oid = l[4]
					price = Decimal(l[5])
					qty = int(l[6])
					order = books[uid].find_order(oid)
					if not books[uid].replace_order(order.side,price,oid,qty,recv,exch,oid):
						logging.error("Error with REPLACE_ORDER at line {0} in {1}".format(_C,fi.name))
						#logging.error("{} {}".format(l[4],l[5]))
						#logging.error("{} {}".format(oldoid,newoid))
						#logging.error(line)
				elif msg=='EXECUTION':
					exch= int(l[2])
					recv= int(l[3])
					#oid = l[4]
					side = int(l[5])-1
					price = Decimal(l[6])
					qty = int(l[7])
					books[uid].report_trade(price,qty,recv,exch,side)
				elif msg=='EXECUTION2':
					exch= int(l[2])
					recv= int(l[3])
					#buyer_oid = l[4]
					#seller_oid = l[5]
					price = Decimal(l[6])
					qty = int(l[7])
					books[uid].report_trade(price,qty,recv,exch)
				else:
					pass

	return books,symbols
