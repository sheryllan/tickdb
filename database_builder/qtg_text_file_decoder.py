import time,os,logging

from datetime import *
from decimal import *
from numpy import *
from pandas import *

import Book

def decode_qtg_EOBI(fi,levels,day):
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
			pass
		if msg=='SUBSCRIBE':
			sname=l[2].rstrip()
			if sname != 'N/A':
				symbols[uid] = l[2].rstrip()
				books[uid] = Book.Book(uid,day,levels)
		elif uid in books:
			if msg in ['UNSUBSCRIBE','END_OPERATION','END_PACKET','ORDERBOOK_STATE','REALTIME']:
				pass
			else: # dated messages now with recv and exch in common
				exch= int(l[2])
				recv= int(l[3])
				if msg=='CLEAR':
					books[uid].clear(recv,exch)
				elif msg=="EXEC_SUMMARY":
					qty = int(l[7])
					price = Decimal(l[8])
					books[uid].exec_summary(price,qty,recv,exch)
				elif msg=='ADD_ORDER':
					side= int(l[4].split('/')[0])-1
					oid = l[4]
					price=Decimal(l[6])
					qty = int(l[7])
					if not books[uid].add_order(side,price,oid,qty,recv,exch):
						logging.error("Error with ADD_ORDER at line {}".format(_C))
						logging.error(line)

				elif msg=='REDUCE_ORDER':
					side= int(l[4].split('/')[0])-1
					oid = l[4]
					delta_qty = int(l[5])
					price=Decimal(l[6])
					order = books[uid].find_order(oid)
					if order is None:
						logging.error("ERROR at line {}, order does not exist".format(_C))
						logging.error("{} {}".format(l[4],l[5]))
						logging.error("{} {}".format(oldoid,newoid))
						logging.error(line)
					else:
						oldqty = order.qty
						if not books[uid].modify_inplace(side,oid,oldqty-delta_qty,recv,exch):
							logging.error("Error with REDUCE_ORDER at line {}".format(_C))
							logging.error(line)
				elif msg=='REPLACE_ORDER':
					side=int(l[4].split('/')[0])-1
					oldoid  = l[4]
					newoid  = l[5]
					newprice=Decimal(l[6])
					newqty = int(l[7])
					if not books[uid].replace_order(side,newprice,newoid,newqty,recv,exch,oldoid):
						logging.error("Error with REPLACE_ORDER at line {}".format(_C))
						logging.error("{} {}".format(l[4],l[5]))
						logging.error("{} {}".format(oldoid,newoid))
						logging.error(line)
				elif msg=='DEL_ORDER':
					side= int(l[4].split('/')[0])-1
					oid = l[4]
					if not books[uid].delete_order(side,-1,oid,recv,exch):
						logging.error("Error with DEL_ORDER at line {}".format(_C))
						logging.error(line)
				elif msg=='EXECUTION':
					price=Decimal(l[6])
					qty = int(l[7])
					books[uid].report_trade(price,qty,recv,exch)
				else:
					pass

	return books,symbols
