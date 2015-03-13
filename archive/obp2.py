#!/bin/env python3

# Output:
# A : add
# D : delete
# M : modify same priority
# L : modify loss of priority
# T : trade execution
# S : execution summary
# Q : quote (Kospi)

import time, os
import argparse
from datetime import *
import csv,gzip
import mimetypes as mim

args = 0 

# maps indexed by symbol id
symbols = {} # contains symbols' names
obooks = {}  # contains books
ofiles = {}  # contains output files
messages_not_used=set(['END_OPERATION','END_PACKET','EXEC_MODE','ORDERBOOK_STATE','REALTIME'])

#order book:
#  book{contract_id}[ {bid} {ask} ]
#	 bid/ask: { pricelevel: [orders] }
#

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

def printBook(otype, uid, texch, trecv):
	global ofiles
	tse = tots(texch) # convert timestamps to microseconds since midnight
	tsr = tots(trecv)
	ofiles[uid].write("{5},{0},{1},{2},{3},{4},B,0,0".format(tse[0],tse[1],tse[2],tse[4],tsr[4],otype))

	for side in [0,1]:
		book = obooks[uid][side]
		prices = sorted(book.keys(), reverse = side==0)
		count = 1

		for price in prices:
			if count <= args.levels:
				sum = 0
				for o in book[price]:
					sum += o[1]
				ofiles[uid].write(",{0},{1}".format(price,sum))
				count += 1
		if count<=args.levels:
			for i in range(count,int(args.levels)+1):
				ofiles[uid].write(",NA,NA")
	ofiles[uid].write("\n")

def printKospiQuote(uid,exch,recv,l):
	global ofiles
	tse = tots(exch)
	tsr = tots(recv)
	ofiles[uid].write("Q,{0},{1},{2},{3},{4},B,0,0".format(tse[0],tse[1],tse[2],tse[4],tsr[4]))

	price = []
	qty = []
	nbo = []

	for i in range(0,10):
		price.append(float(l[3*i+4]))
		qty.append(int(l[3*i+5]))
		nbo.append(int(l[3*i+6]))

	idx = [i[0] for i in sorted(enumerate(price), key=lambda x:x[1])]
	mid=len(price)//2
	# print bid
	for i in range(mid-1,-1,-1):
		j = idx[i]
		ofiles[uid].write(",{0},{1},{2}".format(price[j],qty[j],nbo[j]))
	# print ask
	for i in range(mid,len(idx)):
		j = idx[i]
		ofiles[uid].write(",{0},{1},{2}".format(price[j],qty[j],nbo[j]))
	ofiles[uid].write("\n")

def printKospiTrade(uid,exch,recv,l):
	global ofiles
	tse = tots(exch)
	tsr = tots(recv)
	ofiles[uid].write("T,{0},{1},{3},{4},T,{5},{6}".format(tse[0],tse[1],tse[2],tse[4],tsr[4],l[5],l[4]))
	ofiles[uid].write(",NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA\n")

def printTrade(uid,exch,recv,price,qty):
	tse = tots(exch)
	tsr = tots(recv)

	ofiles[uid].write("T,{0},{1},{2},{3},{4},T,{5},{6}".format(tse[0],tse[1],tse[2],tse[4],tsr[4],price,qty))
	for i in range(1,int(args.levels)+1):
		ofiles[uid].write(",NA,NA,NA,NA")
	ofiles[uid].write("\n")

def printExecSummary(uid,price,qty,exch,recv):
	tse = tots(exch)
	tsr = tots(recv)
	ofiles[uid].write("S,{0},{1},{2},{3},{4},T,{5},{6}".format(tse[0],tse[1],tse[2],tse[4],tsr[4],price,qty))
	for i in range(1,int(args.levels)+1):
		ofiles[uid].write(",NA,NA,NA,NA")
	ofiles[uid].write("\n")

# find index of tuple in a list of tuples where the first value is the key
# [ (key1, v1), (key2,v2), ... , (keyn,vn) ]
def vindex(loftuples, key):
	i=0
	for v in loftuples:
		if v[0]==key: return i
		i += 1
	return -1

def new_order(uid, oid, side, price, qty, exch):
	global obooks
	book = obooks[uid][side] # ref to symbol-side book

	if price not in book:
		book[price] = [] # create a new level
	if oid not in book[price]: # add new order if not exist
		book[price].append( (oid,qty,exch) ) # append order to price level list
	else:
		modify_order(uid, oid, side, price, qty, exch)

def modify_order(uid, oid, side, price, delta_qty, exch):
	global obooks
	book = obooks[uid][side]

	if price not in book: # check if price exists first
		return
	else:
		indx = vindex(book[price], oid) # get index of orderid(oid) in price level list
		if indx>=0: # if oid is valid
			x=book[price][indx]
			if delta_qty > 0:  # move to the back if quantity increases
				del book[price][indx] # remove order first
				# put it last in priority
				book[price].append( (x[0], x[1]+delta_qty, exch))
			else:	 # qty decrease => priority doesn't change
				book[price][indx] = (x[0], x[1]+delta_qty, x[2])

def delete_order(uid, oid, side, price):
	global obooks
	book = obooks[uid][side]

	if price not in book and price != 0: # wrong price information, ignore order
		return
	if price != 0: # We can use price info
		indx = vindex(book[price], oid) # find index directly
	else: # SGX and EOBI - find price of particular orderID
		for price in book:
			indx = vindex(book[price],oid)
			if indx != -1: break # break if correc price is found
	if indx>=0: # finally remove the order and the level if it's empty
		del book[price][indx]
		if len(book[price])==0:
			del book[price]

#def exec_order(uid,oid,side,price,qty,exch):
#	global obooks
#	book = obooks[uid][side]
#	if price in book:
#		idx = vindex(book[price],oid)
#		if idx != -1:
#			if qty==book[price][idx]
#		return

def calcEOBI(fi):
	global obooks
	global ofiles
	global symbols

	for line in fi: # read file line by line
		l = line.split(':') # get fields from line
		msg = l[0]
		if(len(l)>1): # just to catch bad lines
			uid = int(l[1])

		if msg in messages_not_used:
			pass
		elif msg=='SUBSCRIBE':
			sname=l[2].rstrip()
			if sname != 'N/A':
				# l[1]: symbol id. tuple of 2 dicts (bid, ask)
				obooks [uid] = ({}, {})
				# add symbol's name
				symbols[uid] = l[2].rstrip()
				# get day
				day = os.path.basename(args.ifname).split('.')[1]
				ofiles[uid] = open("{0}/{1}_{2}.csv".format(args.odir,
					day, symbols[uid]), 'w')
				# write header
				ofiles[uid].write("otype,hour,min,sec,exch,recv,type,price,qty")
				for i in range(1,int(args.levels)+1):
					ofiles[uid].write(",bid{0},bidv{0}".format(i))
				for i in range(1,int(args.levels)+1):
					ofiles[uid].write(",ask{0},askv{0}".format(i))
				ofiles[uid].write("\n")
		elif msg=='UNSUBSCRIBE':
			obooks.pop(uid,None)
			symbols.pop(uid,None)
		elif msg=='CLEAR':
			obooks[uid] = ({}, {})
		else:
			# Get basic timestamps
			exch= int(l[2])
			recv= int(l[3])

			# these fiels are from Kospi
			if msg=='QUOTE':
				printKospiQuote(uid,exch,recv,l)
			elif msg=='TRADE' or msg=='PARTIAL_TRADE':
				printKospiTrade(uid,exch,recv,l)
			elif msg=="EXEC_SUMMARY":
				#side = int(l[6])-1
				qty = int(l[7])
				price = float(l[8])
				printExecSummary(uid,price,qty,exch,recv)
			else:
				# these fields only exist in Eurex
				sordid= l[4].split('/')
				side= int(sordid[0])-1
				oid = int(sordid[1])
				price=float(l[6])
	
				if msg=='ADD_ORDER':
					qty = int(l[7])
					new_order(uid,oid,side,price,qty,exch)
					printBook("A",uid,exch,recv)
	
				elif msg=='REDUCE_ORDER':
					qty = int(l[7])
					modify_order(uid,oid,side,price,-qty,exch)
					printBook("M",uid,exch,recv)
	
				elif msg=='REPLACE_ORDER':
					qty = int(l[7])
					newsideordid = l[5].split('/')
					if oid==newsideordid[1]:
						# Queue priority not lost, qty reduced
						modify_order(uid, oid, side, price, qty, exch)
						printBook("M",uid,exch,recv)
					else:
						# Queue priority lost, qty increased or price/side changed
						delete_order(uid, oid, side, 0)
						new_order(uid,int(newsideordid[1]),int(newsideordid[0])-1,price,qty,exch) 
						printBook("L",uid,exch,recv)

				elif msg=='DEL_ORDER':
					delete_order(uid, oid, side, 0)
					printBook("D",uid, exch,recv)

				elif msg=='EXECUTION':
					qty = int(l[7])
					printTrade(uid,exch,recv,price,qty)
				else:
					pass

def main():
	global args

	parser = argparse.ArgumentParser(__file__,description="order book parser")
	parser.add_argument("--levels","-l",help="max OB levels", type=int,default=5)
	parser.add_argument("--odir","-o",  help="output directory",type=str,default=".")
#	parser.add_argument("--eurex_eobi","-e", help="setup specific algorithms for dealing with Eurex EOBI",type=bool,default=True)
	parser.add_argument("ifname",help="input filename",type=str)
	args = parser.parse_args()

	# Run decoding
	if mim.guess_type(args.ifname)[1]=='gzip':
		fi = gzip.open(args.ifname,mode='rt')
	else:
		fi = open(args.ifname)

	calcEOBI(fi)

if __name__ == '__main__':
	main()
