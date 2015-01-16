#!/bin/env python3

# import modules
import time,os,argparse,sys
import csv
import gzip,lzma
import mimetypes as mim
from datetime import *
from decimal import *
from numpy import *
from pandas import *
import logging
from qtg_text_file_decoder import *

# ============
# Main program
# ============
def main():
	# Setup command line parameters
	parser = argparse.ArgumentParser(__file__,description="order book parser")
	parser.add_argument("--levels", "-l", help="max OB levels", type=int,default=5)
	parser.add_argument("--odir",   "-o", help="output directory",type=str,default=".")
	parser.add_argument("--decoder","-d", help="decoder type: qtg_eobi,qtg_emdi,pcap_eobi,pcap_emdi",type=str,default="qtg_eobi")
	parser.add_argument("--format", "-f", help="output format: TOB,...",type=str,default=".")
	parser.add_argument("--ofmt",   "-m", help="output file format: csv, csv.xz, hdf,...",type=str,default="csv.xz")
	parser.add_argument("--insdb",  "-i", help="Instrument database",type=str,default="/data/database/instRefdataCoh.csv")
	parser.add_argument("--log",    "-g", help="loggin file",type=str,default="./obp.log")
	parser.add_argument("ifname",help="input filename",type=str)
	args = parser.parse_args()

	# Open loggin
	logging.basicConfig(filename=args.log,level=logging.DEBUG)

	# Run decoding
	if mim.guess_type(args.ifname)[1]=='gzip':
		fi = gzip.open(args.ifname,mode='rt')
	else:
		fi = open(args.ifname)

	date = os.path.basename(args.ifname).split('.')
	if len(date)>1:
		date=date[1]
	else:
		sys.exit(1)
	
	if args.decoder=="qtg_eobi":
		books,symbols = decode_qtg_EOBI(fi,args.levels,date)
	elif args.decoder=="qtg_emdi":
		books,symbols = decode_qtg_EMDI(fi,args.levels,date)
	elif args.decoder=="pcap_eobi":
		books,symbols = decode_pcap_EOBI(fi,args.levels,date)
	elif args.decode=="pcap_emdi":
		books,symbols = decode_pcap_EMDI(fi,args.levels,date)

	# Write files
	df = read_csv(args.insdb)
	for uid in books:
		x = DataFrame(books[uid].output,columns=books[uid].header)
		prodname = df[df['#instuid']==uid].iat[0,3]
		filename = args.odir+"/"+prodname+"_"+"20"+date+"."+args.ofmt
		if args.ofmt=="csv":
			x.to_csv(filename,na_rep="NA",index=False,index_label=False)
		elif args.ofmt=="csv.xz":
			xzfile = lzma.open(filename,"wt",format=lzma.FORMAT_XZ,preset=9|lzma.PRESET_EXTREME)
			x.to_csv(xzfile,na_rep="NA",index=False,index_label=False)

	sys.exit(0)

if __name__ == '__main__':
	main()
