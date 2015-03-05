#!/bin/env python3

# import modules
import time,os,argparse,sys
import csv
import gzip,lzma,bz2
import mimetypes as mim
from datetime import *
from decimal import *
from numpy import *
from pandas import *
import logging
from qtg_EOBI_file_decoder import *
from qtg_CME_file_decoder import *

# ============
# Main program
# ============
def main():
	# Setup command line parameters
	parser = argparse.ArgumentParser(__file__,description="order book parser")
	parser.add_argument("--levels", "-l", help="max OB levels", type=int,default=5)
	parser.add_argument("--odir",   "-o", help="output directory",type=str,default=".")
	parser.add_argument("--decoder","-d", help="decoder type: qtg_eobi,qtg_cme,qtg_emdi,pcap_eobi,pcap_emdi",type=str,default="qtg_eobi")
	parser.add_argument("--format",   "-f", help="output file format: csv, csv.xz, csv.bz2",type=str,default="csv")
	parser.add_argument("--insdb",  "-i", help="Instrument database",type=str,default="/mnt/data/qtg/instRefdataCoh.csv")
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
	elif args.decoder=="qtg_cme":
		books, symbols = decode_qtg_CME(fi,args.levels,date)
	elif args.decoder=="qtg_emdi":
		books,symbols = decode_qtg_EMDI(fi,args.levels,date)
	elif args.decoder=="pcap_eobi":
		books,symbols = decode_pcap_EOBI(fi,args.levels,date)
	elif args.decode=="pcap_emdi":
		books,symbols = decode_pcap_EMDI(fi,args.levels,date)

	# Write files
	df = read_csv(args.insdb)
	for uid in books:
		# DEBUG
#		print(uid,"--", len(books[uid].output),"--")
#		if uid==1587:
#			print(books[uid].output)
#			print(books[uid].header)
#			print(type(books[uid].output))
#			books[uid].output=[list(range(1,21)), list(range(2,22))]
		# DEBUG

		if len(books[uid].output)>0:
			x = DataFrame(books[uid].output,columns=books[uid].header)
		else:
			x = DataFrame([ [np.nan]*len(books[uid].header) ],columns=books[uid].header)

		prodname = df[df['#instuid']==uid].iat[0,3]
		output_file = args.odir+"/"+prodname+"_"+"20"+date+"."+args.format

		if args.format=="csv":
			pass
		elif args.format=="csv.xz":
			output_file = lzma.open(output_file,"wt",format=lzma.FORMAT_XZ,
					preset=9|lzma.PRESET_EXTREME)
		elif args.format=="csv.bz2":
			output_file = bz2.open(output_file,"wt",9,"UTF-8")

		x.to_csv(output_file,na_rep="NA",index=False,index_label=False)

	sys.exit(0)

if __name__ == '__main__':
	main()
