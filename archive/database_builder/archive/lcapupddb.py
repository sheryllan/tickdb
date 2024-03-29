#!/usr/bin/env python3.5

# IMPORTANT: this code is only a translation of the previous code written in bash and awk
# As the project is becoming more complex, I implemented it again in Python.
# That's why the structure and the way I manipulate strings can be strange sometimes
# More improvement is needed to make it more python-integrated.
# - David 18-03-2016

import time,os,argparse,sys
import json
import re
import subprocess
import csv

# clean up bad characters in Reference files
def filtchar(x):
	a=bytearray(range(0,256))
	b=bytearray(b'xxxxxxxxxx\nxxxxxxxxxxxxxxxxxxxxx !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7fxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
	t=bytes.maketrans(a,b)
	return x.translate(t)

def merge_first_fields(l,hdrsize):
	d = len(l)-hdrsize # how many fields to merge at the front of the list
	# merge and add the rest of the list. LISP ??? :-D
	return [",".join(l[0:d+1])]+l[d+1:len(l)]

def process_ref_data(rawdata_dir,ref_data_file):
	# find all ref data files in the raw data repository
	ref_files=[]
	for root,dirs,files in os.walk(rawdata_dir):
		for f in files:
			if f.endswith(".csv") and 'ReferenceData' in f:
				ref_files.append(os.path.join(root,f))

	# read all the files
	ref_data={}
	hdrsize = 0
	for f in ref_files:
		with open(f,'rb') as rf:
			# read entire file
			for line in rf:
				# remove white spaces first
				line = line.replace(b' ',b'')
				# remove bad chars, convert to str, split to csv elements
				l = filtchar(line).decode('utf-8').rstrip(',\n').split(',')
				# get header size
				if l[0]=='ProductID':
					hdrsize=len(l)
				# some lines have a product ID with commas
				if hdrsize>0 and len(l)>hdrsize:
					l = merge_first_fields(l,hdrsize)
				# get Product Id (sometimes ProductID have commas in their name)
				ref_data[l[0]] = l # store line, replacing each elmt with its last version

	return ref_files
	
	# write output to CSV file
	with open(ref_data_file,'w') as f:
		writer=csv.writer(f)
		writer.writerow(ref_data['ProductID']) # write header
		del ref_data['ProductID']
		writer.writerows([ref_data[k] for k in ref_data]) # dump the rest of the file

def find_files(rawdata_dir,processed_files):
	# read list of already processed files
	proc_files = open(processed_files,'r').readlines()
	proc_files = [x[:-1] for x in proc_files]

	# find all files in the raw data repository
	raw_files=[]
	for root,dirs,files in os.walk(rawdata_dir):
		for f in files:
			if f.endswith(".csv"): # Reactor writes uncompressed csv files
				raw_files.append(os.path.join(root,f))

	# extract the new files which have never been processed before
	return [x for x in set(raw_files).difference(set(proc_files)) if 'ReferenceData' not in x]

def create_month_directories(dbdir,rawfiles):
	# extract months from filenames
	months = [ os.path.basename(os.path.dirname(x))[:-2] for x in rawfiles ]

	# remove duplicates
	months = list(set(months))

	# create non existing directories
	for m in months:
		d = os.path.join(dbdir,m)
		if not os.path.exists(d):
			os.makedirs(d)

# return a dict of prefix file names as a key and a list of timestamps for each complete file name
# because some products have several capture files per day and we want to concat them into
# one final file per day
def create_dict_of_files(rawfiles):
	D = {}
	for f in rawfiles:
		# decompose file name to extract the prefix only
		# the timestamp is always the last field
		# example: <some exchange/appliance>-A50-F-APR2016-20160317-072908.csv ->
		x = os.path.basename(f).split('.')[0].split('-') # remove .csv and decompose
		timestamp = int(x[len(x)-1]) # extract timestamp
		x.pop() # remove timestamp
		short_name = os.path.join(os.path.dirname(f),'-'.join(x)) # recompose filename

		# Store the result in a tuple
		if short_name not in D:
			D[short_name] = [timestamp]
		else:
			D[short_name].append(timestamp)

	# sort each sub list to be sure we concat files chronologically
	for k in D:
		D[k].sort()
	
	return D

def usage():
	print("Usage: ./liquid_capture_update_db.py <json config file>",file=sys.stderr)

def main(argv):
	if(len(argv)<2):
		usage()
		sys.exit(1)
	config = argv[1]

	# Test json conf file first
	if not os.path.isfile(config):
		print("Error: json config file",config," does not exist",file=sys.stderr)
		sys.exit(1)

	# prevent running 2 instances of the update_db program
	pid = str(os.getpid())
	pidfile = "/tmp/mypid.pid"
	while os.path.exists(pidfile):
		time.sleep(20)
	pidfile = open("/tmp/mypid.pid",'w')
	pidfile.write(pid)

	try:
		with open(config) as jsonfile:
			# Read json config
			cfg = json.load(jsonfile)
			rawdata_dir = cfg['liquid_capture']['src_dir']
			processed_files = cfg['liquid_capture']['dbprocessed']
			dbdir = cfg['liquid_capture']['dbdir']
			ref_data_file = cfg['liquid_capture']['instdb']
			prefix = cfg['liquid_capture']['prefix']
			tmpdir = cfg['tmpdir']
			nbcores = cfg['nbcores']
			gnupar = os.popen("which parallel").read().strip()

			# Check if GNU parallel exists
			if gnupar=='':
				print("Error: GNU parallel not found",file=sys.stderr)
				sys.exit(1)

			# Check if database file of processed files exists
			if not os.path.exists(processed_files):
				open(processed_files,'w').close() # create empty file

			# 1- retrieve file names from rawdata
			# 2- Load PROCESSED_FILES
			# 3- Make the difference to find new files
			files_to_process = find_files(rawdata_dir,processed_files)

			# 4- Create directories if they don't exist
			create_month_directories(dbdir,files_to_process)
	
			# 5- Create dict of files to process: we can have several for the same instrument
			# in the same day (when the capture device crashes for example)
			dfiles = create_dict_of_files(files_to_process)
	
			# 6- Generate job files
			jobfile=[]
			j=''
			for x in dfiles:
				if len(dfiles[x])>1:
					j='(cat '+ x+'-'+format(dfiles[x][0],'06d')+'.csv;'
					for i in range(1,len(dfiles[x])):
						j=j+'tail -n +2 '+x+'-'+format(dfiles[x][i],'06d')+'.csv;'
					j=j+')|bzip2 -c -9 '
				else:
					j = 'bzip2 -c -9 '+x+'-'+format(dfiles[x][0],'06d')+'.csv'
				
				# get month of the file
				month = os.path.basename(os.path.dirname(x))[:-2]
				# make a clean destination file name, removing the exchange_appliance name
				destination = os.path.join(dbdir,month,os.path.basename(x))+'.csv.bz2'
				for pr in prefix:
					destination = destination.replace(pr,'')
				j = j+' > '+ destination
				# add job to job list
				jobfile.append(j)

			# 7- pipe jobs to GNU parallel
			proc = subprocess.Popen([gnupar,'-j',str(nbcores)],stdin=subprocess.PIPE)
			proc.communicate('\n'.join(jobfile).encode())
			proc.wait()
  
   			# 8- update reference data
			ref_files = process_ref_data(rawdata_dir,ref_data_file)
   
	   		# 9- compress the processed files
	   		# note: these files will not appear in the next round because we filter out
	   		# everything but those ending with .csv
			x=["sudo","-b","-n",gnupar,'-j',str(nbcores),'xz','-9',':::']
			x.extend(files_to_process)
			x.extend(ref_files)
			proc = subprocess.Popen(x,stdin=subprocess.PIPE)
			proc.wait()

			# NOTE: the rawdata files are written by user rsprod.dev without write permission to the group dev
			# In /etc/sudoers I added the following lines:
			# Cmnd_Alias COMPRESSION = /usr/bin/xz, /usr/local/bin/parallel
			# %dev ALL=NOPASSWD: COMPRESSION
			# to allow members of the group dev (like my own user) to run sudo xz

   			# 10- update list of processed_files
			pf = open(processed_files,'a')
			pf.write('\n'.join(files_to_process))
			pf.close()
 
	finally:
		os.unlink("/tmp/mypid.pid")

if __name__=="__main__":
	main(sys.argv)
