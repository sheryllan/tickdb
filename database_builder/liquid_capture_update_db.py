#!/usr/bin/env python3

# IMPORTANT: this code is only a translation of the previous code written in bash and awk
# As the project is becoming more complex, I implemented it again in Python.
# That's why the structure and the way I manipulate strings can be strange sometimes
# More improvement is needed to make it more python-integrated.
# - David 18-03-2016

import time,os,argparse,sys
import json
import re
import subprocess

def find_files(rawdata_dir,processed_files):
	# read list of already processed files
	proc_files = open(processed_files,'r').readlines()
	proc_files = [x[:-1] for x in proc_files]

	# find all files in the raw data repository
	raw_files=[]
	for root,dirs,files in os.walk(rawdata_dir):
		for f in files:
			if f.endswith(".csv"):
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
		# decompose file name
		# example: A50-F-APR2016-20160317-072908.csv ->
		# x[0] = A50 x[1] = F x[2] = APR2016 x[3] = 20160317 x[4] = 072908 x[5]=csv
		x = re.findall(r"[\w']+", os.path.basename(f))
		if x[0] != 'ReferenceData':
			# make a name without the timestamp (like x[4]=072908)
			short_name = os.path.join(os.path.dirname(f), x[0]+'-'+x[1]+'-'+x[2]+'-'+x[3])
	
			# Store the result in a tuple
			if short_name not in D:
				D[short_name] = [int(x[4])]
			else:
				D[short_name].append(int(x[4]))

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
				month = re.findall(r"[\w']+",os.path.basename(x))[3][:-2]
				# add destination
				j = j+' > '+ os.path.join(dbdir,month,os.path.basename(x))+'.csv.bz2'
				# add job to job list
				jobfile.append(j)

			# 7- pipe jobs to GNU parallel
			#proc = subprocess.Popen([gnupar,'-j',str(nbcores),' &>/dev/null'],stdin=subprocess.PIPE)
			proc = subprocess.Popen([gnupar,'-j',str(nbcores)],stdin=subprocess.PIPE)
			proc.communicate('\n'.join(jobfile).encode())
			proc.wait()

			# 8- update processed_files
			pf = open(processed_files,'a')
			pf.write('\n'.join(files_to_process))
			pf.close()

	finally:
		os.unlink("/tmp/mypid.pid")

if __name__=="__main__":
	main(sys.argv)
