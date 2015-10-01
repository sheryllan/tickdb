#!/usr/bin/env python3

import os
import tarfile
import uuid
import shutil
import gc
import sys
import pathlib
import time
import pcapfile

from datetime import *
from Book import *
from struct import unpack

def unpack_tar_file(tfile,tmpdir='/tmp'):
	if os.path.exists(tfile) and os.path.isfile(tfile) and tarfile.is_tarfile(tfile):
		# make a uniquely named temp directory
		uniqdir = tmpdir+'/'+str(uuid.uuid4())
		os.mkdir(uniqdir)

		# extract files
		print("unpack tar ", datetime.now())
		t = tarfile.open(tfile)
		t.extractall(unique_dirname)
		t.close()
	
		return uniqdir

if __name__=="__main__":
	main()

#def decode_pcap_files(fnames):
#	channel = []
#	time_spent = 0
#	N = 0
#	while fnames:
#		t0=time.time()
#		fi = open(fnames.pop(0),'rb')
#		pcap = pcapfile.savefile.load_savefile(fi,layers=3)
#		# parse packets down to UDP datagram and extract IP datagram
#		channel.extend([x.packet.payload for x in pcap.packets])
#		fi.close()
#		t1 = time.time() - t0
#		N=N+1
#		time_spent = time_spent+t1
#		print("channel={} packets time spent:{:.1f} s avg time:{:.1f} s".format(len(channel), time_spent, time_spent/N))
#
#	return channel
