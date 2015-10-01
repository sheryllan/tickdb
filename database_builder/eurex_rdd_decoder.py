#!/usr/bin/env python3
import uuid
import os
import tarfile
import shutil
import sys
import pathlib
import time
import argparse
from datetime import *
from enum import Enum

def which(program):
	""" Find executable path """
	def is_exe(fpath):
		return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

	fpath, fname = os.path.split(program)
	if fpath:
		if is_exe(program):
			return program
	else:
		for path in os.environ["PATH"].split(os.pathsep):
			path = path.strip('"')
			exe_file = os.path.join(path, program)
			if is_exe(exe_file):
				return exe_file
	return None

def unpack_tar_file(tfile,tmpdir='/tmp',verbose=True):
	if os.path.exists(tfile) and os.path.isfile(tfile) and tarfile.is_tarfile(tfile):
		# make a uniquely named temp directory
		uniqdir = tmpdir+'/'+str(uuid.uuid4())
		os.mkdir(uniqdir)

		if verbose:
			print("unpacking tar file "+tfile+" in dir "+uniqdir)
			print("starting at ", datetime.now())

		# extract files
		t = tarfile.open(tfile)
		t.extractall(uniqdir)
		t.close()

		if verbose:
			print("done at ", datetime.now())
	
		return uniqdir

class state(Enum):
	init = 0
	reading_datagram=1
	wait_for_snapshot=2

class channel(Enum):
	A=1
	B=2

class code(Enum):
	new_scid = 1
	too_old = 2
	on_time = 3
	too_early = 4
	broken_stream = 5
	good = 6
	EOF = 7

class decode:
	def __init__(self):
		self.udp_state = { channel.A : state.init, channel.B:state.init}
		self.rdd_state = { channel.A : state.init, channel.B:state.init}
		self.md_state  = { channel.A : state.init, channel.B:state.init}
		self.sendercompid = {}
		self.queue = []
		self.max_queue = 20

	def line2msg(self,string):
		l = string.split(':')
		d = {l[i]:l[i+1].rstrip("\r\n") for i in range(0,len(l),2)}
		return d

	def check_packet_seq(self,packet):
		print("CHECK PACKET SEQ")
		print(packet)
		new_scid = int(packet[0]['SenderCompID'])
		new_psn  = int(packet[0]['PacketSeqNum'])

		if new_scid in self.sendercompid:
			current_psn = self.sendercompid[new_scid]
		else:
			return code.new_scid

		if new_scid <= current_psn:
			return code.too_old
		elif new_scid == current_psn+1:
			return code.on_time
		else:
			return code.too_early

	def process_packet(self,packet,apply_fct):
		# try to use packets in queue first
		if len(self.queue)>0:
			processed=1
			while processed>0:
				processed=0
				for p in self.queue:
					if self.check_packet_seq(p) == code.on_time:
						apply_fct(p)
						processed+=1
			print("processed queue")
		# check new packet is in sequence
		packet_time = self.check_packet_seq(packet)
		if packet_time == code.on_time:
			print("packet on time, calling apply")
			apply_fct(p)
		elif packet_time == code.too_early:
			print("too early")
			self.queue.append(packet)
			# check if queue is not too big
			if len(self.queue) > self.max_queue:
				return code.broken_stream
		else:
			return code.broken_stream

	def process_line(self,line,packet,header_type, apply_fct,chan):
		""" Process one line of FAST text. If a UDP datagram is complete with this line
		then process the packet otherwise keeps accumulating lines
		"""
		data=self.line2msg(line)
		print(data)
		print("==========================================================================")
		if self.udp_state[chan] == state.init:
			if header_type in data:
				self.udp_state[chan] = state.reading_datagram
				packet.append(data)
				print("init -> reading_datagram")
		elif self.udp_state[chan] == state.reading_datagram:
			if header_type in data: 
				print("processing packet")
				result = self.process_packet(packet,apply_fct)
				if result==code.broken_stream:
					self.md_state[chan] = state.wait_for_snapshot
				packet = [data]
				return result
			elif header_type not in data:
				print("append")
				packet.append(data)
		return code.good

	def parse_RDD_text(self,namea,nameb):
		""" Parse 2 files for channel A and B of FAST packet and stop when data are exhausted
		on both channels
		"""
		filea=open(namea)
		fileb=open(nameb)
		linea=filea.readlines()
		lineb=fileb.readlines()
		filea.close()
		fileb.close()
	
		packeta=[]
		packetb=[]
		ia=0
		ib=0
		
		while ia<len(linea) or ib<len(lineb):
			# channel A
			if ia<len(linea):
				if self.process_line(linea[ia],packeta,'RDPacketHeader',testapply,channel.A) == code.broken_stream:
					print("##### BROKEN STREAM #####")
					break
				ia+=1
	
			# channel B
			if ib<len(lineb):
				if self.process_line(lineb[ib],packetb,'RDPacketHeader',testapply,channel.B) == code.broken_stream:
					print("##### BROKEN STREAM #####")
					break
				ib+=1

		return(ia,ib)

	def generate_big_decoded_RDD(self,emdi_dir,verbose=True):
		# get pcap files list
		filesa=''
		filesb=''
		for f in sorted(os.listdir(emdi_dir+'/emdi-a')):
			filesa += ' ' + emdi_dir+'/emdi-a/'+f
		for f in sorted(os.listdir(emdi_dir+'/emdi-b')):
			filesb += ' ' + emdi_dir+'/emdi-b/'+f
	
		# result files
		outputa = '/tmp/'+'rdd_a_'+uuid.uuid4().hex
		outputb = '/tmp/'+'rdd_b_'+uuid.uuid4().hex
	
		if verbose:
			print("results in "+outputa+" and "+outputb)
	
		# generate command
		nbcpu = str(int(os.cpu_count() / 2))
		parallela=(which("parallel") + " -j " + nbcpu
					+ ' --keep-order pcapread -f compact EurexRDD {} ::: ' 
					+ filesa
					+ ' > ' + outputa)
		parallelb=(which("parallel") + " -j " + nbcpu
					+ ' --keep-order pcapread -f compact EurexRDD {} ::: ' 
					+ filesb
					+ ' > ' + outputb)
	
		# run command
		if verbose:
			print("start pcap on channel A decoding at ", datetime.now())
		os.system(parallela)
		if verbose:
			print("start pcap on channel B decoding at ", datetime.now())
		os.system(parallelb)
		if verbose:
			print("done at ",datetime.now())
			print("output files are ",outputa, " " ,outputb)
	
		return (outputa,outputb)


	def decode_eurex_pcap(self,tarfile,verbose=True):
		# unpack tar file to /tmp
		#print(datetime.now(), " unpacking tar file ", tarfile)
		#emdi_dir = unpack_tar_file(tarfile)
		#emdi_dir = '/tmp/8d6abda6-9293-4144-b30e-e7168ef1d549'
		#print(datetime.now(), " emdi dir: ",emdi_dir)
	
		# generate 2 files for each channel A and B with RDD data
		#print(datetime.now(), " generate big pcap file")
		#(namea,nameb) = self.generate_big_decoded_RDD(emdi_dir)

		namea='/tmp/rdd_a_35ddc5df3fcf4136864bfb805351a299'
		nameb='/tmp/rdd_b_5971cfcae178493e9e7011e0ca5a6516'

		print(datetime.now(), " files are : ",namea, " ",nameb)
		print(datetime.now(), " parsing pcap files")
		self.parse_RDD_text(namea,nameb)

def testapply(packet):
	print("apply packet:",len(packet)," msgs. ")
	
if __name__=="__main__":
#	parser = argparse.ArgumentParser(__file__)
#	parser.add_argument("--verbose","-v", help="verbose mode", type=bool, default=False)
#	parser.add_argument("tarfile",help="input tar file name",type=str)
#	args = parser.parse_args()

	x=decode()
	x.decode_eurex_pcap("toto")
