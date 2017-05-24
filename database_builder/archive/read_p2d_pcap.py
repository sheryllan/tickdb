import os,sys,bz2
import mimetypes as mim
import dpkt

class pcap_reader:
	def __init__(self,dir):
		# read files in directory
		if(os.path.isdir(dir) and os.path.exists(dir):
			pcapfiles = [x for x in os.listdir(dir) if mim.guess_type(x)[0]=='application/x-bzip2']
			if len(pcapfiles)==0:
				raise IOError("no bzip2 files to read")
		else:
			raise IOError("parameter dir={0} is not a valid directory".format(dir))
		# recreate history of UDP datagrams
		for f in pcapfiles:
			b = bz2.open(f)
			b.name = f # work-around for missing field in bz2
			pc = dpkt.pcap.Reader(b) # open pcap file for decoding

			for ts, pkt in pc:
				eth=dpkt.ethernet.Ethernet(pkt)
				if eth.type == dpkt.ethernet.ETH_TYPE_IP: # if ethernet packet
					ip = eth.data
					if ip.p == dpkt.ip.IP_PROTO_UDP: # if IP packet contains UDP
						udp = ip.data

	def decode_udp(pc):
		""" generator function that filter UDP packets on IP and IPv6"""
		for ts,pkt in pc:
			eth = dpkt.ethernet.Ethernet(pkt)
			if eth.type == dpkt.ethernet.ETH_TYPE_IP: # if ethernet packet
				ip = eth.data
				if ip.p == dpkt.ip.IP_PROTO_UDP: # if IP contains UDP
					udp = ip.data
					# Pass the IP addresses, source port, destination port, and data back to the caller.
					yield ( ip.src, udp.sport, ip.dst, udp.dport, udp.data, ip.v)
			elif eth.type == dpkt.ethernet.ETH_TYPE_IP6 :
				ip = eth.data
				if ip.nxt == dpkt.ip.IP_PROTO_UDP:
					udp = ip.data
					# Pass the IP addresses, source port, destination port, and data back to the caller.
					yield ( ip.src, udp.sport, ip.dst, udp.dport, udp.data, ip.v)
			else :
				pass


	def extract_eobi_packets():
	
	def extract_emdi_packets():
