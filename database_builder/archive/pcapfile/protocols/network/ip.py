"""
IP protocol definitions.
"""

import binascii
import ctypes
import struct


class IP(ctypes.Structure):
	"""
	Represents an IP packet.
	"""

	_fields_ = [('v', ctypes.c_ushort),		# version
			('hl', ctypes.c_ushort),	# internet header length
			('tos', ctypes.c_ubyte),	# type of service
			('len', ctypes.c_ushort),	# total length
			('id', ctypes.c_ushort),	# IPID
			('flags', ctypes.c_ushort, 3),  # flags
			('off', ctypes.c_ushort, 13),   # fragmentation offset
			('ttl', ctypes.c_ubyte),	# TTL
			('p', ctypes.c_ubyte),		# protocol
			('sum', ctypes.c_ushort),	# checksum
			('src', ctypes.c_char_p),	# source address
			('dst', ctypes.c_char_p),	# destination address
			('opt', ctypes.c_char_p),	# IP options
			('pad', ctypes.c_char_p)]	# padding bytes
	payload = None					# packet payload

	def __init__(self, packet, layers=0):
		# parse the required header first, deal with options later
		magic = struct.unpack('!B',packet[0:1])[0]
		assert ((magic & 0b1100) == 4 and
				(magic & 0b0111) > 4), 'not an IPv4 packet.'

		fields = struct.unpack('!BBHHHBBHII', packet[:20])
		self.v = fields[0] & 0b1100
		self.hl = fields[0] & 0b0111
		self.tos = fields[1]
		self.len = fields[2]
		self.id = fields[3]
		self.flags = fields[4] >> 13
		self.off = fields[4] & 0x1fff
		self.ttl = fields[5]
		self.p = fields[6]
		self.sum = fields[7]
		self.src = ctypes.c_char_p(parse_ipv4(fields[8]))
		self.dst = ctypes.c_char_p(parse_ipv4(fields[9]))

		if self.hl > 0x14:
			start = 0x15
			end = self.len - 0x14
			self.opt = binascii.hexlify(packet[start:end])
		else:
			self.opt = b'\x00'
			payload = binascii.hexlify(packet[0x14:])
			self.payload = payload

		self.pad = b'\x00'

		if layers:
			self.load_transport(layers)

	def load_transport(self, layers=1):
		"""
		Given an IP packet, determine the appropriate sub-protocol;
		If layers is greater than zero determine the type of the payload
		and load the appropriate type of network packet. It is expected
		that the payload be a hexified string. The layers argument determines
		how many layers to descend while parsing the packet.
		"""
		if layers:
			ctor = payload_type(self.p)[0]
		if ctor:
			ctor = ctor
			payload = binascii.unhexlify(self.payload)
			self.payload = ctor(payload, layers-1)
		else:
			# if no type is found, do not touch the packet.
			pass

	def __str__(self):
		packet = 'ipv4 packet from %s to %s carrying %d bytes'
		packet = packet % (self.src, self.dst, (len(self.payload) / 2))
		return packet


def parse_ipv4(address):
	"""
	Given a raw IPv4 address (i.e. as an unsigned integer), return it in
	dotted quad notation.
	"""
	raw = struct.pack('I', address)
	octets = struct.unpack('BBBB', raw)[::-1]
	ipv4 = b'.'.join([('%d' % o).encode('ascii') for o in bytearray(octets)])
	return ipv4


def strip_ip(packet):
	"""
	Remove the IP packet layer, yielding the transport layer.
	"""
	if not type(packet) == IP:
		packet = IP(packet)
	payload = packet.payload

	if type(payload) == str:
		payload = binascii.unhexlify(payload)
	return payload

def payload_type(ip_protocol):
	"""
	Returns the appropriate payload constructor based on the supplied
	ip_protocol.
	"""
	if ip_protocol == 17 :
		from pcapfile.protocols.transport.udp import UDP
		return (UDP, 'UDP')
	#elif ip_protocol == 6:
	#	from pcapfile.protocols.transport.tcp import TCP
	#	return (TCP, 'TCP')
	#elif ip_protocol == 1:
	#	from pcapfile.protocols.transport.tcp import ICMP
	#	return (ICMP, 'ICMP')
	#elif ip_protocol == 41:
	#	from pcapfile.protocols.transport.tcp import IPv6
	#	return (IPv6, 'IPv6')
	#elif ip_protocol == 41:
	#	from pcapfile.protocols.transport.tcp import IPv6
	#	return (IPv6, 'IPv6')
	else:
		return (None, 'unknown')

#def __call__(packet):
#	return IP(packet)
