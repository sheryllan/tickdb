"""
UDP protocol definitions.
"""

import binascii
import ctypes
import struct


class UDP(ctypes.Structure):
	"""
	Represents an UDP packet.
	"""

	_fields_ = [('src_port', ctypes.c_ushort),      # Source port
			('dst_port', ctypes.c_ushort),  # Destination port
			('len', ctypes.c_ushort),	# Length
			('checksum', ctypes.c_ushort)]  # Checksum
	payload = None					# packet payload

	def __init__(self, packet, layers=0):
		# parse the required header first, compute the checksum after
		fields = struct.unpack("!HHHH",packet[:8])
		self.src_port = fields[0]
		self.dst_port = fields[1]
		self.len = fields[2]
		self.checksum = fields[3]

		self.payload = binascii.hexlify(packet[8:])

		if layers:
			self.load_application(layers)

	def load_application(self,layers=1):
		"""
		Given an UDP packet, determine the appropriate sub-protocol;
		If layers is greater than zero determine the type of the payload
		and load the appropriate type of network packet. It is expected
		that the payload be a hexified string. The layers argument determines
		how many layers to descend while parsing the packet.
		"""
		if layers:
			ctor = payload_type(self.payload)[0]
			if ctor:
				ctor = ctor
				payload = binascii.unhexlify(self.payload)
				self.payload = ctor(payload, layers - 1)
			else:
				# if no type is found, do not touch the packet.
				pass

	def __str__(self):
		datagram = 'udp datagram from {} to {} carrying {} bytes with checksum {}'
		datagram = datagram.format(self.src_port,self.dst_port, self.len, self.checksum)
		return datagram

def payload_type(udp_protocol):
	""" Returns the appropriate payload constructor based on the supplied
	udp_protocol payload
	"""

	#if ip_protocol == 17 :
	#	from pcapfile.protocols.transport.udp import UDP
	#	return (UDP, 'UDP')
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
	#else:
	return (None, 'unknown')

def __call__(packet):
	return UDP(packet)
