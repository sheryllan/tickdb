#!/bin/env python3

import socket
import sys
import time

while True:
	try:
		s=socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		s.bind('\0symbol_per_day__lock')
		break
	except socket.error:
		time.sleep(60)

x={}

for line in sys.stdin:
	line=line.split()
	symbol=line[0]
	date=line[1]

	if date not in x:
		x[date]=list()
	x[date].append(symbol)

dates=sorted(x.keys())
for d in dates:
	print("{0},".format(d),end="")
	print(",".join(sorted(x[d])))

time.sleep(10)
