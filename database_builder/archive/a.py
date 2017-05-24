#!/usr/bin/env python3
import time
import line2msg

def line2msg1(string):
	l = string[:-1].split(':')
	data = {}

	for i in range(0,len(l),2):
		key = l[i]
		if key in data:
			data[key].append(l[i+1])
		else:
			data[key] = [l[i+1]]

	d1 = {k+'.'+str(i):data[k][i] for k in data if len(data[k])>1 for i in range(len(data[k]))}
	d1.update({k:data[k][0] for k in data if len(data[k])==1})
	return d1

def line2msg2(string):
	l = string[:-1].split(':')
	data = {}

	for i in range(0,len(l),2):
		key = l[i]
		if key in data:
			data[key].append(l[i+1])
		else:
			data[key] = [l[i+1]]

	d1 = {k+'.'+str(i):data[k][i] for k in list(data.keys()) if len(data[k])>1 for i in range(len(data[k]))}
	d1.update({k:data[k][0] for k in list(data.keys()) if len(data[k])==1})
	return d1

if __name__=="__main__":
	f=open('toto').readlines()[0]

#	t0=time.time()
#	for i in range(100000):
#		l = line2msg1(f)
#	t1=time.time()
#	print(t1-t0)

#	t0=time.time()
#	for i in range(100000):
#		l = line2msg2(f)
#	t1=time.time()
#	print(t1-t0)

	t0=time.time()
	for i in range(100000):
		l = line2msg.line2msg(f)
	t1=time.time()
	print(t1-t0)
