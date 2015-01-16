#!/usr/local/bin/python
# vim: expandtab ts=4 ai

from ut import massZip,output
import sys

try:
    ziptype= sys.argv[1]
except IndexError:
    print "ERROR! Please give command (bzip or bunzip) as first argument"
    sys.exit(-1)


try:
    path = sys.argv[2]
except IndexError:
    print "ERROR! Please give root dir from which to start walking as first argument"
    sys.exit(-1)


mZ = massZip(output())

if ziptype == "bunzip":
	print mZ.bunzipdir(path, overwrite="yes")
elif ziptype == "bzip2":
	print mz.bzipdir(path, overwrite="yes")
else:
	print "I'm sorry, ziptype %s is not recognised. Cannot continue" % ziptype
	sys.exit(-1)



sys.exit(0)


