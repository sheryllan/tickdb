#!/usr/bin/python
# vim: ts=4 expandtab

import os,sys
from multiprocessing import Pool,cpu_count

try:
    path = sys.argv[1]
except IndexError:
    print "ERROR! Please give root dir from which to start walking as first argument"
    sys.exit(-1)


files = os.popen("find %s -type f -not -name \"*.bz2\"" % path).readlines()
files = map( lambda x: x.strip(), files)

print "We are batch bunzipping %d files" % len(files)

def prun(x):
    print ">>>" + x
    rc = os.system("/usr/bin/bzip2 %s" % x)
    if rc == 256:
        print "Removing '%s'" % x
        os.unlink(x)

p = Pool(cpu_count() + (cpu_count() / 4))
p.map(prun, files)

