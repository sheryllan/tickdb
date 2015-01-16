#!/usr/bin/python
# This is a library to ease access of ZFS from python
# vim: expandtab 

import os,sys
import subprocess as sp

def call(cmd, *args):
    # Execute command specified, and return stdout
    cmd = [cmd]
    cmd.extend(args)
    pipe = sp.Popen(  cmd ,  shell=False, bufsize=65535, stdout=sp.PIPE, close_fds=True)
    return pipe.stdout.readlines()


def getZFSstats(poolname):
    # We get all the metrics we care about here
    
    # 1. First get hold of zpool status
    zps = call("/sbin/zpool", "status", poolname)
    zps = filter(lambda x: x.find(':') != -1 , zps)
    zstat = {}
    for item in zps:
        item = item.split(':')
        key= item[0].strip()
        data= item[1].strip()
        zstat.update( { str(key): str(data) } )
        
    # Get compression status
    zps = call("/sbin/zfs", "get", "-H", "compressratio", poolname)
    if len(zps) > 1:
        # Something went wrong, we should not get more than one line for this command
        raise IOError("ERROR, got invalid input from '%s'" % ' '.join(["/sbin/zfs", "get", "-H", "compressratio", poolname]))
    zps =  zps[0].split('\t')
    zstat.update({zps[1]:zps[2]})

    #Get dedup status/ratio
    zps = call("/sbin/zfs", "get", "-H", "dedup", poolname)
    if len(zps) > 1:
        # Something went wrong, we should not get more than one line for this command
        raise IOError("ERROR, got invalid input from '%s'" % ' '.join(["/sbin/zfs", "get", "-H", "dedup", poolname]))
    zps =  zps[0].split('\t')
    if zps[2].strip() == "on":
        #dedup enabled, get current ratio
        zps2 = call("/sbin/zpool","list","-H",poolname)
        if len(zps2) > 1:  raise IOError("ERROR, got invalid input from '%s'" % ' '.join(["/sbin/zpool","list","-H",poolname]))
        zps2 = zps2[0].split('\t')
        dedup = zps2[5]
    else:
        dedup = -1
    
    zstat.update({zps[1]:[zps[2], dedup]})

    #More useful info
    zstat.update({ 'size':zps2[1], 'alloc':zps2[2], 'free':zps2[3], 'used%':zps2[4], 'health':zps2[6]})

    return zstat


if __name__ == "__main__":
    from pprint import pprint
    print "ZFS library selftest, getting status summary:"
    pprint(getZFSstats("storage"))
    print "All good, quitting..."
    sys.exit(0)
