#!/usr/local/bin/python
# vim: expandtab ts=4 ai

import os
import multiprocessing as mp
from time import time,sleep
from config import *
from ut import *

#ts = int(time())
ts = 1407332608

from multiprocessing import Pool, Queue


errorQ = Queue()
def pbunzip2(x):
    pout(">>>" + x)
    rc = os.system("/usr/bin/bunzip2 %s" % x)
    if rc != 0:
        perr("ERROR: Could not unzip file: %s, got return code %s" % (x,rc))
        #failedunzip.append(x)
        errorQ.put(x)

def bunzip2dir(directory):
    ''' does what it says on the tin. Given a dir it walks it and bunzips it all in parallel '''
    files = os.popen("find %s -type f -name \"*.bz2\"" % directory).readlines()
    files = map( lambda x: x.strip(), files)

    pout("We are batch bunzipping %d files" % len(files) )

    failedunzip = []

    p = mp.Pool(mp.cpu_count())
    p.map(pbunzip2, files)

    while errorQ.empty == False:
        failedunzip.append(errorQ.get())
    #serial write to log file (expensive, due to mass syscalls, but we are not caring here)
    if len(failedunzip) != 0:
        map(lambda x: perr(x,ERRLOG), failedunzip)
        return failedunzip
    else:
        return True #all OK

def pullPCAP(host,path, attempts=4):
    targetpath = "%s/%s/%s" % (scratchpath, host, ts)
    if os.path.exists(targetpath) == False:
        os.makedirs(targetpath, 0770) #mode 0770 because we need exec for folders to be accessed    

    while (attempts >= 0):
        rc = system("rsync","-z","--skip-compress=bz2","-c","-r","--progress","-t", "pcapdump@%s:%s/" % (host,path), targetpath)
        if rc != 0:
            attempts -= 1
            pout("Error with rsync, retrying (%d attempts left)" % attempts)
        else:
            pout("rsync success! Continuing...")
            return (True, targetpath)

    perr("ERROR: Failed rsync! from %s:%s to %s" % (host,path,targetpath),ERRLOG)
    return (False, None) #we failed to do the rsync

if __name__ == "__main__":
    failed_transfers = pflexidict()
    intray = sources 

    for row in intray:
        # 1. Get the data from the remote host
#        result, savedpath = pullPCAP(row[0],row[1])
        result = True
        savedpath = "/storage/scratchdisk/lcmfrs23/1407332608/"

        if result == False: 
            perr("ERROR: We failed rsync transfer! >> %s:%s" % (row[0],row[1]) )
            row.append("ERROR: rsync failed")
            failed_transfers[row[0]] = row
            continue

        # 2. Make sure we have the two folders there that we need.
        if len(filter(lambda x: not os.path.exists(os.path.join(savedpath,x)), target_pcap_folders )) != 0:
            perr("ERROR: EMDI and/or ETI folders are missing from '%s'" % savedpath,ERRLOG)
            row.append("ERROR: EMDI/ETI folders are missing from '%s'" % savedpath)
            failed_transfers[row[0]] = row
            continue

        #3. All folders are there. bunzip them all!
        failures = bunzip2dir(savedpath)
        if len(failures) > 0:
            map(lambda x: perr(x,ERRLOG), failures)
            perr("ERROR: Could not bunzip all files. See log for more errors. ",ERRLOG)
            row.append("ERROR: Could not bunzip all files. See log for more errors. ", savedpath )
            failed_transfers[row[0]] = row
            continue





