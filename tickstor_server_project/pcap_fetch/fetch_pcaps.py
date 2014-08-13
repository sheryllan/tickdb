#!/usr/local/bin/python
# vim: expandtab ts=4 ai

import os
import multiprocessing as mp
from time import time,sleep
from config import *
from ut import *

#ts = int(time())
ts = 1407332608



def pbunzip2(x,errorQ):
    pout(">>>" + x)
    rc = os.system("/usr/bin/bunzip2 %s" % x)
    if rc != 0:
        errorQ.put(x)
        perr("ERROR: Could not unzip file: %s, got return code %s" % (x,rc))

def bunzip2dir(directory):
    ''' does what it says on the tin. Given a dir it walks it and bunzips it all in parallel '''
    files = os.popen("find %s -type f -name \"*.bz2\"" % directory).readlines()
    files = map( lambda x: x.strip(), files)

    pout("We are batch bunzipping %d files" % len(files) )

    failedunzip = []

    cpus= mp.cpu_count()
    errorQ = mp.Queue()
    procs = []
    while len(files) > 0:
        while (len(procs) < cpus):
            if len(files) == 0: break
            p = mp.Process(target=pbunzip2, args=(files.pop(), errorQ))
            p.start()
            procs.append(p)
        sleep(0.5)
        procs = filter(lambda x: x.is_alive() == True, procs)


    map(lambda x: x.join(), procs)

    while errorQ.empty() == False:
        failedunzip.append(errorQ.get())
    #serial write to log file (expensive, due to mass syscalls, but we are not caring here)
    if len(failedunzip) != 0:
        map(lambda x: perr(x,ERRLOG), failedunzip)
        return failedunzip
    else:
        return []#all OK

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


