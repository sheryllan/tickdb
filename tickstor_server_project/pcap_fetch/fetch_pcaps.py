#!/usr/local/bin/python
# vim: expandtab ts=4 ai

import os
from time import time,sleep
from config import *
from ut import *

<<<<<<< HEAD
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
=======
ts = int(time())
#ts = 1407332608  #for testing
>>>>>>> f3910ab97f8d8268f70a5c5764fbdc942af1c909

def pullPCAP(host,path, attempts=4):
    targetpath = "%s/%s/%s" % (scratchpath, host, ts)
    if os.path.exists(targetpath) == False:
        os.makedirs(targetpath, 0770) #mode 0770 because we need exec for folders to be accessed    

    while (attempts >= 0):
        rc = system("rsync","-z","--skip-compress=bz2","-c","-r","--progress","-t", "pcapdump@%s:%s/" % (host,path), targetpath)
        if rc != 0:
            attempts -= 1
            out.pout("Error with rsync, retrying (%d attempts left)" % attempts)
        else:
            out.pout("rsync success! Continuing...")
            return (True, targetpath)

    out.perr("ERROR: Failed rsync! from %s:%s to %s" % (host,path,targetpath))
    return (False, None) #we failed to do the rsync

<<<<<<< HEAD
=======
if __name__ == "__main__":

    def cleanup_after_fail():
        ''' If we have a failure of the program, or it is killed, this cleans up target CSV output dirs, etc...
            so that we can attempt it again. Prevents leaving partially complete folders about '''
        cleanup() #We do what the standard cleanup does

        #And then some more
        # cleanup the scratchdisk path
    #   os.system("rm 

    def cleanup():
        ''' cleans up the tmpdir etc... after finish/abort '''
        out.pout("Commencing cleanup.")
        
        return False

    
    failed_transfers = pflexidict()
    intray = sources 

    out = output(LOGFILE)
    mZ = massZip(out)

    for row in intray:
        # 1. Get the data from the remote host
        result, savedpath = pullPCAP(row[0],row[1])
#        result = True
#        savedpath = "/storage/scratchdisk/lcmfrs23/1407332608/"

        if result == False: 
            out.perr("ERROR: We failed rsync transfer! >> %s:%s" % (row[0],row[1]) )
            row.append("ERROR: rsync failed")
            failed_transfers[row[0]] = row
            continue

        # 2. Make sure we have the two folders there that we need.
        if len(filter(lambda x: not os.path.exists(os.path.join(savedpath,x)), target_pcap_folders )) != 0:
            out.perr("ERROR: EMDI and/or ETI folders are missing from '%s'" % savedpath)
            row.append("ERROR: EMDI/ETI folders are missing from '%s'" % savedpath)
            failed_transfers[row[0]] = row
            continue

        #3. All folders are there. bunzip them all!
        failures = mZ.bunzipdir(savedpath)
        if len(failures) > 0:
            map(lambda x: out.perr(x), failures)
            out.perr("ERROR: Could not bunzip all files. See log for more errors. ")
            row.append("ERROR: Could not bunzip all files. See log for more errors. ")
            failed_transfers[row[0]] = row

	if len(failed_transfers) != 0:
	    out.pout("Error summary:")
	    for key in failed_transfers:
	        out.pout("\t >> %s -> %s" % (key,failed_transfers[key]) )
	else:
		out.pout("No errors reported.")

>>>>>>> f3910ab97f8d8268f70a5c5764fbdc942af1c909

