#!/usr/local/bin/python
# vim: expandtab ts=4 ai

import os
from time import time,sleep
from config import *
from ut import *

ts = int(time())
#ts = 1407332608  #for testing

def pullPCAP(host,path, attempts=4):
    targetpath = "%s/%s/%s" % (scratchpath, host, ts)
    if os.path.exists(targetpath) == False:
        os.makedirs(targetpath, 0770) #mode 0770 because we need exec for folders to be accessed    

    while (attempts >= 0):
        rc = system("rsync","-z","--skip-compress=bz2","-u","-r","--progress","-t", "pcapdump@%s:%s/" % (host,path), targetpath)
        if rc != 0:
            out.pout("Error with rsync, retrying (%d attempts left)" % attempts)
            attempts -= 1
        else:
            out.pout("rsync success! Continuing...")
            return (True, targetpath)

    out.perr("ERROR: Failed rsync! from %s:%s to %s" % (host,path,targetpath))
    return (False, None) #we failed to do the rsync

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


