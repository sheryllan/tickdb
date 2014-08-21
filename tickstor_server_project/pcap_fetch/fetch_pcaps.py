#!/usr/local/bin/python
# vim: expandtab ts=4 ai

import os
from time import time,sleep
from config import *
from ut import *

ts = int(time())
#ts = 1407332608  #for testing

def pullPCAP(host,path, attempts=4):
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

    createdPaths=[] #a record of everything we have created on disk, in case we have to roll back

    import shutil, signal
    def cleanup_incomplete(signum, frame):
        ''' If we have a failure of the program, or it is killed, this cleans up target CSV output dirs, etc...
            so that we can attempt it again. Prevents leaving partially complete folders about '''
        out.pwarn("Signal %d caught. Processing incomplete. Cleaning up")
        cleanup() #We do what the standard cleanup does

        #Plus we cleanout the P2P output folder, because whatever in there will
        # not be completely valid, due to us not finishing processing
        if os.path.exists(outpath) == True:  shutil.rmtree(outpath)
        

    def cleanup():
        ''' cleans up the tmpdir etc... after finish/abort '''
        out.pout("Commencing cleanup.")
        #remove temporary saved path
        if os.path.exists(savedpath) == True:  shutil.rmtree(savedpath)
        
        return False

    # Signal me, baby
    signal.signal(signal.SIGTERM, cleanup_incomplete)
    signal.signal(signal.SIGINT, cleanup_incomplete)



    failed_transfers = pflexidict()
    intray = sources 

    out = output(LOGFILE)
    mZ = massZip(out)

    for row in intray:
        targetpath = "%s/%s/%s" % (scratchpath, row[0], ts)
        if os.path.exists(targetpath) == False:
            os.makedirs(targetpath, 0770) #mode 0770 because we need exec for folders to be accessed    
            createdPaths.append(targetpath)


        # 1. Get the data from the remote host
        result, savedpath = pullPCAP(targetpath,row[1])

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

        # So, we managed to bunzip all the pcaps. Now what is left is to move them to the long term storage area.a
        outpath = os.path.join(raw_pcap_path,row[0],str(ts))
        out.pout("Output to %s. Creating path if non existant." % outpath)
        if os.path.exists(outpath) == False: os.makedirs(outpath)
        rc = os.system("mv -v %s %s" % (savedpath,outpath))
        if rc == 0: 
            out.pout("Folder moved successfully.")
            createdPaths.append(outpath)
        else:
            out.perr("Error moving folder. Got return code: %d" % rc)
            # According to POSIX, return codes are 8 bit. Python and GNU/Linux seems to use 16-bit return codes.
            # Therefore,  a return code which is modulo 256 will cause overflow,
            # and a return code of 0 is passed back to OS, even if it was a failure. The below
            # max() makes sure that any error > 256 is returned as 255. We lose some error info, but better than
            # returning 0 inadvertantly.
            rc = max(rc,255) 
            sys.exit(rc) # this failure is a showstopper really. If we can't write to target dir, then something is
                         # seriously wrong.
            
 
  	if len(failed_transfers) != 0:
   	    out.pout("Error summary:")
   	    for key in failed_transfers:
   	        out.pout("\t >> %s -> %s" % (key,failed_transfers[key]) )
        sys.exit(2)
   	else:
   		out.pout("No errors reported.")
        sys.exit(0)




