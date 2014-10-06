#!/usr/local/bin/python
# vim: expandtab ts=4 ai

import os,datetime
from time import time,sleep
from config import *
from ut import *
import subprocess as sp

ts = int(time())
hrts = datetime.datetime.fromtimestamp(ts).strftime("%d_%m_%Y") #Human readable timestamp, daily

#ts = 1407332608  #for testing

class pcaps:
    def __init__(self,logoutput):
        self.pcap_retry = 4
        self.logoutput = logoutput
        self.rsyncChild = -1
        signal.signal(signal.SIGTERM, self.cleanup_incomplete)
        signal.signal(signal.SIGINT, self.cleanup_incomplete)

    def cleanup_incomplete(self,signum,frame):
        self.logoutput.pwarn("PCAP Fetch interrupted by signal %d, Aborting!" % signum) 
        self.pcap_retry=-1
        if self.rsyncChild != -1:
            self.rsyncChild.send_signal(signal.SIGTERM)

    def pull(self,host,path,targetpath):
        if self.pcap_retry == -1: return (False, "ABORTED")
        else: attempts = self.pcap_retry
    
        while (attempts >= 0):
            sleep(0.5) #Wait for system signal to propagate before checking.  Otherwise you get a race condition
            if self.pcap_retry == -1: break #We've been aborted
            proc = sp.Popen(["rsync","-z","--skip-compress=bz2","-u","-r","--progress","-t", "pcapdump@%s:%s/" % (host,path), targetpath ] )
            self.rsyncChild = proc
            rc = proc.wait()  #wait returns the return code, while waiting until completion
            if rc != 0:
                self.logoutput.pout("Error with rsync, retrying (%d attempts left)" % attempts)
                attempts -= 1
            else:
                self.logoutput.pout("rsync success! Continuing...")
                return (True, targetpath)

        self.logoutput.perr("ERROR: Failed rsync! from %s:%s to %s" % (host,path,targetpath))
        return (False, None) #we failed to do the rsync

if __name__ == "__main__":

    import shutil, signal
    createdPaths=[] #a record of everything we have created on disk, in case we have to roll back
    failed_transfers = pflexidict()
    intray = sources 
    out = output(LOGFILE)
    mZ = massZip(out)
    pcap = pcaps(out)

    out.pout("Begin fetch pcaps: %s" % time())
    if os.path.exists("/tmp/fetchpcaps.pid") == True:
        fd = open("/tmp/fetchpcaps.pid","r")
        pid = fd.readline()
        fd.close()
        out.perr("ERROR!  Lockfile present. Apparently we are already running as PID %d. If this is an error please delete /tmp/fetchpcaps.pid and rerun" % int(pid))
        sys.exit(2)
    else:
        fd = open("/tmp/fetchpcaps.pid","w")
        fd.write(str(os.getpid()) + "\n")
        fd.close()

    def cleanup(rc):
        #remove temporary saved path
        try:
            os.unlink("/tmp/fetchpcaps.pid")
        except OSError as e:
            out.perr("Could not delete /tmp/fetchpcaps.pid. got error: %s" % e)
        else:
            out.pout("Lockfile removed. Goodbye!")
        sys.exit(rc)

    def cleanup_incomplete(signum, frame):
        ''' If we have a failure of the program, or it is killed, this cleans up children, target CSV output dirs, etc...
            so that we can attempt it again. Prevents leaving partially complete folders about '''
        out.pwarn("Signal %d caught. Processing incomplete. Cleaning up" % signum) 
        # Because this is an abort, we go to the other libraries, and call their abort/clenup functions as well
        pcap.cleanup_incomplete(signum,frame)
        mZ.cleanup_unfinished(signum,frame)

        #And then we delete the leftovers
        for path in createdPaths:
            out.pout("Removing self-created path: %s" % path)
            try:
                shutil.rmtree(path)
            except OSError as e:
                out.perr("Could not delete path '%s', got error: '%s'" % (path, e))
        
        cleanup(signum) #We do what the standard cleanup does


    # Signal me, baby
    signal.signal(signal.SIGTERM, cleanup_incomplete)
    signal.signal(signal.SIGINT, cleanup_incomplete)

    for row in intray:
        targetpath = "%s/%s/%s" % (scratchpath, row[0],  hrts  )
        createdPaths.append(targetpath)
        if os.path.exists(targetpath) == False:
            os.makedirs(targetpath, 0770) #mode 0770 because we need exec for folders to be accessed    
            createdPaths.append(targetpath)


        # 1. Get the data from the remote host
        result, savedpath = pcap.pull(row[0],row[1],targetpath)
        createdPaths.append(savedpath)

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
        failures = mZ.bunzipdir(savedpath,overwrite=True)
        if len(failures) > 0:
            map(lambda x: out.perr(x), failures)
            out.perr("ERROR: Could not bunzip all files. See log for more errors. ")
            row.append("ERROR: Could not bunzip all files. See log for more errors. ")
            failed_transfers[row[0]] = row

        # So, we managed to bunzip all the pcaps. Now what is left is to move them to the long term storage area.a
        outpath = os.path.join(raw_pcap_path,row[0], hrts )
        out.pout("Output to %s. Creating path if non existant." % outpath)
        if os.path.exists(outpath) == False: os.makedirs(outpath)
        createdPaths.append(outpath)
        rc = os.system("rsync -rvc %s/ %s/" % (savedpath,outpath))
        
        if rc == 0: 
            createdPaths.append(outpath)
            system("rm","-r","-f",savedpath) #We no longer need the savedpath, as we've copied the files off OK (zero rc)
            out.pout("Folder moved successfully.")
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
        cleanup(2)
    else:
        out.pout("No errors reported.")
        cleanup(0)




