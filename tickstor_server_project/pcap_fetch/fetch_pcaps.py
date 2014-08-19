#!/usr/local/bin/python
# vim: expandtab ts=4 ai

import os
import multiprocessing as mp
from time import time,sleep
from config import *
from ut import *

#ts = int(time())
ts = 1407332608  #for testing

# Exit codes
# 1 = invalid ziptype when zipping or unzipping

class massZip:
    def __init__(self,logginginstance):
        self.errorQ = mp.Queue()
        self.out = logginginstance
    
    def bz(self,x,cmd):
        self.out.pout(">>>" + x)
        rc = os.system("/usr/bin/bunzip2 %s" % x)
        if rc != 0:
            self.out.perr("Could not unzip file: %s, got return code %s" % (x,rc))
            self.errorQ.put(x)

    def execute(self,directory,ziptype):
        ''' does what it says on the tin. Given a dir it walks it and bunzips it all in parallel '''
        if ziptype == "bunzip":
            files = os.popen("find %s -type f -name \"*.bz2\"" % directory).readlines()
            self.out.pout("We are batch bunzipping %d files" % len(files) )
            cmd = "/usr/bin/bunzip2" 
        elif ziptype == "bzip":
            files = os.popen("find %s -type f -not -name \"*.bz2\"" % path).readlines()
            self.out.pout("We are batch bunzipping %d files" % len(files))
            cmd = "/usr/bin/bzip2"
        else:
            self.out.perr("Sorry, type '%s' unrecognized, failing" % ziptype)
            sys.exit(1)

        files = map( lambda x: x.strip(), files)

        failures = []
        running_procs = []

        while True:
            if len(files) == 0: break
            while (len(running_procs ) < mp.cpu_count() ):
                p = mp.Process(target=self.bz, args=(files.pop(),cmd)) 
                p.start()
                running_procs.append(p)

            sleep(0.5)
			print "Wait"
            running_procs = filter(lambda x: x.is_alive() == True, running_procs) #cleanup
            if len(running_procs) == 0: break


        os.wait()
       # p = mp.Pool(mp.cpu_count())
        #p.map(self.pbunzip2, files)

        while self.errorQ.empty == False:
            failures.append(self.errorQ.get())
        #serial write to log file (expensive, due to mass syscalls, but we are not caring here)
        if len(failures) != 0:
            map(lambda x: self.out.perr(x), failures)
        return failures

    def bunzipdir(self,x):
        return self.execute(x,"bunzip")

    def bzipdir(self,directory):
		return self.execute(x,"bzip")

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
#        result, savedpath = pullPCAP(row[0],row[1])
        result = True
        savedpath = "/storage/scratchdisk/lcmfrs23/1407332608/"

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
            map(lambda x: perr(x), failures)
            out.perr("ERROR: Could not bunzip all files. See log for more errors. ")
            row.append("ERROR: Could not bunzip all files. See log for more errors. ", savedpath )
            failed_transfers[row[0]] = row
            continue



    out.pout("Error summary:")
    for key,value in failed_transfers:
        out.pout("\t >> %s -> %s" % (key,value) )


