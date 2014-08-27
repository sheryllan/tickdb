#!/usr/bin/python
# vim: ts=4 expandtab 
import shutil, sys, re
import multiprocessing as mp
from config import *
from ut import *
from time import time, sleep
from hashlib import md5
from Queue import Empty as Qerr

class pcapMerge:
    def __init__(self,parallel=False):
        self.out = output(LOGFILE)
        self.multi = bool(parallel) #Make sure they passed a bool, not string

    def mergePCAPs(self,outfile,pcaplist):
        ''' Take a list of pcap files to merge (full paths) that we want to merge '''
        #If we are processing PCAPs again, we need to delete the old target (if it exists), and recreate
        rc = -1
        if os.path.exists( os.path.dirname(outfile) ) == True:  shutil.rmtree( os.path.dirname(outfile) )
        os.makedirs( os.path.dirname(outfile) )
        pcaplist = map( lambda x: x.strip(), pcaplist)
        if self.multi == False:
            # We are sticking to single process merging
            workfile = None
            while len(pcaplist) > 1:
                partA = pcaplist.pop(0) #pop from top of list (so we don't pop workfile immediately
                partB = pcaplist.pop(0)
                workfile = "%s.pcap" % os.path.join(TMPFOL,md5(partA+partB).hexdigest())
                print "%40s + %-40s -> %30s" % (os.path.basename(partA), os.path.basename(partB), os.path.basename(workfile) )

                try:
                    rc = system(mergecap_path+"/mergecap","-w",workfile,partA,partB)
                    pcaplist.append(workfile)
                except OSError as e:
                    print "Error calling mergecap. We tried the following: \n %s" % "%s/mergecap -w %s %s" % (mergecap_path,outfile,arguments)
                    raise(e)
                
            #For the last entry, merge it into outfile
            rc = system(mergecap_path+"/mergecap","-w",outfile, workfile, pcaplist.pop())


        else:
            inQ = mp.Queue()
            map(lambda x: inQ.put(x), pcaplist)
            # Because the end result is a single file, this is an all or nothing action. So no failure handling.
            running_procs = []
            workfile = None #Tell python that this var is one level up, scope wise.
            while inQ.empty() == False:
                while (len(running_procs ) < mp.cpu_count() ):
                    try: partA = inQ.get() #If we have empty Queue here, we are done (should not hit this in normal operation)
                    except Qempty: break # So break out of loop, and the inQ.empty loop should break on next evaluation
                    try: partB = inQ.get()
                    except Qempty:
                        finalfile = partA
                        break
                        # If we get here, then there was only one element done in the Queue. So we write the final pcap
                        #Wait for any still running processes to finisha
#                        for item in running_procs:
#                            print "Waiting for process: %s to finish." % dir(item[0].ident)
#                            item[0].join()

#                        rc = system(mergecap_path+"/mergecap","-w",outfile, workfile, partA )
#                        break
#                    else:
                     
                    workfile = "%s.pcap" % os.path.join(TMPFOL,md5(partA+partB).hexdigest())
                    print "%40s + %-40s -> %30s" % (os.path.basename(partA), os.path.basename(partB), os.path.basename(workfile) )
                    p = mp.Process(target=system, args=(mergecap_path+"/mergecap","-w",workfile, partA, partB ))
                    p.start()
                    running_procs.append([p, workfile])

                    for item in running_procs:       
                        if len(running_procs) == 0: break
                        # Only push the workfile to Queue if proc is finished (prevent race conditions)
                        if item[0].is_alive() == False:
                            inQ.put(workfile)
                            running_procs.pop(running_procs.index(item))
                running_procs = filter(lambda x: x[0].is_alive() == True, running_procs) 
                if len(running_procs) == 0: break
                sleep(0.5)
#        while len(running_procs) != 0:
#            running_procs = filter(lambda x: x[0].is_alive() == True, running_procs)
#            print running_procs
 #           sleep(0.5)
            for item in running_procs: 
                print "Waiting for process: %s to finish." % dir(item[0].ident)
                item[0].join() # wait for processes
#            os.wait()
            #Now that we are all done, the final workfile should be moved to the right spot
            sync()
            rc =  system(mergecap_path+"/mergecap","-w",outfile, workfile, finalfile)


        if rc != 0:
            self.out.perr("ERROR merging pcap files. We got exit code %d " % rc)
        else:
            self.out.pout("PCAP merged")

        return rc


    def merge_unprocessed_pcaps(self):
        # 1. Get a list of all the folders, and see if there is already an equivalent
        #    in the processed_pcaps folder

        uf = [] #unprocessed_folders 
        for pcaphost in fetchrawPCAPhosts():
            for dayslot in fetchPCAPdaySlots(pcaphost):
                currentslot = os.path.join(raw_pcap_path,pcaphost,dayslot)
                mergedslot  = os.path.join(merged_pcap_path,pcaphost,dayslot)

                # See if we have the A/B folders in EMDI/ETI in processed_pcaps. If not, add to processing list
                if not os.path.exists("%s/EMDI/A.pcap" % mergedslot): uf.append(["%s/EMDI/A/" % currentslot, "%s/EMDI/A.pcap" % mergedslot, pcaphost, dayslot])
                if not os.path.exists("%s/EMDI/B.pcap" % mergedslot): uf.append(["%s/EMDI/B/" % currentslot, "%s/EMDI/B.pcap" % mergedslot, pcaphost, dayslot])
                if not os.path.exists("%s/ETI/A.pcap" %  mergedslot): uf.append(["%s/ETI/A/"  % currentslot,  "%s/ETI/A.pcap" % mergedslot, pcaphost, dayslot])
                if not os.path.exists("%s/ETI/B.pcap" %  mergedslot): uf.append(["%s/ETI/B/"  % currentslot,  "%s/ETI/B.pcap" % mergedslot, pcaphost, dayslot])


        for item in uf:
            # 1. get list of all pcap files in folder. Following pattern as setup by Kieran ( $filename.pcap([0-9]*) )
            files = os.listdir(item[0])
            files = filter( lambda x: re.match("(EMDI.*|ETI)\.pcap[0-9]{0,9}$", x) != None, files)
            files = map(lambda x: os.path.join(item[0], x), files)
            if len(files) == 0:
                self.out.pwarn("We have no PCAP files in %s" % item[0])
                continue
            rc = self.mergePCAPs(item[1],files)
            if rc != 0:
                self.out.perr("Failed PCAP merge, exiting.")
                self.out.perr("Could not merge %s" % item[1])

if __name__ == "__main__":
    clock = time()
    pm = pcapMerge(False)
    pm.merge_unprocessed_pcaps()
    print "Done. Execution took %.2f seconds" % ( time() - clock )
    sys.exit(0)
