#!/usr/bin/python
# vim: ts=4 expandtab 
import shutil, sys, re
import multiprocessing as mp
from config import *
from ut import *
from time import time, sleep
from hashlib import md5
from Queue import Empty as Qerr
import shutil

class pcapMerge:
    def __init__(self,parallel=False):
        self.out = output(LOGFILE)
        self.multi = bool(parallel) #Make sure they passed a bool, not string

    def mergePCAPs(self,outfile,pcaplist):
        ''' Take a list of pcap files to merge (full paths) that we want to merge '''
        #If we are processing PCAPs again, we need to delete the old target (if it exists), and recreate
        if len(pcaplist) < 3: self.multi = False
        if len(pcaplist) == 1 : 
            shutil.copyfile(pcaplist.pop(), outfile) #We only have one file, no need to merge
            return 0

        rc = -1
        if os.path.exists( os.path.dirname(outfile) ) == True:  shutil.rmtree( os.path.dirname(outfile) )
        os.makedirs( os.path.dirname(outfile) )
        pcaplist = map( lambda x: x.strip(), pcaplist)
        tmplist = []
        if self.multi == False:
            # We are sticking to single process merging
            # tcpslice can handle large pcap files, so we don't need to split it and do it one by one like before. 
            rc = system(mergecap_path+"/tcpslice","-w",outfile,*pcaplist)
            if (rc != 0):
                os.unlink(outfile)
                map(lambda x: os.unlink(x), pcaplist)
                self.out.perr("Error writing pcap file. Aborting. Outfile and temp files cleared")
                sys.exit(rc)
            return rc    
        else:
            def execbot(workfile,infiles):
                tries = 5
                while (0 != system(mergecap_path+"/tcpslice","-w",workfile, *infiles ) ):
                    sleep(30) #wait 30 seconds to see if the file is ready yet. a
                    print "Retrying %s. %d attempts left." % (workfile, tries)
                    tries -= 1
                    if tries == 0: break
            
            # Because the end result is a single file, this is an all or nothing action. So no failure handling.
            running_procs = []
            workfile = None #Tell python that this var is one level up, scope wise.
            
            cpus = mp.cpu_count()
            idx = 0 # Index pointing to the start of the window
            while len(pcaplist) != 0:
                sleep(0.5)
                if (len(pcaplist) <= 2 ) or (idx == -1):
                    print "Final set being done in Series. Waiting for all procs to finish"
                    for proc in running_procs: 
                        proc[0].join() #Wait for all processes to finish
                        if os.path.exists(outfile) == True:
                            shutil.move(outfile,outfile+".previous")
                        else:
                            shutil.copy(proc[1], outfile+".previous")
                        print "Merging..."    
                        rc = system(mergecap_path+"/tcpslice","-w",outfile,outfile+".previous",proc[1])
                        if rc == 0:
                            os.unlink(outfile+".previous")
                        else:
                            self.out.perr("Error merging! Could not merge: %s and %s to %s" % (outfile+".previous",proc[1],outfile))
                            sys.exit(rc)

                    #We have less files then CPU's, so finish off in series
#                    print "Final merge to outfile commencing"
                    #The last $cups have not been merged, as we are waiting for them to finish. Merge them all here.
 #                   rc = system(mergecap_path+"/tcpslice","-w",outfile,*pcaplist[(idx-cpus):])
                    print "Done!"
                    return rc
               
                for proc,workfile in running_procs:
                    if proc.is_alive() == False:
                        pcaplist.append(workfile) #only append workfile if the processes has finished successfully and written out file.

                running_procs = filter(lambda x: x[0].is_alive() == True, running_procs)

                while (len(running_procs ) < (cpus) ):
                    print "%d -> %d" % ( len(pcaplist) , idx )
                    partA = pcaplist[idx]
                    idx += 1
                    try:
                        partB = pcaplist[idx]
                    except IndexError as e:
                        idx = -1
                        break #We're at the end
                    idx += 1
                    workfile = "%s.pcap" % os.path.join(TMPFOL,"tmpfile_"+md5(''.join(partA+partB)).hexdigest())
                    print "%40s + %-40s -> %30s" % (os.path.basename(partA), os.path.basename(partB), os.path.basename(workfile) )
                    p = mp.Process(target=execbot, args=(workfile, [partA, partB]) )
                    p.start()
                    running_procs.append([p,workfile])


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
    print "Starting auto-testing system"
    sclock = time()
    pm = pcapMerge(False)
    pm.merge_unprocessed_pcaps()
    print "Done. Execution took %2f seconds" % ( time() - sclock )

    sys.exit(0)
