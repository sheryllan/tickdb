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
        rc = -1
        if os.path.exists( os.path.dirname(outfile) ) == True:  shutil.rmtree( os.path.dirname(outfile) )
        try:
            os.makedirs( os.path.dirname(outfile) )
        except OSError as e:
            out.perr("Could not make dir '%s'! Aborting and cleaning up." % os.path.dirname(outfile) )
            exit(-1)

        if len(pcaplist) == 1 : 
            shutil.copyfile(pcaplist.pop(), outfile) #We only have one file, no need to merge
            return 0


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
            return rc    
        else:
            def execbot(workfile,infiles):
                tries = 5
                while (0 != system(mergecap_path+"/tcpslice","-w",workfile, *infiles ) ):
                    sleep(30) #wait 30 seconds to see if the file is ready yet. a
                    print "Retrying %s. %d attempts left." % (workfile, tries)
                    tries -= 1
                    if tries == 0: 
                        self.out.perr("ERROR, could not merge PCAPS: %s -> %s" % (workfile,infiles))
                        return False
            
            # Because the end result is a single file, this is an all or nothing action. So no failure handling.
            running_procs = []
            workfile = None #Tell python that this var is one level up, scope wise.
            
            cpus = mp.cpu_count()
            idx = 0 # Index pointing to the start of the window
            while len(pcaplist) > 0:
                sleep(0.5)
                for proc,workfile in running_procs:
                    if proc.is_alive() == False:
                        pcaplist.append(workfile) #only append workfile if the processes has finished successfully and written out file.

                running_procs = filter(lambda x: x[0].is_alive() == True, running_procs)

                if len(pcaplist) == 1:
                    #We are done, only one file left. Move it to the outfile, now stack is == 0, and we will break
                    shutil.move(pcaplist.pop(), outfile)

                while (len(running_procs ) < (cpus) ):
                    print "%d -> %d" % ( len(pcaplist) , idx )
                    partA = pcaplist.pop(0)
                    try:
                        partB = pcaplist.pop(0)
                    except IndexError as e:
                        pcaplist.append(partA) #We can't process this, so push it back onto stack
                        break #We're at the end
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


#        for item in uf:
        def execbot(item):
            # 1. get list of all pcap files in folder. Following pattern as setup by Kieran ( $filename.pcap([0-9]*) )
            files = os.listdir(item[0])
            files = filter( lambda x: re.match("(EMDI.*|ETI)\.pcap[0-9]{0,9}$", x) != None, files)
            files = map(lambda x: os.path.join(item[0], x), files)
            files = filter( lambda x: x.endswith("bz2") == False, files) 
            if len(files) == 0:
                self.out.pwarn("We have no PCAP files in %s" % item[0])
                #continue
                return False
            rc = self.mergePCAPs(item[1],files)
            if rc != 0:
                self.out.perr("Failed PCAP merge, exiting.")
                self.out.perr("Could not merge %s" % item[1])
                return False
            return True

        rp = []
        while len(uf) != 0:
            sleep(0.5)
            print uf
            while (len(rp) < (mp.cpu_count()) ):
                try:
                    item = uf.pop()
                except IndexError as e:
                    break #We reacjed emd of list
                print "Spawining process to do pcap merge of %s" % item[0]
                p = mp.Process(target=execbot, args=([item]) )
                p.start()
                rp.append([p, item])

            rp = filter(lambda x: x[0].is_alive() == True, rp)

        for p in rp: p[0].join() #wait for processes to finish
if __name__ == "__main__":
    def cleanup():
        os.unlink("/tmp/pcapmerge.pid")

    if os.path.exists("/tmp/pcapmerge.pid") == True:
        fd = open("/tmp/pcapmerge.pid","r")
        pid = fd.readline()
        fd.close()
        print "ERROR! Lockfile present. Apparently we are already running as PID %d. If this is an error please delete /tmp/pcapmerge.pid and rerun" % int(pid)
        sys.exit(2)
    else:
        fd = open("/tmp/pcapmerge.pid","w")
        fd.write(str(os.getpid()) + "\n")
        fd.close()

    print "Beginning PCAP Merge"
    sclock = time()
    pm = pcapMerge(False)
    pm.merge_unprocessed_pcaps()
    print "Done. Execution took %2f seconds" % ( time() - sclock )

    cleanup()
    sys.exit(0)
