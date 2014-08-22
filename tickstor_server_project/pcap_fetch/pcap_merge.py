#!/usr/bin/python
# vim: ts=4 expandtab 
import shutil, sys, re
import multiprocessing as mp
from config import *
from ut import *

class pcapMerge:
    def __init__(self,parallel=False):
        self.out = output(LOGFILE)
        self.multi = parallel

    def mergePCAPs(self,outfile,pcaplist):
        ''' Take a list of pcap files to merge (full paths) that we want to merge '''
        if os.path.exists( os.path.dirname(outfile) ) == False: os.makedirs( os.path.dirname(outfile) )
        if self.multi == False:
            # We are sticking to single process merging
            try:
                rc = system(mergecap_path+"/mergecap","-w",outfile,*pcaplist)
            except OSError as e:
                print "Error calling mergecap. We tried the following: \n %s" % "%s/mergecap -w %s %s" % (mergecap_path,outfile,arguments)
                raise(e)
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
            rc = self.mergePCAPs(item[1],files)
            if rc != 0:
                self.out.perr("Failed PCAP merge, exiting.")
                sys.exit(2) #Exit for now, perhaps in future we just log it, and continue

if __name__ == "__main__":
    pm = pcapMerge(False)
    pm.merge_unprocessed_pcaps()
