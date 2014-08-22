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
        if self.multi == False:
            # We are sticking to single process merging
            arguments = ' '.join(pcaplist)
            rc = system("%s/mergecap -w %s %s" % (outfile,mergecap_path,arguments))
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
                if not os.path.exists("%s/EMDI/A.pcap" % mergedslot): uf.append(["%s/EMDI/A/" % currentslot, pcaphost, dayslot])
                if not os.path.exists("%s/EMDI/B.pcap" % mergedslot): uf.append(["%s/EMDI/B/" % currentslot, pcaphost, dayslot])
                if not os.path.exists("%s/ETI/A.pcap" % mergedslot): uf.append(["%s/ETI/A/" % currentslot, pcaphost, dayslot])
                if not os.path.exists("%s/ETI/B.pcap" % mergedslot): uf.append(["%s/ETI/B/" % currentslot, pcaphost, dayslot])


        
        for item in uf:
            # 1. get list of all pcap files in folder. Following pattern as setup by Kieran ( $filename.pcap([0-9]*) )
            files = os.listdir(item[0])
            files = filter( lambda x: re.match("(EMDI.*|ETI)\.pcap[0-9]{0,9}", x) != None, files)
            print files

if __name__ == "__main__":
    pm = pcapMerge(False)
    pm.merge_unprocessed_pcaps()
