#!/usr/local/bin/python
# vim: ts=4 expandtab ai
import os,sys
import fetch_pcaps as fpc
from pprint import pprint
from ut import *
from config import *

if __name__ == "__main__":
    failed_transfers = pflexidict()
    intray = sources

    for row in intray:
        # 1. Get the data from the remote host
#        result, savedpath = fpc.pullPCAP(row[0],row[1])
        result = True
        savedpath = "/storage/scratchdisk/lcmfrs23/1407332608/"

        if result == False:
            perr("ERROR: We failed rsync transfer! >> %s:%s" % (row[0],row[1]) )
            row.append("ERROR: rsync failed")
            failed_transfers[row[0]] = row
            continue

        # 2. Make sure we have the two folders there that we need.
        if len(filter(lambda x: not os.path.exists(os.path.join(savedpath,x)), target_pcap_folders )) != 0:
            perr("ERROR: EMDI and/or ETI folders are missing from '%s'" % savedpath,ERRLOG)
            row.append("ERROR: EMDI/ETI folders are missing from '%s'" % savedpath)
            failed_transfers[row[0]] = row
            continue

        #3. All folders are there. bunzip them all!
        failures = fpc.bunzip2dir(savedpath)
        if len(failures) > 0:
            map(lambda x: perr(x,ERRLOG), failures)
            perr("ERROR: Could not bunzip files in %s. See log for more errors. " % savedpath,ERRLOG)
            map(lambda x: row.append("ERROR: Could not bunzip files in %s. File '%s' corrupt/notbzip? " % (savedpath,x) ), failures)
            failed_transfers[row[0]] = row
            continue


    pprint (failed_transfers)
