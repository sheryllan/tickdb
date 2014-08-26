# The config file, but can actually have python logic in it. Very powerful, but easy to get carried away
# Don't abuse it! 
import os

sources = [

["lcmfrs23","/var/reactor/pcaps"],

]

target_pcap_folders = ["EMDI","ETI"]

LOGFILE="/home/pcapdump/pcap_dump.log"

try: TMPFOL=os.environ['MPCAPTMP']
except KeyError:TMPFOL="/storage/scratchdisk/pcapmerge_tmp"
#Deliberatly set to fail if we are creating recursive paths (in case of misset variable/config
if not os.path.exists(TMPFOL): os.mkdir(TMPFOL) 

scratchpath="/storage/scratchdisk"
raw_pcap_path="/storage/P2D/raw_pcaps/"
merged_pcap_path="/storage/P2D/merged_pcaps/"
datamolepath="/storage/P2D_datamole"

mergecap_path="/usr/local/bin/"
