#!/bin/sh
echo "Type in the ssh password for the server below. then go to 
http://localhost:8000 
for the eternus web UI
=========================

"
 ssh -NL 8000:192.168.1.1:80 system@192.168.140.89
