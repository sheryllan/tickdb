Usage - 

"python3 tcpdumpfilter_for_cme.py -x ./config.xml.20181122 -i ./20181119/cme-a_20181119-09\:00\:00.pcap -o ./output/test/ -c "310" -p 4"

1. -x ./config.xml.20181122                       -- read cme channel configurations from this xml file
2. -i ./20181119/cme-a_20181119-09\:00\:00.pcap   -- the input pcap file
3. -o ./output/test/                              -- output directory, the output file name will be "filtered_310_cme-a_cme-a_20181119-09:00:00.pcap"
4. -c "310"                                       -- which channel/channels we want to get its/their data
5. -p 4                                           -- will run 4 tcpdump instance at the same time



"python3 tcpdumpfilter_for_cme.py -x ./config.xml.20181122 -i ./20181119/cme-a_20181119-09\:00\:00.pcap -o ./output/test/ -c "310, 311" -p 4"

1. -c "310, 311"                                   -- we would like to have date from both channel 310 and 311


"python3 tcpdumpfilter_for_cme.py -x ./config.xml.20181122 -i /mnt/tank/var/reactor/p2d/captures/cme-a/20181119 -o ./output/test/ -c "310" -p 16"

1. -i /mnt/tank/var/reactor/p2d/captures/cme-a/20181119   -- the input is a directory, will process all pcap files in that directory
2. -p 16                                                  -- will run 16 tcpdump instance at the same time



Actual tcpdump command line -

tcpdump -r /mnt/tank/var/reactor/p2d/captures/cme-a/20181119/cme-a_20181119-18:00:00.pcap 
        -w ./output/test/filtered_310-311_cme-a_20181119-18:00:00.pcap 
        '(host 205.209.218.10 and port 10000) or (dst host 224.0.31.1 and dst port 14310) or (dst host 224.0.32.1 and dst port 15310) or 
        (dst host 224.0.31.43 and dst port 14310) or (dst host 224.0.32.43 and dst port 15310) or 
        (dst host 224.0.31.22 and dst port 14310) or (dst host 224.0.32.22 and dst port 15310) or 
        (dst host 233.72.75.1 and dst port 23310) or (dst host 233.72.75.64 and dst port 22310) or
        (host 205.209.218.10 and port 10000) or (dst host 224.0.31.2 and dst port 14311) or 
        (dst host 224.0.32.2 and dst port 15311) or (dst host 224.0.31.44 and dst port 14311) or 
        (dst host 224.0.32.44 and dst port 15311) or (dst host 224.0.31.23 and dst port 14311) or 
        (dst host 224.0.32.23 and dst port 15311) or (dst host 233.72.75.2 and dst port 23311) or 
        (dst host 233.72.75.65 and dst port 22311)'
        
        
Slice pcap if it is too big - editcap, https://www.wireshark.org/docs/man-pages/editcap.html

editcap -F libpcap -A "2018-11-19 15:00:00" -B "2018-11-19 15:01:00" ./filtered_311_cme-a_20181119-09\:00\:00.pcap ./test.pcap

1. timestamp in pcap file name is the local machine time where we capture the packets
2. we have to convert it to our local time if we run editcap in a different time-zone
3. this is why we have "2018-11-19 15:00:00" cme CST VS GMT



