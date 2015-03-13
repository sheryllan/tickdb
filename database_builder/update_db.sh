#!/bin/bash

rootdir=/mnt/data
tmpdir=/tmp
level=5

destdir=${rootdir}/level${level}data
dbfile=${rootdir}/obp2.db
tmpfile=${tmpdir}/`date +%s%N``echo -n ${RANDOM}`_obp2.dat
dirlist="BSXEURMA1 BSBUSKMA1 BSXCBTMA1 BSXCMEMA1 BSXNYMMA1"
gnupar=`which parallel`

# test if dbfile exists
if [ ! -f $dbfile ]; then
	touch $dbfile
fi

# remove tmpfile
rm -f $tmpfile

# find new files to process
find $dirlist -type f | sort - $dbfile | uniq -u | sort -t '/' +1 -2 -n -r > $tmpfile

# run parallel update
$gnupar $rootdir/obp2.py -l $level -o $destdir :::: $tmpfile

# update db
if [ $? -eq 0 ]; then
	cat $tmpfile >> $dbfile
	rm -f $tmpfile
else
	echo "Error while processing files"
fi
