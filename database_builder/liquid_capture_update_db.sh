#!/bin/bash

if [[ $# -ne 1 ]]; then
	echo "Error: missing argument"
	echo "Usage: liquid_capture_update_db.sh <JSON config file>"
	exit 1
fi

# Test json conf file first
jsonconf=$1
if [ ! -e ${jsonconf} ]; then
	echo "Error: JSON config file does not exit. Aborting..."
	exit 1
fi


# prevent running 2 instances of the update_db program
while [ $(pgrep liquid_capture_update_db.sh|wc -l) -ge 3 ]
do
	sleep 10
done
# for those wondering why -ge 3, it's because $(...) forks the shell
# so if another update_db is running, we will have 3 instances of it
# at the time of the pgrep: this script, the fork of it in between $(...)
# and the other one, the one we want to detect. If this script is alone, then
# only 2 processes will exist.

RAWDATA_DIR=$(jshon -e 'liquid_capture' -e 'src_dir' -u < $1)
PROCESSED_FILES=$(jshon -e 'liquid_capture' -e 'dbprocessed' -u < $1)
DBDIR=$(jshon -e 'liquid_capture' -e 'dbdir' -u < $1)

TMPDIR=$(jshon -e 'tmpdir' -u < $1)
RAWFILES=${TMPDIR}/liquid_capture_tmp.txt
NBCORES=$(jshon -e 'nbcores' -u < $1)
gnupar=$(which parallel)

# Check if db file exists
if [ ! -e ${PROCESSED_FILES} ]; then
	touch ${PROCESSED_FILES}
fi

# Check if GNU parallel exists
if [ ! -e ${gnupar} ]; then
	echo "Error: GNU parallel not found"
	exit 1
fi

# Create awk script inline
# http://stackoverflow.com/questions/15020000/embedding-awk-in-a-shell-script
read -d '' awk_script << 'EOF'
{
	n = split($0,a,"/") # basename
	m = split(a[n],b,"[-.]") # components of the filename
	month = substr(b[4],1,6) # retrieve month to form the target dirname
	printf("bzip2 -9c %s > ",$0) # compress job
	printf("%s/%s/%s-%s-%s_%s.csv.bz2\\n", target_dir,month,b[1],b[2],b[3],b[4]) # destination
}
EOF

# 1- Retrieve file names from RAWDATA
# 2- Load PROCESSED_FILES
# 3- Make the difference to find new files
find ${RAWDATA_DIR} -type f -name '*.csv' | sort - ${PROCESSED_FILES} | uniq -u > ${RAWFILES}

# 4- Create directories if they don't exist
awk -v target_dir=${DBDIR} '{split($0,a,"-"); month=substr(a[4],1,6); printf("mkdir %s/%s\n",target_dir,month)}' ${RAWFILES} | sort | uniq | sh &>/dev/null

# 5- Create new file names
# 6- Create compression jobs
awk -v target_dir=${DBDIR} "$awk_script" ${RAWFILES} |\
${gnupar} -j ${NBCORES} &> /dev/null # 7- pass it to GNU parallel to process everything

# 8- update PROCESSED_FILES
cat ${RAWFILES} >> ${PROCESSED_FILES}

# 9- Clean up
rm -f ${RAWFILES}
