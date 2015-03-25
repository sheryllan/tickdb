#!/bin/bash

function exchange {
echo $(basename $1 | awk -F '.' '{print $1}')
}

function month {
echo $(basename $(dirname $1))
}

function decoder {
local exch=$(exchange $1)
if [ "$exch" = "eurex" ]; then
	echo qtg_eobi
elif [ "$exch" = "cme" ]; then
	echo qtg_cme
elif [ "$exch" = "cbot" ]; then
	echo qtg_cme
else
	echo qtg_eobi
fi
}

function is_database_builder_in_path {
which database_builder.py &> /dev/null ; echo $?
}

rootdir=/mnt/data/
tmpdir=/tmp
level=5
dbdir=${rootdir}/database
dbprocessed=${dbdir}/processed_files.db
timestamp=$(date --utc --rfc-3339='ns' | tr ' .:+-' '_')
gnupar=$(which parallel)
qtg_src_dir=${rootdir}/qtg
parjobfile=/tmp/gnupar_job_file.sh
# Check if db file exists
if [ ! -f ${dbprocessed} ]; then
	touch ${dbprocessed}
fi

# Check if GNU parallel exists
if [ ! -f ${gnupar} ]; then
	echo "Error: GNU parallel not found"
	exit 1
fi

# Check if database_builder exists
if [ "$(is_database_builder_in_path)" = "1" ]; then
	database_builder="/home/dbellot/recherche/tickdatabase/database_builder/database_builder.py"
	PYTHONPATH="/home/dbellot/recherche/tickdatabase/database_builder/":${PYTHONPATH}
else
	database_builder=$(which database_builder.py)
fi

# -----
#  QTG
# -----
all_qtg=${tmpdir}/qtg_${timestamp}
new_qtg=${tmpdir}/new_qtg_${timestamp}


# get all qtg files
find ${qtg_src_dir} -name '*.dat.gz' -type f > ${all_qtg}

# find new files only
sort ${all_qtg} ${dbprocessed} | uniq -u | sort - ${all_qtg} | uniq -d > ${new_qtg}
rm -f ${all_qtg}

rm -f ${parjobfile}
while read line
do
echo ${database_builder} -l 5 -o ${dbdir}/qtg/$(exchange ${line})/$(month ${line}) -d $(decoder ${line}) -f csv.bz2 -i ${dbdir}/qtg/instRefdataCoh.csv -g ${dbdir}/qtg/qtg.log ${line} >> ${parjobfile}
done < "${new_qtg}"

cat ${parjobfile} | ${gnupar} &> /dev/null

rm -f ${parjobfile} 
cat ${new_qtg} >> ${dbprocessed}
rm -f ${new_qtg}
