#!/bin/bash

# ---------------
#    Functions
# ---------------

function qtg_exchange {
echo $(basename $1 | awk -F '.' '{print $1}')
}

function month {
echo $(basename $(dirname $1))
}

# this function analyses the file name and validate it
# by returning a decoder for it
# if the file is invalid for any reason, then "not_valid" is returned
# and this file will simply be ignored during this session
function qtg_valid_and_decoder {
local exch=$(qtg_exchange $1)
if [ "$exch" = "eurex" ]; then
	echo qtg_eobi
elif [ "$exch" = "cme" ]; then
	echo qtg_cme
elif [ "$exch" = "cbot" ]; then
	echo qtg_cme
#elif [ "$exch" = "kospi" ]; then
#	echo qtg_kospi
#elif [ "$exch" = "nym" ]; then
#	echo qtg_nym_after_2015
else
	echo not_valid
fi
}

function is_database_builder_in_path {
which database_builder.py &> /dev/null ; echo $?
}

function symbol_per_day {
/bin/env python3 -c '
import sys
x={}

for line in sys.stdin:
	line=line.split()
	symbol=line[0]
	date=line[1]

	if date not in x:
		x[date]=list()
	x[date].append(symbol)

dates=sorted(x.keys())
for d in dates:
	print("{0},".format(d),end="")
	print(",".join(sorted(x[d])))
'
}

# ---------------------------
#    Setup main parameters
# ---------------------------
rootdir=/mnt/data
tmpdir=/tmp
level=5
dbdir=${rootdir}/database
dbprocessed=${dbdir}/processed_files.db
unwanted=${dbdir}/unwanted_files.db
symbol_per_day_file=${dbdir}/symbol_per_day.db
timestamp=$(date --utc --rfc-3339='ns' | tr ' .:+-' '_')
gnupar=$(which parallel)
qtg_src_dir=${rootdir}/qtg
parjobfile=/tmp/gnupar_job_file.sh
nbcores=$(($(nproc)-2)) # get all the cores minus 2

# Check if db file exists
if [ ! -e ${dbprocessed} ]; then
	touch ${dbprocessed}
fi

# Check if the unwanted database exists
if [ ! -e ${unwanted} ]; then
	unwanted=""
fi

# Check if GNU parallel exists
if [ ! -e ${gnupar} ]; then
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

# ---------
#    QTG
# ---------
all_qtg=${tmpdir}/qtg_${timestamp}
new_qtg=${tmpdir}/new_qtg_${timestamp}
invalid_qtg=${tmpdir}/invalid_qtg_${timestamp}

# get all qtg files
find ${qtg_src_dir} -name '*.dat.gz' -type f > ${all_qtg}

# find new files only
sort ${all_qtg} ${dbprocessed} ${unwanted} | uniq -u > ${new_qtg}
rm -f ${all_qtg}

rm -f ${parjobfile}
while read line
do
	outputdir=${dbdir}/qtg/$(qtg_exchange ${line})/$(month ${line})
	mkdir -p ${outputdir} # -p create dirs all the way long and does not fail if dir exists

	# Look for a decoder and discard invalid files
	dec=$(qtg_valid_and_decoder ${line})
	if [ "$dec" != "not_valid" ]; then
		# write the command line to process this file
		echo ${database_builder} -l 5 -o ${outputdir} -d ${dec} -f csv.bz2 -i ${dbdir}/qtg/instRefdataCoh.csv -g ${dbdir}/qtg/qtg.log ${line} >> ${parjobfile}
	else
		# store invalid files here
		echo $line >> ${invalid_qtg}
	fi
done < "${new_qtg}"

# Launch the parallel jobs
#cat ${parjobfile} | ${gnupar} -j ${nbcores} &> /dev/null
cat ${parjobfile} | ${gnupar} -j 10 &> /dev/null

# Update the list of processed for only valid files
cat ${new_qtg} ${invalid_qtg} | sort | uniq -u >> ${dbprocessed}

rm -f ${parjobfile} 
rm -f ${new_qtg}

# -----------------------
#    LIQUID PCAP FILES
# -----------------------

# ----------------------------
#    STATISTICS AND REPORTS
# ----------------------------

# Find available days for each symbol in QTG database

# find produced files | extract symbol name and date in 2 columns |
# remove _ | create a csv files with date,symbol,symbol,symbol,...
# for each date we obtain the list of available symbols in the database
find ${dbdir} -name '*.bz2' |\
${gnupar} basename {} .csv.bz2 |\
awk -F '_' '{print $1,$2}' |\
symbol_per_day > ${symbol_per_day_file}

# Run daily statistic on each symbol
# TODO
