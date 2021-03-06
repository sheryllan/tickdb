#!/bin/bash

if [[ $# -ne 1 ]]; then
	echo "Error: missing argument"
	echo "Usage: qtg_update_db.sh <JSON config file>"
	exit 1
fi

# prevent running 2 instances of the qtg_update_db program
while [ $(pgrep qtg_update_db.sh|wc -l) -ge 3 ]
do
	sleep 10
done
# for those wondering why -ge 3, it's because $(...) forks the shell
# so if another qtg_update_db is running, we will have 3 instances of it
# at the time of the pgrep: this script, the fork of it in between $(...)
# and the other one, the one we want to detect. If this script is alone, then
# only 2 processes will exist.

# ---------------
#    Functions
# ---------------

function launch_qtg_jobs {
cat $1 | $2 -j $3 &> /dev/null
}

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
# argv[1] = JSON config file
# argv[2] = filename
function qtg_find_decoder {
/bin/env python3 -c '
import sys,os,json

j=json.load(open(sys.argv[1]))

x=sys.argv[2]
x=x.split("/")
try:
	month=int(x[len(x)-2])
except:
	print("not_valid")
	sys.exit(0)

repo =x[len(x)-3]

if repo not in j["decoder"]:
	print("not_valid")
else:
	for v in j["decoder"][repo]:
		from_=v["from"]
		to_=v["to"]
		if to_==0:
			to_=10**10
		if month>=from_ and month<=to_:
			print(v["code"])
			sys.exit(0)
	print("not_valid")
' $1 $2
}

function is_database_builder_in_path {
which database_builder.py &> /dev/null ; echo $?
}

# ---------------------------
#    Setup main parameters
# ---------------------------
jsonconf=$1
if [ ! -e ${jsonconf} ]; then
	echo "Error: JSON config file does not exit. Aborting..."
	exit 1
fi

# Extract config from JSON file
JSHON=$(which jshon)
if [ $? -eq 1 ]; then
	echo "Error: jshon not found"
	echo $PATH
	exit 1
fi
tmpdir=$(${JSHON} -e 'tmpdir'               -u < ${jsonconf})
level=$(${JSHON} -e 'level'                -u < ${jsonconf})
dbdir=$(${JSHON} -e 'qtg' -e 'dbdir'       -u < ${jsonconf})
dbprocessed=$(${JSHON} -e 'qtg' -e 'dbprocessed' -u < ${jsonconf})
unwanted=$(${JSHON} -e 'qtg' -e 'unwanted'    -u < ${jsonconf})
qtg_src_dir=$(${JSHON} -e 'qtg' -e 'src_dir'     -u < ${jsonconf})
qtg_instrument_db=$(${JSHON} -e 'qtg' -e 'instdb'      -u < ${jsonconf})

timestamp=$(date --utc --rfc-3339='ns' | tr ' .:+-' '_')
# Check if GNU parallel exists
gnupar=$(which parallel)
if [ $? -eq 1 ]; then
	echo "Error: GNU parallel not found"
	exit 1
fi

parjobfile=$(${JSHON} -e 'parjobfile' -u < ${jsonconf})
nbcores=$(${JSHON} -e 'nbcores'    -u < ${jsonconf})

# Check if db file exists
if [ ! -e ${dbprocessed} ]; then
	touch ${dbprocessed}
fi

# Check if the unwanted database exists
if [ ! -e ${unwanted} ]; then
	unwanted=""
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
find ${qtg_src_dir}/ -name '*.dat.gz' > ${all_qtg} 2>/dev/null

# find new files only
sort ${all_qtg} ${dbprocessed} ${unwanted} | uniq -u > ${new_qtg}
rm -f ${all_qtg}

rm -f ${parjobfile}
touch ${parjobfile}

while read line
do
	# Look for a decoder and discard invalid files
	dec=$(qtg_find_decoder ${jsonconf} ${line})
	if [ "$dec" != "not_valid" ]; then
		outputdir=${dbdir}/$(qtg_exchange ${line})/$(month ${line})
		mkdir -p ${outputdir} # -p create dirs all the way long and does not fail if dir exists
		# write the command line to process this file
		echo ${database_builder} -l 5 -o ${outputdir} -d ${dec} -f csv.bz2 -i ${qtg_instrument_db} -g ${dbdir}/qtg.log ${line} >> ${parjobfile}
	else
		# store invalid files here
		echo $line >> ${invalid_qtg}
	fi
done < "${new_qtg}"

# Launch the parallel jobs
#cat ${parjobfile} | ${gnupar} -j ${nbcores} &> /dev/null
cat ${parjobfile} | ${gnupar} -j 12 &> /dev/null
rm -f ${parjobfile} 

# Update the list of processed for only valid files
cat ${new_qtg} ${invalid_qtg} | sort | uniq -u >> ${dbprocessed}
rm -f ${new_qtg} ${invalid_qtg}
