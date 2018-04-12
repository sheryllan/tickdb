#!/bin/bash

export IMPORT_TYPE=REACTOR
export HTTP_HOST="192.168.20.34"
export HTTP_PORT=8086
export INFLUX_DB="reactor_tick"
#directory of qtg tick data
export TICK_DIR=/mnt/tank/backups/london/quants/data/rawdata
#0 : trace, 1 : debug, 2 : info, 3 :warning, 4:error 5:fatal. default: debug
#export DEBUG_LEVEL=0
#number of threads to post influx data, default 4.
export POST_INFLUX_THREAD_COUNT=8
#number of threads to decode qtg file. default 8. decoding is faster than posting(influx can't keep up with decoding). 
#so normally, no need to increase number of thread here.
#export DECODE_THREAD_COUNT=4
#number of qtg market data records in one influx message. 5000 by default.
export INFLUX_BATCH_COUNT=15000
#converts file whose date is bigger than or equal to BEGIN_DATE. default 0000000
export BEGIN_DATE=20180408
#converts file whose date is less than or equal to END_DATE, default INT_MAX
export END_DATE=20180408

#only files for products whose product type is in PRODUCT_TYPES are converted. Strategy is not supported
#valid product types: F : future, O : option, I : index, E : equity, C: currency, B : bond
#default empty which means all product types
export PRODUCT_TYPES='F,O'

#only files for products whose product name is in PRODUCT_NAMES are converted.
#product name here is configured in influx db. check product_name_mapping for detailed configuration.
#default empty which means all product names.
#export PRODUCT_NAMES='FDAX,ED'

export LOG_FILE=reactor2influx`date +%Y-%m-%dT%H:%M:%S`.log

#only files for products whose qtg id is in PRODUCT_ID_RANGES are converted
#1 - 500 means all product ids from 1 to 500 are valid. space does not matter.
#default empty which means all product ids.
#export PRODUCT_ID_RANGES="1 - 500, 600, 2000-2005"

export LD_LIBRARY_PATH=/home/ylei/development/Linuxi686/ylei_depot/BuildKit2/tbb/lib:$LD_LIBRARY_PATH
export LD_PRELOAD="libtbbmalloc_proxy.so.2 libtbbmalloc.so.2"

../release/tick2influx $*

#python resend.py ${LOG_FILE} $HTTP_HOST $HTTP_PORT $INFLUX_DB
