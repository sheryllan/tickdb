export QTG_PRODUCT_FILE=/mnt/data/qtg/instRefdataCoh.csv
export HTTP_HOST="192.168.55.49"
export HTTP_PORT=8086
export INFLUX_DB="qtg_tick"
#directory of qtg tick data
export STORE_TICK_DIR=$1
#0 : trace, 1 : debug, 2 : info, 3 :warning, 4:error
#export DEBUG_LEVEL=0
#number of threads to post influx data, default 4.
export POST_INFLUX_THREAD_COUNT=8
#number of threads to decode qtg file. default 8. decoding is faster than posting(influx can't keep up with decoding). 
#so normally, no need to increase number of thread here.
#export DECODE_THREAD_COUNT=14
#number of qtg market data records in one influx message. 5000 by default.
export INFLUX_BATCH_COUNT=30000
#converts file whose date is bigger than or equal to BEGIN_DATE. default 0000000
export BEGIN_DATE=20101123
#convers file whose date is less than or equal to END_DATE, default INT_MAX
export END_DATE=`date -d '2 days' +%Y%m%d`

#only files for products that belong to exchange in PRODUCT_EXCHCHANGES are converted
#exchange is seperated by ',', default empty which means all exchanges
export PRODUCT_EXCHCHANGES='XEUR'

#only files for products whose product type is in PRODUCT_TYPES are converted.
#valid product types: F : future, O : option, S : strategy, I : index, E : equity, C: currency, B : bond
#default empty which means all product types
export PRODUCT_TYPES='F,O'

#only files for products whose product name is in PRODUCT_NAMES are converted.
#product name here is configured in influx db. check product_name_mapping for detailed configuration.
#default empty which means all product names.
#export PRODUCT_NAMES='FDAX,ED'

#files for products whose product name is in EXCLUDED_PRODUCT_NAMES are not converted.
#product name here is configured in influx db. check product_name_mapping for detailed configuration
#default empty which means no product is excluded.
#export EXCLUDED_PRODUCT_NAMES='FDAX,ED'


#only files for products whose qtg id is in PRODUCT_ID_RANGES are converted
#1 - 500 means all product ids from 1 to 500 are valid. space does not matter.
#default empty which means all product ids.
#export PRODUCT_ID_RANGES="1 - 500, 600, 2000-2005"

export LD_LIBRARY_PATH=/home/ylei/development/Linuxi686/ylei_depot/BuildKit2/tbb/lib:$LD_LIBRARY_PATH
export LD_PRELOAD="libtbbmalloc_proxy.so.2 libtbbmalloc.so.2"

../release/qtg2influx $*
