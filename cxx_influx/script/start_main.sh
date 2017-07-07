export QTG_PRODUCT_FILE=/home/ylei/work/instRefdataCoh.csv
export HTTP_HOST="192.168.20.34"
export HTTP_PORT=8086
export INFLUX_DB="test"
export STORE_TICK_DIR=$1
#export DEBUG_LEVEL=0
export POST_INFLUX_THREAD_COUNT=8
export INFLUX_BATCH_COUNT=100000
#export BEGIN_DATE=20120101
#export END_DATE=20091001

export LD_LIBRARY_PATH=/home/ylei/development/Linuxi686/ylei_depot/BuildKit2/tbb/lib:$LD_LIBRARY_PATH
export LD_PRELOAD="libtbbmalloc_proxy.so.2 libtbbmalloc.so.2"

../release/strat $*
