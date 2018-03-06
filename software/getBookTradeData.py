import influxdb
from decimal import *
import argparse
import locale
import sys,os
import string
import pytz
from datetime import datetime
from datetime import timedelta

locale.setlocale(locale.LC_NUMERIC,'')
epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)
        
def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0      

def unix_time_nanos(dt):
    return unix_time_millis(dt) * 1000.0 * 1000.0            

def convert_long_utc_time(time):
    return (epoch+timedelta(0,time/1000/1000/1000)).strftime("%Y-%m-%dT%H:%M:%S") + "." + str(time)[-9:] +"Z"
    
def query_influx_for_trades(database,product,timezone,startDate,seconds,debug):        
    utc=pytz.timezone('UTC')
    log_timezone=pytz.timezone(timezone)
    search_date=log_timezone.localize(startDate).astimezone(utc)
    client = influxdb.InfluxDBClient(host='192.168.55.49',port=8086,database=database)
    #Get books
    if product.find(',')>0:
        query ="SELECT time,nbid1,bidv1,bid1,ask1,askv1,nask1,product,nbid5,nbid4,nbid3,nbid2,bidv5,bidv4,bidv3,bidv2,bid5,bid4,bid3,bid2,market,ask2,ask3,ask4,ask5,askv2,askv3,askv4,askv5,nask2,nask3,nask4,nask5,expiry,strike,cp,exch,otype,type FROM book WHERE (product='"+"' OR product='".join(product.split(','))+"') AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds))))
    else:
        query ="SELECT time,nbid1,bidv1,bid1,ask1,askv1,nask1,product,nbid5,nbid4,nbid3,nbid2,bidv5,bidv4,bidv3,bidv2,bid5,bid4,bid3,bid2,market,ask2,ask3,ask4,ask5,askv2,askv3,askv4,askv5,nask2,nask3,nask4,nask5,expiry,strike,cp,exch,otype,type FROM book WHERE product='"+product+"' AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds))))
    if debug: print(query)
    a=client.query(query)
    i=0
    if a.raw.has_key('series'):
        cols=a.raw['series'][0]['columns']
        vals=a.raw['series'][0]['values']
        print(cols)
        #Get trades
        if product.find(',')>0:
            query_trades ="SELECT time,type,product,expiry,strike,cp,volume,side,price,otype,exch FROM trade WHERE (product='"+"' OR product='".join(product.split(','))+"') AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds))))
        else:
            query_trades ="SELECT time,type,product,expiry,strike,cp,volume,side,price,otype,exch FROM trade WHERE product='"+product+"' AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds))))
        if debug: print(query_trades)
        b=client.query(query_trades)
        if b.raw.has_key('series'):
            trade_cols=b.raw['series'][0]['columns']
            trade_vals=b.raw['series'][0]['values']
            print(trade_cols)
            for val in vals:
                if i<len(trade_vals)-1:
                    while(trade_vals[i][0]<=val[0]):
                        trade_vals[i][-1]=convert_long_utc_time(trade_vals[i][-1])
                        print(trade_vals[i])
                        i+=1
                val[-3]=convert_long_utc_time(val[-3])
                print val 
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Get the tick data (trades and books) for a specific set of products around a specific timeframe sorted by recv time')
    parser.add_argument('product', help='The comma separated list of the products we are searching for')
    parser.add_argument('timezone', help='The timezone of the orderMonitord log: Australia/Sydney')
    parser.add_argument('startDate', help='The date and time we want to start looking at: %%Y-%%m-%%dT%%H:%%M:%%S',type=valid_date)
    parser.add_argument('seconds',type=int,default=60,help='Amount of seconds after startDate to fetch data for')
    parser.add_argument('--debug',dest='debug',help="Run in debug mode.",action='store_true')
    parser.add_argument('--qtg',dest='qtg',help="Use QTG tick data",action='store_true')
    args = parser.parse_args()
    database="liquid_tick"
    if args.qtg:
        database="qtg_tick"
    query_influx_for_trades(database,args.product,args.timezone,args.startDate,args.seconds,args.debug)
    