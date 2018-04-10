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
JOIN="\t"

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

def convert_long_utc_time(time,timezone):
    UTC_Time=epoch+timedelta(0,time/1000/1000/1000)
    log_timezone=pytz.timezone(timezone)
    tz_date=UTC_Time.astimezone(log_timezone)
    return tz_date.strftime("%Y-%m-%dT%H:%M:%S") + "." + str(time)[-9:] +tz_date.strftime("%z")[:-2]+":"+tz_date.strftime("%z")[-2:]
    
def convert_utc_time(str_time):
        d=datetime.strptime(str_time[:len("2018-03-07T01:51:22")],"%Y-%m-%dT%H:%M:%S")
        a=str_time.find(".")+1
        date=datetime(d.year,d.month,d.day,d.hour,d.minute,d.second,tzinfo=pytz.utc)
        remainder=str_time[a:-1]        
        return unix_time_nanos(date)+int(remainder)*int(10**(9-len(remainder)))

    
def query_influx_for_trades(database,product,timezone,startDate,seconds,debug,expiration=None,strike=None,cp=None):        
    utc=pytz.timezone('UTC')
    log_timezone=pytz.timezone(timezone)
    search_date=log_timezone.localize(startDate)#.astimezone(utc)
    client = influxdb.InfluxDBClient(host='192.168.55.49',port=8086,database=database)
    #Get books
    additionals=""
    if expiration is not None:
        additionals+=" AND expiry='"+expiration+"'"
    if strike is not None:
        additionals+=" AND strike='"+strike+"'"
    if cp is not None:
        additionals+=" AND cp='"+cp+"'"
    if product.find(',')>0:
        query ="SELECT time,nbid1,bidv1,bid1,ask1,askv1,nask1,product,nbid5,nbid4,nbid3,nbid2,bidv5,bidv4,bidv3,bidv2,bid5,bid4,bid3,bid2,market,ask2,ask3,ask4,ask5,askv2,askv3,askv4,askv5,nask2,nask3,nask4,nask5,expiry,strike,cp,exch,otype,type FROM book WHERE (product='"+"' OR product='".join(product.split(','))+"')"+additionals+" AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds)))) +" tz('"+timezone+"')"
    else:
        query ="SELECT time,nbid1,bidv1,bid1,ask1,askv1,nask1,product,nbid5,nbid4,nbid3,nbid2,bidv5,bidv4,bidv3,bidv2,bid5,bid4,bid3,bid2,market,ask2,ask3,ask4,ask5,askv2,askv3,askv4,askv5,nask2,nask3,nask4,nask5,expiry,strike,cp,exch,otype,type FROM book WHERE product='"+product+"'"+additionals+" AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds)))) +" tz('"+timezone+"')"
    if debug: print(query)
    a=client.query(query)
    i=0
    if a.raw.has_key('series'):
        cols=a.raw['series'][0]['columns']
        vals=a.raw['series'][0]['values']
        #Get trades
        if product.find(',')>0:
            query_trades ="SELECT time,type,product,expiry,strike,cp,side,volume,price,otype,exch FROM trade WHERE (product='"+"' OR product='".join(product.split(','))+"')"+additionals+" AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds)))) +" tz('"+timezone+"')"
        else:
            query_trades ="SELECT time,type,product,expiry,strike,cp,side,volume,price,otype,exch FROM trade WHERE product='"+product+"'"+additionals+" AND time >= "+ str(Decimal(unix_time_nanos(search_date)))+ " AND time <= " + str(Decimal(unix_time_nanos(search_date+timedelta(0,seconds)))) +" tz('"+timezone+"')"
        if debug: print(query_trades)
        b=client.query(query_trades)
        if b.raw.has_key('series'):
            trade_cols=b.raw['series'][0]['columns']
            trade_vals=b.raw['series'][0]['values']
            print(JOIN.join([str(item) for item in cols+["trade"]+trade_cols])) #print the headers but use the trade column
            for val in vals:
                if i<len(trade_vals):
                    while(i<len(trade_vals) and trade_vals[i][0]<=val[0]):#convert_utc_time(trade_vals[i][0])<=convert_utc_time(val[0])):
                        if trade_vals[i][-1]==val[-3]:
                            val[-3]=convert_long_utc_time(val[-3],timezone)
                            trade_vals[i][-1]=convert_long_utc_time(trade_vals[i][-1],timezone)
                            val=val+["TRADE"]+trade_vals[i]
                        else:
                            trade_vals[i][-1]=convert_long_utc_time(trade_vals[i][-1],timezone)
                            print(JOIN.join([str(item) for item in ["" for col in cols]+["TRADE"]+trade_vals[i]]))
                        i+=1
                if "TRADE" not in val:
                    val[-3]=convert_long_utc_time(val[-3],timezone)
                print(JOIN.join([str(item) for item in val]))
        else:
            for val in vals:
                val[-3]=convert_long_utc_time(val[-3],timezone)
                print(JOIN.join([str(item) for item in val]))
                
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Get the tick data (trades and books) for a specific set of products around a specific timeframe sorted by recv time')
    parser.add_argument('product', help='The comma separated list of the products we are searching for')
    parser.add_argument('timezone', help='The timezone of the orderMonitord log: Australia/Sydney')
    parser.add_argument('startDate', help='The date and time we want to start looking at: %%Y-%%m-%%dT%%H:%%M:%%S',type=valid_date)
    parser.add_argument('seconds',type=float,default=60,help='Amount of seconds after startDate to fetch data for')
    parser.add_argument('--expiration',help="The expiration to get data for.")
    parser.add_argument('--strike',help="The strike to get data for.")
    parser.add_argument('--cp',help="The call or put to get data for.")
    parser.add_argument('--debug',dest='debug',help="Run in debug mode.",action='store_true')
    parser.add_argument('--qtg',dest='qtg',help="Use QTG tick data",action='store_true')
    parser.add_argument('--csv',dest='csv',help="Run in debug mode.",action='store_true')
    args = parser.parse_args()
    expiration=None
    strike=None
    cp=None
    if args.expiration: expiration=args.expiration
    if args.strike: strike=args.strike
    if args.cp: cp=args.cp
    database="liquid_tick"
    if args.csv:
        JOIN=","
    if args.qtg:
        database="qtg_tick"
    query_influx_for_trades(database,args.product,args.timezone,args.startDate,args.seconds,args.debug,expiration,strike,cp)
    