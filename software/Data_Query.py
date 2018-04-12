from influxdb import InfluxDBClient
import pytz 
from datetime import datetime
from datetime import timedelta
import argparse

epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

def convert_long_utc_time(time,timezone):
    UTC_Time=epoch+timedelta(0,time/1000/1000/1000)
    log_timezone=pytz.timezone(timezone)
    tz_date=UTC_Time.astimezone(log_timezone)
    return tz_date.strftime("%Y-%m-%dT%H:%M:%S") + "." + str(time)[-9:] +tz_date.strftime("%z")[:-2]+":"+tz_date.strftime("%z")[-2:]

def valid_short_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)
        
def query_influx_for_book_times(database,product,timezone,startDate,endDate,group,debug,expiration=None,strike=None,cp=None):        
    utc=pytz.timezone('UTC')
    log_timezone=pytz.timezone(timezone)
    search_date=log_timezone.localize(startDate)#.astimezone(utc)
    client = InfluxDBClient(host='192.168.55.49',port=8086,database=database)
    #Get books
    additionals=""
    if expiration is not None:
        additionals+=" AND expiry='"+expiration+"'"
    if strike is not None:
        additionals+=" AND strike='"+strike+"'"
    if cp is not None:
        additionals+=" AND cp='"+cp+"'"
    if product.find(',')>0:
        query="SELECT max(exch),min(exch),count(otype) from book where product='".join(product.split(','))+"')"+additionals+" and time>'"+str(startDate)+"' and time<'"+str(endDate)+"' group by time("+group+"),product" +" tz('"+timezone+"')"
    else:
        query="SELECT max(exch),min(exch),count(otype) from book where product='"+product+"'"+additionals+" and time>='"+str(startDate)+"' and time<'"+str(endDate)+"' group by time("+group+"),product" +" tz('"+timezone+"')"    
    if debug: print(query)
    data=client.query(query)
    i=0
    for point in data.get_points():
        if point["count"]>0:
            point["min"]=convert_long_utc_time(point["min"],timezone)
            point["max"]=convert_long_utc_time(point["max"],timezone)            
            print("{0}\t{1}\t{2}\t{3}".format(point['time'],point['min'],point['max'],point['count']))

if __name__=="__main__":
    group="1d"
    endDate=datetime.now()
    expiration=None
    strike=None
    cp=None
    debug=False
    database="liquid_tick"
    parser = argparse.ArgumentParser(description='Get the min and max time of data recorded from the exchange timestamp for a series of days.')
    parser.add_argument('product', help='The comma separated list of the products we are searching for')
    parser.add_argument('timezone', help='The timezone you want the data searched and presented in: e.g. Australia/Sydney')
    parser.add_argument('startDate', help='The date and time we want to start looking at: %%Y-%%m-%%d',type=valid_short_date)
    parser.add_argument('--endDate', help='The date and time we want to stop looking at: %%Y-%%m-%%d',type=valid_short_date)
    parser.add_argument('--group', help="The way you want to group 1d,1h? Default is {0}".format(group))
    parser.add_argument('--expiration',help="The expiration to get data for.")
    parser.add_argument('--strike',help="The strike to get data for.")
    parser.add_argument('--cp',help="The call or put to get data for.")
    parser.add_argument('--debug',dest='debug',help="Run in debug mode.",action='store_true')
    parser.add_argument('--qtg',dest='qtg',help="Use QTG tick data",action='store_true')
    args = parser.parse_args()

    if args.expiration: expiration=args.expiration
    if args.strike: strike=args.strike
    if args.cp: cp=args.cp
    if args.debug: debug=args.debug
    if args.qtg: database="qtg_store_tick"
    if args.group: group=args.group
    if args.endDate: endDate=args.endDate
    query_influx_for_book_times(database,args.product,args.timezone,args.startDate,endDate,group,debug,expiration,strike,cp)