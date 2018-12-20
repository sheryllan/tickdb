import influxdb
#import itertools
import sys
from datetime import datetime
import pytz

locations=["ALC","AUR","DGH","FR2","INT","JPX","OFF_SYD","OFF_LON","SHA","TKO"]


class TickDB_InfluxDBClient:
    def __init__(self,database=None,location=None,type=None,query_live=False,debug=False):
        self.debug=debug
        self.__query_live=query_live
        if database=="liquid_ticker":
            self.__query_live=True
            self.__port=8086
            if location=="FR2":
                self.__host='10.10.12.205'
            elif location=="INT":
                self.__host='192.168.239.5'
            elif location=="AUR":
                self.__host='192.168.28.71'
            elif location=="OFF_LON":
                self.__host='192.168.140.48'
        if not self.__query_live:
            self.__host='192.168.55.49'
            self.__port=8086
        self.__coreClient=influxdb.InfluxDBClient(host=self.__host,port=self.__port)
        ref_db="liquid_tick"
        if  database is not None:
            ref_db=database
        self.__refdataClient=influxdb.InfluxDBClient(host=self.__host,port=self.__port,database='qtg_store_tick')
        self.__singleClient=None
        self.__multiClients={}
        if (type=="O" or type=="F") and database is None:
            if location in locations:
                self.set_single("liquid_tick_"+location+"_"+type)
            else:
                for location in locations:
                    self.add_multi("liquid_tick_"+location+"_"+type)
        elif database is None:
            for loc in locations:
                if location is None or loc==location:
                    self.add_multi("liquid_tick_"+loc+"_O")
                    self.add_multi("liquid_tick_"+loc+"_F")
        elif database is not None:
                self.set_single(database)
        
    
    def set_single(self,single_name):
        if self.valid_database(single_name):
            if self.debug: print("Single:",self.__host,self.__port,single_name)
            sys.stdout.flush()        
            self.__singleClient=influxdb.InfluxDBClient(host=self.__host,port=self.__port,database=single_name)
            
    def add_multi(self,multi_name):
        if self.valid_database(multi_name):
            self.__multiClients[multi_name]=influxdb.InfluxDBClient(host=self.__host,port=self.__port,database=multi_name)
        
    def valid_database(self,dbname):
        return dbname in [a["name"] for a in self.__coreClient.get_list_database()]
        
    def query(self,query):
        if "from refdata" in query.lower():
            if self.debug: print("Ref Query",query)
            return self.__refdataClient.query(query)
        else:
            if self.__singleClient is not None:
                return self.__singleClient.query(query)
            else:
                results=TickDB_ResultSet(query,debug=self.debug)
                for db in self.__multiClients:
                    try:
                        result= self.__multiClients[db].query(query)
                        if result is not None:
                            if self.debug: print("Got Data: ",query,db)
                            results.addResults(result)
                    except influxdb.exceptions.InfluxDBClientError:
                        pass
                return results
            return None
            

class TickDB_ResultSet:
    def __init__(self,query,debug=False):
        self.__resultset=[]
        self.__query=query
        self.__debug=debug
        self.__raw=None

    def addResults(self,resultSet):
        if 'series' in resultSet.raw:# and self.__resultset is None:
            self.__resultset.append(resultSet)
    
    def __getSeries(self,raw,name):
        res=None
        for series in raw['series']:
            if series['name']==name:
                return series
        return res
    
    def __createRaw(self):
        if self.__raw is None:
            raw={}
            for set in self.__resultset:
                if 'series' in set.raw:
                    if 'series' not in raw:
                        raw['series']=[]
                        for series in set.raw['series']:
                            raw['series'].append(series)
                    else:
                        for series in set.raw['series']:
                            for type in series.keys():
                                if type=="columns":
                                    pass #same query so the columns should be aligned
                                elif type=="values":
                                    use_series=self.__getSeries(raw,series['name'])
                                    use_series['values']=sorted(use_series['values']+series['values'], key = lambda x: convert_utc_time(x[0]))
                                elif type=='name':
                                    pass #same query should have the same name
                                else:
                                    print("ERROR: Unhanded type: " + type)
            self.__raw=raw
            
    @property
    def raw(self):
        if len(self.__resultset)==0:
            return {}
        else:
            if len(self.__resultset)==1:
                return self.__resultset[0].raw
            else:
                self.__createRaw()
                return self.__raw
        
    #def get_points(self, measurement=None, tags=None):
    #    if len(self.__resultset)==0:
    #        return None
    #    else:
    #        if len(self.__resultset)==1:
    #            return self.__resultset[0].get_points()
    #        else:
    #            return self.__resultset[0].get_points()
                #Here is where the problem is.... I am only returning the first results.  I need to merge all the results but this does not work with a generator.

        

###############################
#   TIME CONVERSIONS
###############################
epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0      

def unix_time_nanos(dt):
    return unix_time_millis(dt) * 1000.0 * 1000.0  
def convert_utc_time(str_time):
        d=datetime.strptime(str_time[:len("2018-03-07T01:51:22")],"%Y-%m-%dT%H:%M:%S")
        a=str_time.find(".")+1
        date=datetime(d.year,d.month,d.day,d.hour,d.minute,d.second,tzinfo=pytz.utc)
        b=max(str_time[a:].find("-"),str_time[a:].find("+"))
        if b<=0:
            remainder=str_time[a:]
        else:
            remainder=str_time[a:b+a]
        return unix_time_nanos(date)+int(remainder)*int(10**(9-len(remainder)))
###############################
#   DEBUG
###############################
def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))
        sys.stdout.flush()        