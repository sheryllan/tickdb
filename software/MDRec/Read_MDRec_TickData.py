# -*- coding: utf-8 -*-
"""
Created on Fri Nov 30 16:50:38 2018

@author: mshapiro
"""

import pandas as pd
# import seaborn as sns
import numpy as np
import NanoTime as nt
import os
import pytz
import platform
from pathlib import Path, PureWindowsPath

np.set_printoptions(suppress=True, formatter={'float_kind': '{:0.2f}'.format})


class MDRecQueryParameters:
    def __init__(self):
        # TODO WHAT DO I DO HERE WITH COLO? HOW IS THIS BEING DEFINED?
        self.__coloFacilities = ["JPX", "ALC", "DGH", "TKO", "FR2", "AUR", "INX"]
        self.__exchange_timezone = None
        self.__analysis_timezone = None
        self.__colocation_facility = None
        # TODO  I thiink this all needs to be changed to support PROD.O.MN.* and PROD.F.MN.DEC2018, etc.
        self.__product = []
        self.__series = []
        self.__product_type = []
        #  TODO NEED TO HANDLE Dates BETTER.... NOT AWARE OF WHAT IS IN THE FILES
        self.__trading_date = []
        self.__load_level3 = False
        self.__load_trades = False
        self.__clean_books = False
        self.__use_NanoTimes = False
    
    @property
    def exchange_timezone(self):
        return self.__exchange_timezone
    
    @exchange_timezone.setter
    def exchange_timezone(self, value):
        tz = pytz.timezone(value)
        if tz is not None:
            self.__exchange_timezone = value
        
    @property
    def analysis_timezone(self):
        return self.__analysis_timezone
    
    @analysis_timezone.setter
    def analysis_timezone(self, value):
        tz = pytz.timezone(value)
        if tz is not None:
            self.__analysis_timezone = value
        
    @property
    def colocation_facility(self):
        return self.__colocation_facility 
    
    @colocation_facility.setter
    def colocation_facility(self, value):
        if value in self.__coloFacilities:
            self.__colocation_facility = value
        else:
            raise(TypeError, "Cannot set colo.  Use: ", self.__coloFacilities)
    
    @property
    def product(self):
        return self.__product.copy() 

    def add_product(self, product):
        if product not in self.__product:
            self.__product.append(product)
            
    def remove_product(self, product):
        if product in self.__product:
            self.__product.remove(product)
            
    def clear_product(self):
        self.__product.clear()
        
    @property
    def series(self):
        return self.__series.copy()

    def add_series(self, series):
        if series not in self.__series:
            self.__series.append(series)
            
    def remove_series(self, series):
        if series in self.__series:
            self.__series.remove(series)
            
    def clear_series(self):
        self.__series.clear()        

    @property
    def product_type(self):
        return self.__product_type.copy()

    def add_product_type(self, product_type):
        if product_type not in self.__product_type:
            self.__product_type.append(product_type)
            
    def remove_product_type(self, product_type):
        if product_type in self.__product_type:
            self.__product_type.remove(product_type)
            
    def clear_product_type(self):
        self.__product.clear() 

    @property
    def trading_date(self):
        return self.__trading_date.copy()

    def add_trading_date(self, trading_date):
        if trading_date not in self.__trading_date:
            self.__trading_date.append(trading_date)
            
    def remove_trading_date(self, trading_date):
        if trading_date in self.__trading_date:
            self.__trading_date.remove(trading_date)
            
    def clear_trading_date(self):
        self.__product.clear()                   
    
    @property    
    def load_level3(self):
        return self.__load_level3
    
    @load_level3.setter
    def load_level3(self, value):
        if isinstance(value, bool):
            self.__load_level3 = value
        else:
            raise(TypeError, "Cannot set to non-boolean value")
            
    @property    
    def load_trades(self):
        return self.__load_trades
    
    @load_trades.setter
    def load_trades(self, value):
        if isinstance(value, bool):
            self.__load_trades = value
        else:
            raise(TypeError, "Cannot set to non-boolean value")
                 
    @property    
    def clean_books(self):
        return self.__clean_books
    
    @clean_books.setter
    def clean_books(self, value):
        if isinstance(value, bool):
            self.__clean_books = value
        else:
            raise(TypeError, "Cannot set to non-boolean value")
            
    @property    
    def use_nanotimes(self):
        return self.__use_NanoTimes
    
    @use_nanotimes.setter
    def use_nanotimes(self, value):
        if isinstance(value, bool):
            self.__use_NanoTimes = value
        else:
            raise(TypeError, "Cannot set to non-boolean value")


class MDRecQuery:
    def __init__(self, query):
        self.__query = query
        # Files which were loaded
        self.csv_files = []
        self.l3_files = []
        # Data frames for each file loaded
        self.load_l3_df = []
        self.load_df = []
        # Unclean dataframe of all data loaded
        self.df = None
        self.l3 = None
        if platform.system() == "Windows":
            self.__data_location = Path(PureWindowsPath(r'\\LCLDN-STORE1\backups\london\quants\data\rawdata'))
        else:
            self.__data_location = Path(r'/var/build/TANK0/london/quants/data/rawdata')
        self.__available_dates = os.listdir(self.__data_location)
        self.__load_dataframes()
        self.__clean_data()
        
    def __load_dataframes(self):
        for date in self.__query.trading_date:
            directory = self.__data_location / date
            files = os.listdir(directory)
            # Load Level 2 Files
            self.csv_files = [list((*x.split('-'), x)) for x in files if x.endswith('.csv.xz')]
            # Insert Blank Series
            for el in self.csv_files:
                if el[2] in ("E", "I", "S"):
                    el.insert(3, "")
            # Create data frames with all the files
            csv_files_df = pd.DataFrame(self.csv_files,
                                        columns=['location', 'product', 'product_type',
                                                 'series', 'date', 'stub', 'file'])
            # TODO NEED TO HANDLE THIS BETTER... NOW I REQUIRE ALL 3 when I could do something more complex...
            #  see note in query above
            for file in csv_files_df[(csv_files_df['product_type'].isin(self.__query.product_type) &
                                      csv_files_df['product'].isin(self.__query.product) &
                                      csv_files_df['series'].isin(self.__query.series))].file:
                # TODO This may be wrong to set the dtype of bidc1 to float64.
                self.load_df.append(pd.read_csv(directory / file, compression='xz', dtype={"nicts": "float64"}))
            self.df = pd.concat(self.load_df, axis=0, ignore_index=True)
            
            if self.__query.load_level3:
                # Load Level 3 files
                self.l3_files = [list((*x.split('-'), x)) for x in files if x.endswith('.l3.xz')]
                # Insert Blank Series
                for el in self.l3_files:
                    if el[2] in ("E", "I", "S"):
                        el.insert(3, "")
                l3_files_df = pd.DataFrame(self.l3_files,
                                           columns=['location', 'product', 'product_type',
                                                    'series', 'date', 'stub', 'file'])
                for file in l3_files_df[(l3_files_df['product_type'].isin(self.__query.product_type) &
                                         l3_files_df['product'].isin(self.__query.product) &
                                         l3_files_df['series'].isin(self.__query.series))].file:
                    self.load_l3_df.append(pd.read_csv(directory / file, compression='xz', dtype={"nicts": "float64"}))
                self.l3 = pd.concat(self.load_l3_df, axis=0, ignore_index=True)

    def __clean_data(self):      
        ######################
        # Create books
        ######################
        # Convert the Reactor NaN of 999999999998 and 999999999999 to None
        adj = self.df.replace({999999999998: None, 999999999999: None})
        adj['nicts'] = adj['nicts'].replace({0: np.NaN})
        # Split trades and trade summaries from book updates
        self.books = adj[~adj['otype'].isin(['S', 'P', 'U'])]
        if self.__query.use_nanotimes:
            self.books["exch_ts"] = self.books.exch[~self.books.exch.isna()].apply(
                lambda x: self.__nano_ts(np.int64(x), self.__query.exchange_timezone))
            self.books["recv_ts"] = self.books.recv[~self.books.recv.isna()].apply(
                lambda x: self.__nano_ts(np.int64(x), self.__query.analysis_timezone))
            self.books["nic_ts"] = self.books.nicts[~self.books.nicts.isna()].apply(
                lambda x: self.__nano_ts(np.int64(x), self.__query.analysis_timezone))
        
        ######################
        # Create trades
        ######################
        if self.__query.load_trades:
            all_trades = adj[adj['otype'].isin(['S', 'P', 'U'])]
            self.trades = all_trades.drop(
                columns=['bidv3', 'bidv4', 'bidv5', 'nbid1', 'nbid2', 'nbid3',
                         'nbid4', 'nbid5', 'ask1', 'ask2', 'ask3', 'ask4', 'ask5', 'askv1',
                         'askv2', 'askv3', 'askv4', 'askv5', 'nask1', 'nask2', 'nask3', 'nask4',
                         'nask5', 'bidc1', 'bidc2', 'bidc3', 'bidc4', 'bidc5', 'askc1', 'askc2',
                         'askc3', 'askc4', 'askc5'])
            cols = list(self.trades.columns)
            cols[cols.index('bid1')] = 'price'
            cols[cols.index('bid2')] = 'volume'
            cols[cols.index('bid3')] = 'aggressive_side'
            cols[cols.index('bid4')] = 'buyer'
            cols[cols.index('bid5')] = 'volume_by_cpty'
            cols[cols.index('bidv1')] = 'price_by_cpty'
            cols[cols.index('bidv2')] = 'seller'
            self.trades.columns = cols
            if self.__query.use_nanotimes:
                self.trades["exch_ts"] = self.trades.exch[~self.trades.exch.isna()].apply(
                    lambda x: self.__nano_ts(np.int64(x), self.__query.exchange_timezone))
                self.trades["nic_ts"] = self.trades.nicts[~self.trades.nicts.isna()].apply(
                    lambda x: self.__nano_ts(np.int64(x), self.__query.analysis_timezone))
                self.trades["recv_ts"] = self.trades.recv[~self.trades.recv.isna()].apply(
                    lambda x: self.__nano_ts(np.int64(x), self.__query.analysis_timezone))
        else:
            self.trades = None
        
        ######################
        # Create Top of Book
        ######################
        if self.__query.clean_books:
            # Clean to remove where both sides of the book are not there or the book is inverted
            clean_dropna = self.books.dropna(subset=['bid1', 'ask1'])
            clean = clean_dropna[clean_dropna['bid1'] < clean_dropna['ask1']]
            # Finally only look at books with TOB changes
            self.tob_changes = clean[(clean['bid1'].diff(periods=1) != 0) | (clean['ask1'].diff(periods=1) != 0)]
        else:
            self.tob_changes = None
        
        if self.__query.load_level3 and self.__query.use_nanotimes:
            ######################
            # Format Level 3 Data
            ######################
            self.l3["exch_ts"] = self.l3.exch[~self.l3.exch.isna()].apply(
                lambda x: self.__nano_ts(np.int64(x), self.__query.exchange_timezone))
            self.l3["recv_ts"] = self.l3.recv[~self.l3.recv.isna()].apply(
                lambda x: self.__nano_ts(np.int64(x), self.__query.analysis_timezone))

    @staticmethod
    def __nano_ts(val, tz):
        return nt.NanoTime(val, tzinfo=tz)
    
    def plot_bids(self):
        ######################
        # Create Plot
        ######################
        pass
        # sns.lineplot(x="recv", y="bid1",data=self.tob_changes)

    def print_market_inversions(self):
        ######################
        # Bad data
        ######################
        # TODO Something very odd here as bid1 > ask1 .... IS THIS AUCTION?... I think so.  How to clean better?
        bid_ask_error = self.books[self.books['bid1'] > self.books['ask1']]
        odd = self.books[self.books['exch'].isin(bid_ask_error.exch.unique())]
        for time in odd.exch.unique():
            n = nt.NanoTime(time, tzinfo=self.__query.exchange_timezone)
            print(n)


if __name__ == "__main__":
    qp = MDRecQueryParameters()
    qp.exchange_timezone = "Asia/Tokyo"
    qp.analysis_timezone = "Australia/Sydney"
    qp.colocation_facility = "JPX"
    qp.add_product("MN")
    qp.add_series("DEC2018")
    qp.add_product_type("F")
    qp.add_trading_date("20181205")
    qp.load_level3 = True
    qp.load_trades = True
    qp.clean_books = True
    q = MDRecQuery(qp)
    from datetime import datetime
    start = nt.NanoTime(dt=pytz.timezone("Asia/Tokyo").localize(datetime(2018, 12, 5, 9, 0, 1))).nanoseconds
    end = nt.NanoTime(dt=pytz.timezone("Asia/Tokyo").localize(datetime(2018, 12, 5, 9, 0, 2))).nanoseconds
    print(str(datetime.now()))
    rng = q.books[((q.books['recv'] > start) & (q.books['recv'] < end))]
    print(str(datetime.now()))
