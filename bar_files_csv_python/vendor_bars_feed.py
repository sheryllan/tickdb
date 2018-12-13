import sys
import datetime
import pandas as pd

from bars_feed import BarsFeed

class VendorBarsFeed(BarsFeed):

    def __init__(self, inst_name, clock, width, offset, history_len, bar_file_path):
        super(VendorBarsFeed, self).__init__(inst_name, clock, width, offset, history_len, bar_file_path)

    def __parse_date_time(self, date, time):
        year = int(date[-4:])
        day = int(date[:2])
        month = int(date[3:5])
        hour = int(time[:2])
        minute = int(time[-2:])
        ret = datetime.datetime(year, month, day, hour, minute)

    def _file_to_dataframe(self):

        df_bars = pd.read_csv(self._bar_file_path, parse_dates = [[0,1]], header = None) #,  date_parser=self.__parse_date_time)
        #df_bars.index = pd.to_datetime(df_bars[0])
        df_bars.columns = ['bar_time','open','high','low','close','volume']
        df_bars['bar_time'] = pd.to_datetime(df_bars['bar_time'])
        df_bars['bar_time'] = (df_bars['bar_time'] - datetime.datetime(1970,1,1)).dt.total_seconds() * 1e9
        print('Got Bars...')
        print(df_bars.describe())
        print(df_bars.head())

        return df_bars

    def run(self, alpha_framework_python_component, verbose=False):
        super(VendorBarsFeed, self).run(alpha_framework_python_component, verbose)

if __name__ == "__main__":

    bf = VendorBarsFeed(10, sys.argv[1])
    bf.run(None, True)
    print('VendorBarFeed: done')
