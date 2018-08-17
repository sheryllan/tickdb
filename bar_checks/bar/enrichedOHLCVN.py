from collections import namedtuple

from influxdb import DataFrameClient

from holidaycalendar import *
from bar.timeseries_check import *
from influxcommon import *
from quantdb1_config import EnrichedOHLCVN as m
from xmlconverter import *

BarId = namedtuple('BarId', m.TAGS)


class BarChecker(object):
    
    # region check columns
    OK_HIGH_LOW = 'ok_high_low'
    OK_PCLV_ORDER = 'ok_pclv_order'
    OK_BID_ASK = 'ok_bid_ask'
    OK_NET_VOLUME = 'ok_net_volume'
    OK_VOL_ON_LV1 = 'ok_vol_on_lv1'
    OK_VOL_ON_LV2 = 'ok_vol_on_lv2'
    OK_VOL_ON_LV3 = 'ok_vol_on_lv3'
    # endregion

    TAG_KEYS = [m.PRODUCT, m.TYPE, m.EXPIRY, m.CLOCK_TYPE, m.OFFSET, m.WIDTH]
    CHECK_COLS = [OK_HIGH_LOW, OK_PCLV_ORDER, OK_BID_ASK, OK_NET_VOLUME,
                  OK_VOL_ON_LV1, OK_VOL_ON_LV2, OK_VOL_ON_LV3]

    OUTCOLS = TAG_KEYS + CHECK_COLS

    @classmethod
    def check_high_low(cls, df):
        hask, high, hbid = df[m.HASK], df[m.HIGH], df[m.HBID]
        lask, low, lbid = df[m.LASK], df[m.LOW], df[m.LBID]

        df[cls.OK_HIGH_LOW] = (hask >= high) & (high >= low) & (low >= lbid)
        return df

    @classmethod
    def check_pclv_order(cls, df):
        cask1, cask2, cask3 = df[m.CASK1], df[m.CASK2], df[m.CASK3]
        cbid1, cbid2, cbid3 = df[m.CBID1], df[m.CBID2], df[m.CBID3]

        df[cls.OK_PCLV_ORDER] = (cask3 > cask2) & (cask2 > cask1) & (cbid1 > cbid2) & (cbid2 > cbid3)
        return df

    @classmethod
    def check_bid_ask(cls, df):
        cask1, cask2, cask3, hask, lask = df[m.CASK1], df[m.CASK2], df[m.CASK3], df[m.HASK], df[m.LASK]
        cbid1, cbid2, cbid3, hbid, lbid = df[m.CBID1], df[m.CBID2], df[m.CBID3], df[m.HBID], df[m.LBID]

        df[cls.OK_BID_ASK] = (cask1 > cbid1) & (cask2 > cbid2) & (cask3 > cbid3) & (hask > hbid) & (lask > lbid)
        return df

    @classmethod
    def check_net_volume(cls, df):
        df[cls.OK_NET_VOLUME] = df[m.TBUYV] - df[m.TSELLV] == df[m.NET_VOLUME]
        return df

    @classmethod
    def check_vol_on_lv(cls, df):
        df[cls.OK_VOL_ON_LV1] = ~((df[m.CASK1] != 0) ^ (df[m.CASKV1] != 0))
        df[cls.OK_VOL_ON_LV2] = ~((df[m.CASK2] != 0) ^ (df[m.CASKV2] != 0))
        df[cls.OK_VOL_ON_LV3] = ~((df[m.CASK3] != 0) ^ (df[m.CASKV3] != 0))
        return df




class SeriesChecker(object):
    @classmethod
    def bday_start(cls, dt, tz=True):
        tm = time(0, 0, 0)
        return datetime.combine(dt, tm, tz)

    @classmethod
    def bday_end(cls, dt, tz=True):
        tm = time(0, 0, 0)
        next_dt = last_n_days(-1, dt)
        return datetime.combine(next_dt, tm, tz)

    @classmethod
    def run_timeseries_checks(cls, data, hcalendar):
        for id, records in data.groupby(m.TAGS):
            barid = BarId(*id)._asdict()
            for (dt, tz), dt_df in records.groupby(lambda x: (x.date(), x.tz)):
                if dt.weekday() not in range(0, 5):
                    continue

                timestamps = dt_df.index
                start_ts, end_ts = cls.bday_start(dt, tz), cls.bday_end(dt, tz)
                offset, interval = barid[m.OFFSET], barid[m.WIDTH]
                valid, invalid = validate_timestamps(timestamps, start_ts, end_ts, offset, interval)
                reversions = list(check_time_reversion(valid))
                gaps = list(check_intraday_gaps(valid))

                print()




class CheckTask(object):
    def __init__(self, ):
        self.client = DataFrameClient(host=HOSTNAME, port=PORT, database=Bar.DBNAME)

    def get_data(self, time_from, product=None, ptype=None, time_to=None, expiry=None):
        fields = {m.PRODUCT: product, m.TYPE: ptype, m.EXPIRY: expiry}
        terms = format_arith_terms(fields)
        qstring = select_where_time_bound(Bar.MS_EOHLCVN, time_from, time_to, where_terms=terms)
        return self.client.query(qstring).get(Bar.MS_EOHLCVN, None)

    def run_bar_checks(self, data):
        BarChecker.check_high_low(data)
        BarChecker.check_pclv_order(data)
        BarChecker.check_bid_ask(data)
        BarChecker.check_net_volume(data)
        BarChecker.check_vol_on_lv(data)

        results = data[~data[BarChecker.CHECK_COLS].all(axis=1)]

        xml = df_to_xmletree('timeseries', 'entry', results[BarChecker.OUTCOLS], TIME_IDX)

        from io import StringIO
        sio = StringIO()
        to_xslstyle_xml(xml, 'sample.xsl', sio)
        sio.seek(0)
        print(sio.read())

        print()

    def run_checks(self, dfrom, product=None, ptype=None, dto=None, expiry=None):
        data = self.get_data(dfrom, product, ptype, dto, expiry)
        if data is None:
            msg = {'product': product, 'ptype': ptype, 'expiry': expiry, 'dfrom': dfrom, 'dto': dto}
            raise ValueError('No data selected for {}'.format({k: v for k, v in msg.items()}))

        SeriesChecker.run_timeseries_checks(data)


import pytz

cbh = CustomBusinessHour(calendar=GeneralCalendar(), start=time(0, 0, 0, tzinfo=pytz.timezone('EST')), end=time(21, 0, 0, tzinfo=pytz.timezone('EST')))
datetime(2018, 8, 15, 0, tzinfo=pytz.UTC) + cbh

if __name__ == '__main__':
    c = CheckTask()
    c.run_checks(date(2018, 6, 1), 'CL', 'F')


# self.aparser = argparse.ArgumentParser()
# self.aparser.add_argument('--dfrom', nargs=3, type=int, help='type from date in year(yyyy), month, and day')
# self.aparser.add_argument('--dto', nargs='*', type=int, help='type to date in year(yyyy), month, and day')
# self.aparser.add_argument('--product', nargs='?', type=str)
# self.aparser.add_argument('--type', nargs='?', type=str)
# self.aparser.add_argument('--expiry', nargs='?', type=str)
# self.task_args = self.aparser.parse_args()
# self.data = self.get_data()