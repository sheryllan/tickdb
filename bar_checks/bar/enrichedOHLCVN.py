import argparse
import os
from collections import namedtuple

from influxdb import DataFrameClient

from bar.quantdb1_config import *
from bar.timeseries_check import *
from holidaycalendar import *
from influxcommon import *
from xmlconverter import *

Fields = EnrichedOHLCVN.Fields
Tags = EnrichedOHLCVN.Tags
BarId = namedtuple('BarId', Tags.__members__.keys())


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

    CHECK_COLS = [OK_HIGH_LOW, OK_PCLV_ORDER, OK_BID_ASK, OK_NET_VOLUME,
                  OK_VOL_ON_LV1, OK_VOL_ON_LV2, OK_VOL_ON_LV3]

    @classmethod
    def check_high_low(cls, df):
        hask, high, hbid = df[Fields.HASK], df[Fields.HIGH], df[Fields.HBID]
        lask, low, lbid = df[Fields.LASK], df[Fields.LOW], df[Fields.LBID]

        df[cls.OK_HIGH_LOW] = (hask >= high) & (high >= low) & (low >= lbid)
        return df

    @classmethod
    def check_pclv_order(cls, df):
        cask1, cask2, cask3 = df[Fields.CASK1], df[Fields.CASK2], df[Fields.CASK3]
        cbid1, cbid2, cbid3 = df[Fields.CBID1], df[Fields.CBID2], df[Fields.CBID3]

        df[cls.OK_PCLV_ORDER] = (cask3 > cask2) & (cask2 > cask1) & (cbid1 > cbid2) & (cbid2 > cbid3)
        return df

    @classmethod
    def check_bid_ask(cls, df):
        cask1, cask2, cask3 = df[Fields.CASK1], df[Fields.CASK2], df[Fields.CASK3]
        cbid1, cbid2, cbid3 = df[Fields.CBID1], df[Fields.CBID2], df[Fields.CBID3]
        hask, lask = df[Fields.HASK], df[Fields.LASK]
        hbid, lbid = df[Fields.HBID], df[Fields.LBID]

        df[cls.OK_BID_ASK] = (cask1 > cbid1) & (cask2 > cbid2) & (cask3 > cbid3) & (hask > hbid) & (lask > lbid)
        return df

    @classmethod
    def check_net_volume(cls, df):
        df[cls.OK_NET_VOLUME] = df[Fields.TBUYV] - df[Fields.TSELLV] == df[Fields.NET_VOLUME]
        return df

    @classmethod
    def check_vol_on_lv(cls, df):
        df[cls.OK_VOL_ON_LV1] = ~((df[Fields.CASK1] != 0) ^ (df[Fields.CASKV1] != 0))
        df[cls.OK_VOL_ON_LV2] = ~((df[Fields.CASK2] != 0) ^ (df[Fields.CASKV2] != 0))
        df[cls.OK_VOL_ON_LV3] = ~((df[Fields.CASK3] != 0) ^ (df[Fields.CASKV3] != 0))
        return df


class SeriesChecker(object):

    ERRORTYPE = 'error_type'
    ERRORVAL = 'error_value'

    DATE = 'date'  # attribute
    TIMEZONE = 'timezone'  # attribute

    GAPS = 'gaps'  # tag
    REVERSIONS = 'reversions'  # tag
    INVALIDS = 'invalids'  # tag

    BAR = 'bar'  # tag

    TIMESTAMP = 'timestamp'  # tag
    START_TS = 'start_ts'  # attribute
    END_TS = 'end_ts'  # attribute
    INDEX = 'index'  # attribute
    LOC = 'loc'  # attribute

    OFFSET_MAPPING = {'M': offsets.Minute}

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def validate(self, data, window, closed):
        for tz, tz_df in data.groupby(lambda x: x.tz):
            start_date, end_date = tz_df.index[0].date(), tz_df.index[-1].date()
            schedules = list(self.scheduler.get_schedules(start_date, end_date, *window, tz))
            for (clock_type, width), bars_df in tz_df.groupby([Tags.CLOCK_TYPE, Tags.WIDTH]):
                step, unit = width, self.OFFSET_MAPPING[clock_type]
                tsgenerator = StepTimestampGenerator(schedules, step, unit, closed=closed)
                validation = KnownTimestampValidation(tsgenerator)
                for barid, barid_df in map(lambda x: (BarId(*x[0]), x[1]), bars_df.groupby(Tags.values())):
                    ts_dict = {d: list(ts) for d, ts in groupby(barid_df.index, lambda x: x.date())}
                    for schedule in schedules:
                        date = schedule[0].date()
                        if date not in ts_dict:
                            invalids, reversions = [], []
                            gaps = [{self.TIMESTAMP: {self.START_TS: schedule[0], self.END_TS: schedule[1]}}]
                        else:
                            validation.tsgenerator.offset = barid.OFFSET
                            validation.timestamps = ts_dict[date]
                            invalids = [(self.TIMESTAMP, ts) for ts in validation.invalids()]
                            gaps = [{self.TIMESTAMP: {self.START_TS: tsrange[0], self.END_TS: tsrange[-1]}}
                                    for tsrange in validation.gaps(date)]
                            reversions = [{self.TIMESTAMP: [{self.LOC: loc, self.INDEX: idx}, ts]}
                                          for ts, loc, idx in validation.reversions(date)]

                        record = {self.DATE: str(date), self.TIMEZONE: tz._tzname, self.BAR: barid}
                        if invalids:
                            yield {**record, self.ERRORTYPE: self.INVALIDS, self.ERRORVAL: invalids}
                        if gaps:
                            yield {**record, self.ERRORTYPE: self.GAPS, self.ERRORVAL: gaps}
                        if reversions:
                            yield {**record, self.ERRORTYPE: self.REVERSIONS, self.ERRORVAL: reversions}

    def to_dated_results(self, data):
        keys = [lambda x: (x[self.DATE], x[self.TIMEZONE]),
                lambda x: x[self.ERRORTYPE],
                lambda x: x[self.BAR]]
        sort_keys = [True, False, False]
        itemfunc = lambda x: x[self.ERRORVAL]

        dated = hierarchical_group_by(data, keys, itemfunc, sort_keys)
        for d in dated:
            for errortype in dated[d]:
                dated[d][errortype] = [{self.BAR: [bar._asdict(), dated[d][errortype][bar]]}
                                       for bar in dated[d][errortype]]
            yield {self.DATE: d[0], self.TIMEZONE: d[1], **dated[d]}


class CheckTask(object):
    REPORT = 'report'  # tag
    START_DATE = 'start_date'  # attribute
    END_DATE = 'end_date'  # attribute
    START_TIME = 'start_time'  # attribute
    END_TIME = 'end_time'  # attribute

    RECORD = 'record'  # tag
    BAR = 'bar'  # tag
    WINDOW_FMT = '%H:%M'

    def __init__(self, settings):
        self.client = DataFrameClient(host=HOSTNAME, port=PORT, database=DBNAME)
        self.scheduler = BScheduler(settings.calendar, (settings.open_time, settings.close_time),
                                    settings.tzinfo, settings.custom_schedule)
        self.schecker = SeriesChecker(self.scheduler)
        self.aparser = argparse.ArgumentParser()
        self.aparser.add_argument('--window', nargs='*', type=str, default=os.getenv('WINDOW', ('00:00', '22:00')),
                                  help='the check timeslot window, please define start/end time in mm:ss')
        self.aparser.add_argument('--closed', nargs='?', type=str, default=os.getenv('CLOSED', None),
                                  help='defines how the window will be closed: "left" or "right", defaults to None(both sides)')
        self.aparser.add_argument('--dfrom', nargs='*', type=int, default=(last_n_days().timetuple()[0:3]),
                                  help='the check start date in format (yyyy, mm, dd), defaults to yesterday')
        self.aparser.add_argument('--dto', nargs='*', type=int, default=(last_n_days(0).timetuple()[0:3]),
                                  help='the check end date in format (yyyy, mm, dd), defaults to today')
        self.task_args = self.aparser.parse_args()

    @property
    def task_window(self):
        window = self.task_args.window
        if any(not isinstance(x, str) for x in window):
            raise ValueError('The time in window argument must all be string')

        return dt.datetime.strptime(window[0], self.WINDOW_FMT).time(), \
               dt.datetime.strptime(window[1], self.WINDOW_FMT).time()

    @property
    def task_closed(self):
        return self.task_args.closed

    @property
    def task_dfrom(self):
        dfrom = self.task_args.dfrom
        if isinstance(dfrom, dt.date):
            return dfrom
        elif isinstance(dfrom, (dt.datetime, pd.Timestamp)):
            return dfrom.date()
        elif isinstance(dfrom, tuple):
            return dt.date(*dfrom)
        else:
            raise TypeError('Invalid dfrom: must be (yyyy, mm, dd) or a datetime.date/datetime/pandas.Timestamp object')

    @property
    def task_dto(self):
        dto = self.task_args.dto
        if isinstance(dto, dt.date):
            return dto
        elif isinstance(dto, (dt.datetime, pd.Timestamp)):
            return dto.date()
        elif isinstance(dto, tuple):
            return dt.date(*dto)
        else:
            raise TypeError('Invalid dfrom: must be (yyyy, mm, dd) or a datetime.date/datetime/pandas.Timestamp object')


    @property
    def task_report_etree(self):
        return rcsv_addto_etree({self.START_DATE: self.task_dfrom,
                                 self.END_DATE: self.task_dto},
                                self.REPORT)

    def get_data(self, time_from, product=None, ptype=None, time_to=None, expiry=None):
        fields = {Tags.PRODUCT: product, Tags.TYPE: ptype, Tags.EXPIRY: expiry}
        terms = format_arith_terms(fields)
        qstring = select_where_time_bound(EnrichedOHLCVN.name(), time_from, time_to, where_terms=terms)
        return self.client.query(qstring).get(EnrichedOHLCVN.name(), None)

    def run_bar_checks(self, data):
        BarChecker.check_high_low(data)
        BarChecker.check_pclv_order(data)
        BarChecker.check_bid_ask(data)
        BarChecker.check_net_volume(data)
        BarChecker.check_vol_on_lv(data)

        return data[~data[BarChecker.CHECK_COLS].all(axis=1)]

    def bar_checks_xml(self, data, xsl, outpath=None):
        results = self.run_bar_checks(data)

        xml = self.task_report_etree
        for bar, bar_df in results.groupby(Tags.values()):
            root = rcsv_addto_etree(BarId(*bar)._asdict(), self.BAR)
            xml.append(df_to_xmletree(root, self.RECORD, bar_df[BarChecker.CHECK_COLS], TIME_IDX))

        if outpath is not None:
            with open(outpath, mode='wt+') as fh:
                to_xslstyle_xml(xml, xsl, fh)

        return xml

    def run_timeseries_checks(self, data):
        window, closed = self.task_window, self.task_closed
        return self.schecker.validate(data, window, closed)

    def timeseries_checks_xmls(self, data, xsl, outpath=None):
        xml = self.task_report_etree
        window = self.task_window
        xml.set(self.START_TIME, str(window[0]))
        xml.set(self.END_TIME, str(window[1]))
        results = self.run_timeseries_checks(data)
        for record in self.schecker.to_dated_results(results):
            xml.append(rcsv_addto_etree(record, self.RECORD))

        if outpath is not None:
            with open(outpath, mode='wt+') as fh:
                to_xslstyle_xml(xml, xsl, fh)
        return xml

    def set_taskargs(self, **kwargs):
        self.task_args = self.aparser.parse_args()
        for kw in kwargs:
            self.task_args.__dict__[kw] = kwargs[kw]

    def run_checks(self, product=None, ptype=None, expiry=None, **kwargs):
        self.set_taskargs(**kwargs)
        data = self.get_data(self.task_dfrom, product, ptype, self.task_dto, expiry)
        if data is None:
            msg = {'product': product, 'ptype': ptype, 'expiry': expiry,
                   'dfrom': self.task_dfrom, 'dto': self.task_dfrom}
            raise ValueError('No data selected for {}'.format({k: v for k, v in msg.items()}))

        self.bar_checks_xml(data, 'xsl', 'bar_checks.xml')
        self.timeseries_checks_xmls(data, 'xsl', 'timeseries_checks.xml')


if __name__ == '__main__':
    c = CheckTask(CMESchedule)
    c.run_checks('CL', 'F', closed='right', dfrom=dt.date(2018, 6, 1))
    # c.run_checks('CL', 'F', closed='right', dfrom=dt.date(2018, 6, 1), dto=dt.date(2018, 6, 23))

# self.aparser = argparse.ArgumentParser()
# self.aparser.add_argument('--dfrom', nargs=3, type=int, help='type from date in year(yyyy), month, and day')
# self.aparser.add_argument('--dto', nargs='*', type=int, help='type to date in year(yyyy), month, and day')
# self.aparser.add_argument('--product', nargs='?', type=str)
# self.aparser.add_argument('--type', nargs='?', type=str)
# self.aparser.add_argument('--expiry', nargs='?', type=str)
# self.task_args = self.aparser.parse_args()
# self.data = self.get_data()
