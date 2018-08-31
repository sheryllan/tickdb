from collections import namedtuple
import argparse
import os

from influxdb import DataFrameClient

from holidaycalendar import *
from bar.timeseries_check import *
from influxcommon import *
from bar.quantdb1_config import EnrichedOHLCVN

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

    OUTCOLS = Tags.values() + CHECK_COLS

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
    DATETIME = 'datetime'  # tag
    DATE = 'date'  # attribute
    TIMEZONE = 'timezone'  # attribute

    GAPS = 'gaps'  # tag
    REVERSIONS = 'reversions'  # tag
    INVALIDS = 'invalids'  # tag

    TIMESTAMP = 'timestamp'  # tag
    START_TS = 'start_ts'  # attribute
    END_TS = 'end_ts'  # attribute

    OFFSET_MAPPING = {'M': offsets.Minute}

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def check_gaps_reversions(self, data, window, closed):
        date_gdf = data.groupby(lambda x: (x.tz, x.date()))
        tzdate_groups = pd.Series(index=pd.MultiIndex.from_tuples(date_gdf.groups.keys()))
        tzdate_schedules, validation = {}, KnownTimestampValidation()

        for tz in tzdate_groups.index.unique(0):
            dates = tzdate_groups[tz].index
            tzdate_schedules[tz] = {
                s[0].date(): s for s in self.scheduler.get_schedules(dates[0], dates[-1], *window, tz=tz)}

        for (sdate, tz), date_df in date_gdf:
            schedule = tzdate_schedules[tz][sdate]
            validation.valid_timestamps = schedule
            for barid, bar_df in map(lambda x: (BarId(*x[0]), x[1]), date_df.groupby(Tags.values())):
                offset, interval = barid.OFFSET, barid.WIDTH
                unit = self.OFFSET_MAPPING[barid.CLOCK_TYPE]
                timestamps = dtrange_between_time(bar_df.index, *window, closed)

                validation.actual_timestamps = timestamps
                invalids = [(self.TIMESTAMP, ts) for ts in validation.invalids()]
                gaps = [{self.TIMESTAMP: {self.START_TS: tsrange[0], self.END_TS: tsrange[-1]}}
                            for tsrange in validation.gaps()]
                reversions = [{self.TIMESTAMP: ts} for ts in validation.reversions()]





        for barid, date_df in map(lambda x: (BarId(*x[0]), x[1]), data.groupby(Tags.values())):
            for tz, tz_records in date_df.groupby(lambda x: x.tz):
                date_gdf = date_df.groupby(lambda x: x.date())
                dated_indices = list(date_gdf.groups)
                start_date, end_date = dated_indices[0], dated_indices[-1]
                for schedule in self.scheduler.get_schedules(start_date, end_date, *window, tz=tz):
                    sdate = schedule[0].date()
                    start_ts, end_ts = schedule[0], schedule[1]
                    offset, interval = barid.OFFSET, barid.WIDTH
                    unit = self.OFFSET_MAPPING[barid.CLOCK_TYPE]

                    timestamps = dtrange_between_time(date_gdf.groups.get(sdate, None), *window, closed)
                    valid, invalid = validate_timestamps(timestamps, start_ts, end_ts, offset, interval, unit, closed)
                    invalid = [(self.TIMESTAMP, ts) for ts in invalid]
                    reversions = [(self.TIMESTAMP, ts) for ts in check_time_reversion(valid)]
                    gaps = [{self.TIMESTAMP: {self.START_TS: tsrange[0], self.END_TS: tsrange[-1]}}
                            for tsrange in check_intraday_gaps(valid)]

                    erros = {self.GAPS: gaps, self.REVERSIONS: reversions, self.INVALIDS: invalid}
                    erros = {k: v for k, v in erros.items() if v}

                    if erros:
                        yield (barid, {self.DATETIME: {self.DATE: str(sdate), self.TIMEZONE: tz._tzname, **erros}})

    def structure_results(self, data):
        grouped = {}
        for barid, records in data:
            if barid in grouped:
                grouped[barid].append(records)
            else:
                grouped[barid] = [records]

        for barid, records in grouped.items():
            yield [barid._asdict(), records]


class CheckTask(object):
    REPORT = 'report'  # tag
    START_DATE = 'start_date'  # attribute
    END_DATE = 'end_date'  # attribute
    START_TIME = 'start_time'  # attribute
    END_TIME = 'end_time'  # attribute

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
        window = self.task_window
        return rcsv_addto_etree({self.START_DATE: self.task_dfrom,
                                 self.END_DATE: self.task_dto,
                                 self.START_TIME: window[0],
                                 self.END_TIME: window[1]},
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
        xml = df_to_xmletree('timeseries', 'entry', results[BarChecker.OUTCOLS], TIME_IDX)

        if outpath is not None:
            with open(outpath, mode='wt+') as fh:
                to_xslstyle_xml(xml, xsl, fh)

        return xml

    def run_timeseries_checks(self, data):
        window, closed = self.task_window, self.task_closed
        return self.schecker.check_gaps_reversions(data, window, closed)

    def timeseries_checks_xmls(self, data, xsl, outpath=None):
        xml = self.task_report_etree
        results = self.run_timeseries_checks(data)
        for bar in self.schecker.structure_results(results):
            xml.append(rcsv_addto_etree(bar, self.BAR))

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

        self.timeseries_checks_xmls(data, 'xsl', 'check_results.xml')

        # self.bar_checks_xml(data, '')


if __name__ == '__main__':
    c = CheckTask(CMESchedule)
    c.run_checks('CL', 'F', closed='right', dfrom=date(2018, 6, 1))

# self.aparser = argparse.ArgumentParser()
# self.aparser.add_argument('--dfrom', nargs=3, type=int, help='type from date in year(yyyy), month, and day')
# self.aparser.add_argument('--dto', nargs='*', type=int, help='type to date in year(yyyy), month, and day')
# self.aparser.add_argument('--product', nargs='?', type=str)
# self.aparser.add_argument('--type', nargs='?', type=str)
# self.aparser.add_argument('--expiry', nargs='?', type=str)
# self.task_args = self.aparser.parse_args()
# self.data = self.get_data()
