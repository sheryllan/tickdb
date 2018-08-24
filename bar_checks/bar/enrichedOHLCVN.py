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
BarId = namedtuple('BarId', Tags.all())


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

    OUTCOLS = Tags.all() + CHECK_COLS

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
    BAR = 'bar'

    DATETZ = 'date_timezone'
    DATE = 'date'
    TIMEZONE = 'timezone'

    GAPS = 'gaps'
    REVERSIONS = 'reversions'
    INVALID = 'invalid'

    TIMESTAMP = 'timestamp'
    START_TS = 'start_ts'
    END_TS = 'end_ts'

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def check_gaps_reversions(self, data, window, closed):
        for barid, records in map(lambda x: (BarId(*x[0])._asdict(), x[1]), data.groupby(Tags.all())):
            for tz, tz_records in records.groupby(lambda x: x.tz):
                dated_records = records.groupby(lambda x: x.date())
                dated_indices = list(dated_records.groups)
                start_date, end_date = dated_indices[0], dated_indices[-1]
                for schedule in self.scheduler.get_schedules(start_date, end_date, *window, tz=tz):
                    sdate = schedule[0].date()
                    start_ts, end_ts = schedule[0], schedule[1]
                    offset, interval, unit = barid[Tags.OFFSET], barid[Tags.WIDTH], offsets.Minute

                    timestamps = dated_records.groups.get(sdate, None)
                    valid, invalid = validate_timestamps(timestamps, start_ts, end_ts, offset, interval, closed=closed)
                    reversions = [{self.TIMESTAMP: ts} for ts in check_time_reversion(valid)]
                    gaps = [{self.START_TS: tsrange[0], self.END_TS: tsrange[-1]}
                            for tsrange in check_intraday_gaps(valid)]

                    yield (barid,
                           {self.DATETZ: {
                               self.DATE: str(sdate),
                               self.TIMEZONE: tz._tzname,
                               self.GAPS: gaps,
                               self.REVERSIONS: reversions,
                               self.INVALID: invalid
                           }})

    def norm_results(self, rdata):
        results = dict()
        for barid_dict, dated_erros in rdata:
            barid = BarId(**barid_dict)
            if barid in results:
                for key in dated_erros:
                    if key in results[barid]:
                        results[barid][key].append(dated_erros[key])
                    else:
                        results[barid][key] = [dated_erros[key]]
            else:
                values = {[dated_erros[k]] for k in dated_erros}
                results[barid] = {**barid, **values}

        return results.values()


        # def rcsv_norm(target_dict, source_dict):
        #     if not isinstance(source_dict, dict):
        #         return source_dict
        #     elif not isinstance(target_dict, dict):
        #         raise ValueError('target_dict value depth is inconsistent with source_dict')
        #
        #     for k in source_dict:
        #         if k not in target_dict:
        #             target_dict[k] = source_dict[k]
        #         else:
        #             target_dict.update({k: rcsv_norm(target_dict[k], source_dict[k])})
        #
        # results = {}
        # for rd in rdata:
        #     rcsv_norm(results, rd)
        #
        # return results


class CheckTask(object):
    RESULTS = 'results'
    BAR = 'bar'
    WINDOW_FMT = '%H:%M'

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
        self.task_args = self.aparser.parse_args()

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
        return self.schecker.norm_results(self.schecker.check_gaps_reversions(data, window, closed))

    def timeseries_checks_xml(self, data, xsl, outpath=None):
        results = self.run_timeseries_checks(data)
        xml = dicts_to_xmletree(results, self.RESULTS, self.BAR)

        if outpath is not None:
            with open(outpath, mode='wt+') as fh:
                to_xslstyle_xml(xml, xsl, fh)

        return xml

    def set_taskargs(self, **kwargs):
        self.task_args = self.aparser.parse_args()
        for kw in kwargs:
            if kw in self.task_args.__dict__:
                self.task_args.__dict__[kw] = kwargs[kw]

    def run_checks(self, dfrom, product=None, ptype=None, dto=None, expiry=None, **kwargs):
        data = self.get_data(dfrom, product, ptype, dto, expiry)
        if data is None:
            msg = {'product': product, 'ptype': ptype, 'expiry': expiry, 'dfrom': dfrom, 'dto': dto}
            raise ValueError('No data selected for {}'.format({k: v for k, v in msg.items()}))

        self.set_taskargs(**kwargs)

        self.timeseries_checks_xml(data, '')

        self.bar_checks_xml(data, '')


if __name__ == '__main__':
    c = CheckTask(CMESchedule)
    c.run_checks(date(2018, 6, 1), 'CL', 'F', closed='right')


# self.aparser = argparse.ArgumentParser()
# self.aparser.add_argument('--dfrom', nargs=3, type=int, help='type from date in year(yyyy), month, and day')
# self.aparser.add_argument('--dto', nargs='*', type=int, help='type to date in year(yyyy), month, and day')
# self.aparser.add_argument('--product', nargs='?', type=str)
# self.aparser.add_argument('--type', nargs='?', type=str)
# self.aparser.add_argument('--expiry', nargs='?', type=str)
# self.task_args = self.aparser.parse_args()
# self.data = self.get_data()
