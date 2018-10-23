import argparse
from collections import namedtuple
from types import MappingProxyType

from influxdb import DataFrameClient

from bar.taskconfig import *
from bar.quantdb1_config import *
from dfutils import *
from timeutils import *
from schedulefactory import *
from influxcommon import *
from xmlconverter import *

import premailer
from getpass import getpass
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging

Fields = EnrichedOHLCVN.Fields
Tags = EnrichedOHLCVN.Tags


class BarId(namedtuple('BarId', Tags.__members__.keys())):
    def __init__(self, *args, **kwargs):
        self.id = self.__hash__()

    def _asdict(self):
        return {**super()._asdict(), 'id': self.id}


class CheckInfo(Mapping):
    SUMMARY = 'summary'
    DETAIL = 'detail'
    PASSED = 'passed'
    FAILED = 'failed'
    WARNING = 'warning'

    def __init__(self, detail, caveat='', pass_msg='', no_warning=''):
        self.passed = detail == pass_msg
        info = {self.SUMMARY: self.PASSED} if self.passed else {self.SUMMARY: self.FAILED, self.DETAIL: detail}

        self.warning_free = caveat == no_warning
        if not self.warning_free:
            info.update({self.WARNING: caveat})

        self._data = MappingProxyType(info)

    def __getitem__(self, key):
        return self._data[key]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        return self._data.__repr__()

    def __str__(self):
        return self._data.__str__()

    def __bool__(self):
        return self.passed and self.warning_free


class BarChecker(object):
    # region check columns
    HIGH_LOW_CHECK = 'high_low_check'
    PCLV_ORDER_CHECK = 'pclv_order_check'
    BID_ASK_CHECK = 'bid_ask_check'
    VOLUME_CHECK = 'volume_check'
    VOL_ON_LV1_CHECK = 'vol_on_lv1_check'
    VOL_ON_LV2_CHECK = 'vol_on_lv2_check'
    VOL_ON_LV3_CHECK = 'vol_on_lv3_check'
    PRICES_ROLLOVER_CHECK = 'prices_rollover_check'
    VWAP_CHECK = 'vwap_check'
    # endregion

    CHECK_COLS = [PRICES_ROLLOVER_CHECK, HIGH_LOW_CHECK, PCLV_ORDER_CHECK, BID_ASK_CHECK, VOLUME_CHECK,
                  VOL_ON_LV1_CHECK, VOL_ON_LV2_CHECK, VOL_ON_LV3_CHECK, VWAP_CHECK]

    SUMMARY = 'summary'
    DETAIL = 'detail'
    PASSED = 'passed'
    FAILED = 'failed'
    WARNING = 'warning'

    @classmethod
    def to_na_msg(cls, df_na, na_msg_suffix='undefined', notna_msg=''):
        return df_na.astype(str).apply(lambda col: col.map(
            lambda x: ' '.join([col.name, na_msg_suffix]) if x == 'True' else notna_msg),
            result_type='broadcast')

    @classmethod
    def map_check_state(cls, details, caveats=None, pass_msg='', name=None):
        df_tmp = details.to_frame('0')
        if caveats is not None:
            df_tmp['1'] = caveats

        mapped = df_tmp.apply(lambda x: CheckInfo(*x, pass_msg=pass_msg), axis=1)
        mapped.name = name
        return mapped

    @classmethod
    def get_check_info(cls, df, expressions, errmsgs, validate_na=True, name=None, excl_cols=None):
        df_isna = pd.DataFrame(index=df.index)
        excl_cols = [] if excl_cols is None else to_iter(excl_cols)
        rcol = 'rcol'

        def evaluate():
            for e in expressions:
                df_eval = vector_eval(df, rcol, *e)
                cols_orig = [c for c in df_eval.columns if c != rcol and c not in excl_cols]
                yield cols_orig, df_eval[rcol]

        def mask(cols):
            cols_new = [c for c in cols if c not in df_isna]
            df_isna[cols_new] = pd.isna(df[cols_new])
            return df_isna[cols].any(axis=1) if validate_na else None

        dvectors = [vector_map(v, map_false=msg, mask=mask(c), mask_value=np.nan)
                    for (c, v), msg in zip(evaluate(), errmsgs)]
        details = vectors_join(dvectors)

        caveats = vectors_join(cls.to_na_msg(df_isna)) if validate_na else None
        return cls.map_check_state(details, caveats, name=name)


    @classmethod
    def nullify_undefined(cls, df):
        df = df.replace(UNDEFINED, np.nan)
        lv_volumes = [Fields.CASKV1, Fields.CBIDV1, Fields.CASKV2, Fields.CBIDV2, Fields.CASKV3, Fields.CBIDV3]
        prices = [Fields.CASK1, Fields.CBID1, Fields.CASK2, Fields.CBID2, Fields.CASK3, Fields.CBID3,
                  Fields.OPEN, Fields.CLOSE, Fields.HIGH, Fields.CLOSE,
                  Fields.HIGH, Fields.HASK, Fields.HBID,
                  Fields.LOW, Fields.LASK, Fields.LBID]

        cols_zero_nan = lv_volumes + prices
        df[cols_zero_nan] = df[cols_zero_nan].replace(0, np.nan)
        return df

    @classmethod
    def check_high_low(cls, df):
        hask, high, hbid = Fields.HASK, Fields.HIGH, Fields.HBID
        lask, low, lbid = Fields.LASK, Fields.LOW, Fields.LBID
        volume = Fields.VOLUME

        expressions = [(high, '>=', low),
                       split_expr(chained_expr(notna_expr(volume), single_comp_expr(hask, high, '>='), '|')),
                       split_expr(chained_expr(notna_expr(volume), single_comp_expr(low, lbid, '>='), '|'))]
        errmsgs = ['high < low',
                   'hask < high',
                   'low < lbid']

        return cls.get_check_info(df, expressions, errmsgs, True, cls.HIGH_LOW_CHECK, volume)

    @classmethod
    def check_pclv_order(cls, df):
        cask1, cask2, cask3 = Fields.CASK1, Fields.CASK2, Fields.CASK3
        cbid1, cbid2, cbid3 = Fields.CBID1, Fields.CBID2, Fields.CBID3

        expressions = [(cask3, '>', cask2, '>', cask1),
                       (cbid1, '>', cbid2, '>', cbid3)]
        errmsgs = ['cask3 <= cask2 <= cask1',
                   'cbid1 <= cbid2 <= cbid3']

        return cls.get_check_info(df, expressions, errmsgs, True, cls.PCLV_ORDER_CHECK)

    @classmethod
    def check_bid_ask(cls, df):
        cask1, cask2, cask3 = Fields.CASK1, Fields.CASK2, Fields.CASK3
        cbid1, cbid2, cbid3 = Fields.CBID1, Fields.CBID2, Fields.CBID3
        hask, lask = Fields.HASK, Fields.LASK
        hbid, lbid = Fields.HBID, Fields.LBID

        expressions = [(cask1, '>', cbid1),
                       (cask2, '>', cbid2),
                       (cask3, '>', cbid3),
                       (hask, '>', hbid),
                       (lask, '>', lbid)]
        errmsgs = ['cask1 <= cbid1',
                   'cask2 <= cbid2',
                   'cask3 <= cbid3',
                   'hask <= hbid',
                   'lask <= lbid']

        return cls.get_check_info(df, expressions, errmsgs, True, cls.BID_ASK_CHECK)

    @classmethod
    def check_volume(cls, df):
        tbuyv, tsellv, net_volume, volume = Fields.TBUYV, Fields.TSELLV, Fields.NET_VOLUME, Fields.VOLUME

        cols = [tbuyv, tsellv, net_volume, volume]
        df[cols].replace(np.nan, 0)

        expressions = [(tbuyv, '-', tsellv, '==', net_volume),
                       (tbuyv, '+', tsellv, '<=', volume)]

        errmsgs = ['tbuyv - tsellv != net_volume',
                   'tbuyv + tsellv > volume']

        return cls.get_check_info(df, expressions, errmsgs, True, cls.VOLUME_CHECK)

    @classmethod
    def check_vol_on_lv(cls, df):
        cask1, caskv1, cbid1, cbidv1 = Fields.CASK1, Fields.CASKV1, Fields.CBID1, Fields.CBIDV1
        cask2, caskv2, cbid2, cbidv2 = Fields.CASK2, Fields.CASKV2, Fields.CBID2, Fields.CBIDV2
        cask3, caskv3, cbid3, cbidv3 = Fields.CASK3, Fields.CASKV3, Fields.CBID3, Fields.CBIDV3

        expressions1 = [split_expr(chained_expr(notna_expr(cask1), isna_expr(caskv1), '|')),
                        split_expr(chained_expr(isna_expr(cask1), notna_expr(caskv1), '|')),
                        split_expr(chained_expr(notna_expr(cbid1), isna_expr(cbidv1), '|')),
                        split_expr(chained_expr(isna_expr(cbid1), notna_expr(cbidv1), '|'))]
        expressions2 = [split_expr(chained_expr(notna_expr(cask2), isna_expr(caskv2), '|')),
                        split_expr(chained_expr(isna_expr(cask2), notna_expr(caskv2), '|')),
                        split_expr(chained_expr(notna_expr(cbid2), isna_expr(cbidv2), '|')),
                        split_expr(chained_expr(isna_expr(cbid2), notna_expr(cbidv2), '|'))]
        expressions3 = [split_expr(chained_expr(notna_expr(cask3), isna_expr(caskv3), '|')),
                        split_expr(chained_expr(isna_expr(cask3), notna_expr(caskv3), '|')),
                        split_expr(chained_expr(notna_expr(cbid3), isna_expr(cbidv3), '|')),
                        split_expr(chained_expr(isna_expr(cbid3), notna_expr(cbidv3), '|'))]

        errmsgs1 = ['cask1 defined, caskv1 = 0/undefined',
                    'cask1 undefined, caskv1 defined',
                    'cbid1 defined, cbidv1 = 0/undefined',
                    'cbid1 undefined, cbidv1 defined']

        errmsgs2 = ['cask2 defined, caskv2 = 0/undefined',
                    'cask2 undefined, caskv2 defined',
                    'cbid2 defined, cbidv2 = 0/undefined',
                    'cbid2 undefined, cbidv2 defined']

        errmsgs3 = ['cask3 defined, caskv3 = 0/undefined',
                    'cask3 undefined, caskv3 defined',
                    'cbid3 defined, cbidv3 = 0/undefined',
                    'cbid3 undefined, cbidv3 defined']

        values = [cls.get_check_info(df, expressions1, errmsgs1, False, cls.VOL_ON_LV1_CHECK),
                  cls.get_check_info(df, expressions2, errmsgs2, False, cls.VOL_ON_LV2_CHECK),
                  cls.get_check_info(df, expressions3, errmsgs3, False, cls.VOL_ON_LV3_CHECK)]
        return pd.concat(values, axis=1)

    @classmethod
    def check_prices_rollover(cls, df):
        volume = Fields.VOLUME
        fopen, fclose, fhigh, flow = Fields.OPEN, Fields.CLOSE, Fields.HIGH, Fields.LOW
        errmsgs = pd.Series(['open not rolled over', 'close not rolled over',
                             'high not rolled over', 'low not rolled over'])

        def check():
            for date, gdf in df.groupby(lambda x: x.date()):
                prev = gdf.iloc[0]
                for i, row in gdf.iterrows():
                    if na_equal(row[volume], 0):
                        cols = [not na_equal(row[fopen], prev[fclose]),
                                not na_equal(row[fclose], prev[fclose]),
                                not na_equal(row[fhigh], prev[fclose]),
                                not na_equal(row[flow], prev[fclose])]
                    else:
                        prev = row
                        cols = [na_equal(row[fopen], 0),
                                na_equal(row[fclose], 0),
                                na_equal(row[fhigh], 0),
                                na_equal(row[flow], 0)]

                    yield '\n'.join(errmsgs[cols])

        return cls.map_check_state(pd.Series(check(), df.index), name=cls.PRICES_ROLLOVER_CHECK)

    @classmethod
    def check_vwap(cls, df):
        tbuyv, tbuyvwap, tsellv, tsellvwap = Fields.TBUYV, Fields.TBUYVWAP, Fields.TSELLV, Fields.TSELLVWAP

        cols = [tbuyv, tbuyvwap, tsellv, tsellvwap]
        df = df[cols].replace(0, np.nan)

        expressions = [split_expr(chained_expr(isna_expr(tbuyv), isna_expr(tbuyvwap), '==')),
                       split_expr(chained_expr(isna_expr(tsellv), isna_expr(tsellvwap), '=='))]
        errmsgs = ['tbuyvwap undefined',
                   'tsellvwap undefined']

        return cls.get_check_info(df, expressions, errmsgs, False, cls.VWAP_CHECK)


class SeriesChecker(object):
    ERRORTYPE = 'error_type'
    ERRORVAL = 'error_value'

    DATE = 'date'  # attribute
    TIMEZONE = 'timezone'  # attribute

    BAR = 'bar'  # tag
    GAPS = 'gaps'  # tag
    REVERSIONS = 'reversions'  # tag
    INVALIDS = 'invalids'  # tag
    DETAIL = 'detail'  # tag

    TIMESTAMP = 'timestamp'  # attribute
    START_TS = 'start_ts'  # attribute
    END_TS = 'end_ts'  # attribute
    INDEX = 'index'  # attribute
    LOC = 'loc'  # attribute

    OFFSET_MAPPING = {'M': offsets.Minute}

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def validate(self, data, window, closed, tzinfo=None):
        for tz, tz_df in data.groupby(lambda x: x.tz):
            start_date, end_date = tz_df.index[0].date(), tz_df.index[-1].date()
            schedules = list(self.scheduler.get_schedules(start_date, end_date, *window, tz if tzinfo is None else tzinfo))
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
                            gaps = [{self.DETAIL: {self.START_TS: schedule[0], self.END_TS: schedule[1]}}]
                        else:
                            validation.tsgenerator.offset = barid.OFFSET
                            validation.timestamps = ts_dict[date]
                            invalids = [{self.DETAIL: {self.TIMESTAMP: ts}} for ts in validation.invalids()]
                            gaps = [{self.DETAIL: {self.START_TS: tsrange[0], self.END_TS: tsrange[-1]}}
                                    for tsrange in validation.gaps(date)]
                            reversions = [{self.DETAIL: {self.TIMESTAMP: ts, self.LOC: loc, self.INDEX: idx}}
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
    START_DT = 'start'  # attribute
    END_DT = 'end'  # attribute

    MISSING_PRODS = 'missing_products'  # tag
    PRODUCT = 'product'  # tag

    START_TIME = 'start_time'  # attribute
    END_TIME = 'end_time'  # attribute

    RECORD = 'record'  # tag
    BAR = 'bar'  # tag
    WINDOW_FMT = '%H:%M'

    def __init__(self):
        self.client = DataFrameClient(host=HOSTNAME, port=PORT, database=DBNAME)
        self._schedule = None
        self.schecker = None
        self.aparser = argparse.ArgumentParser()

        self.aparser.add_argument('--product', nargs='*', type=str,
                                  help='the product(s) for checking, all if not set')
        self.aparser.add_argument('--ptype', nargs='*', type=str,
                                  help='the product type(s) for checking, all if not set')
        self.aparser.add_argument('--expiry', nargs='*', type=str,
                                  help='the expiry(ies) for checking, all if not set')

        self.aparser.add_argument('--schedule', nargs='*', type=str, default=SCHEDULE,
                                  help='the schedule name(or and the refdata file) for the timeseries check')
        self.aparser.add_argument('--window', nargs='*', type=str, default=WINDOW,
                                  help='the check timeslot window, please define start/end time in mm:ss')
        self.aparser.add_argument('--closed', nargs='?', type=str,
                                  help="""defines how the window will be closed: "left" or "right", 
                                  defaults to None(both sides)""")
        self.aparser.add_argument('--dtfrom', nargs='*', type=int, default=(last_n_days().timetuple()[0:3]),
                                  help='the check start date in format (yyyy, mm, dd), defaults to yesterday')
        self.aparser.add_argument('--dtto', nargs='*', type=int, default=(last_n_days(0).timetuple()[0:3]),
                                  help='the check end date in format (yyyy, mm, dd), defaults to today')
        self.aparser.add_argument('--timezone', nargs='*', type=str, default=TIMEZONE,
                                  help='the timezone for the check to run')

        self.aparser.add_argument('--barxml', nargs='?', type=str,
                                  help='the xml output path of bar check')
        self.aparser.add_argument('--barxsl', nargs='?', type=str,
                                  default=BARXSL,
                                  help='the path of xsl file for styling the bar check output xml')
        self.aparser.add_argument('--tsxml', nargs='?', type=str,
                                  help='the xml output path of timeseries check')
        self.aparser.add_argument('--tsxsl', nargs='?', type=str,
                                  default=TSXSL,
                                  help='the path of xsl file for styling the timeseries check output xml')
        self.aparser.add_argument('--barhtml', nargs='?', type=str,
                                  help='the html output path of bar check after xsl transformation')
        self.aparser.add_argument('--tshtml', nargs='?', type=str,
                                  help='the html output path of timeseries check after xsl transformation')

        self.aparser.add_argument('--sender', nargs='?', type=str, default=SENDER,
                                  help='the email address of sender')
        self.aparser.add_argument('--recipients', nargs='*', type=str, default=RECIPIENTS,
                                  help='the email address of recipients')

        self.set_taskargs()

    @property
    def task_product(self):
        return self.task_args.product

    @property
    def task_ptype(self):
        return self.task_args.ptype

    @property
    def task_expiry(self):
        return self.task_args.expiry

    @property
    def task_window(self):
        window = self.task_args.window
        if any(not isinstance(x, str) for x in window):
            raise ValueError('The time in window argument must all be string')

        return dt.datetime.strptime(window[0], self.WINDOW_FMT).time(), \
               dt.datetime.strptime(window[1], self.WINDOW_FMT).time()

    @property
    def task_closed(self):
        if self.task_args.closed not in ['left', 'right', None]:
            raise ValueError('The value for argument "closed" must be "left"/"right", or if not set, None by default')
        return self.task_args.closed

    @property
    def task_dtfrom(self):
        dtfrom = self.task_args.dtfrom
        if isinstance(dtfrom, tuple):
            dtfrom = pd.Timestamp(*dtfrom)

        if isinstance(dtfrom, (dt.date, dt.datetime)):
            dtfrom = pd.Timestamp(dtfrom)
            return self.task_timezone.localize(dtfrom) if self.task_timezone is not None else dtfrom
        else:
            raise TypeError('Invalid dfrom: must be (yyyy, mm, dd) or a datetime.date/datetime/pandas.Timestamp object')

    @property
    def task_dtto(self):
        dtto = self.task_args.dtto
        if isinstance(dtto, tuple):
            dtto = pd.Timestamp(*dtto)

        if isinstance(dtto, (dt.date, dt.datetime, tuple)):
            dtto = pd.Timestamp(dtto)
            return self.task_timezone.localize(dtto) if self.task_timezone is not None else dtto
        else:
            raise TypeError('Invalid dfrom: must be (yyyy, mm, dd) or a datetime.date/datetime/pandas.Timestamp object')

    @property
    def task_timezone(self):
        if self.task_args.timezone is None:
            return None
        return pytz.timezone(self.task_args.timezone)

    @property
    def task_report_etree(self):
        return rcsv_addto_etree({self.START_DT: self.task_dtfrom,
                                 self.END_DT: self.task_dtto},
                                self.REPORT)
    @property
    def task_barxml(self):
        return self.task_args.barxml

    @property
    def task_barxsl(self):
        return self.task_args.barxsl

    @property
    def task_tsxml(self):
        return self.task_args.tsxml

    @property
    def task_tsxsl(self):
        return self.task_args.tsxsl

    @property
    def task_barhtml(self):
        return self.task_args.barhtml

    @property
    def task_tshtml(self):
        return self.task_args.tshtml

    @property
    def task_sender(self):
        return self.task_args.sender

    @property
    def task_recipients(self):
        return self.task_args.recipients

    def get_data(self, time_from, product=None, ptype=None, time_to=None, expiry=None):
        fields = {Tags.PRODUCT: product, Tags.TYPE: ptype, Tags.EXPIRY: expiry}
        terms = format_arith_terms(fields)
        qstring = select_where_time_bound(EnrichedOHLCVN.name(), time_from, time_to, where_terms=terms)
        return self.client.query(qstring).get(EnrichedOHLCVN.name(), None)

    def run_bar_checks(self, data):
        data = BarChecker.nullify_undefined(data)
        data_cap = data.loc[if_idx_between_time(data, *self.task_window, self.task_closed, self.task_timezone)]
        checks = [
            BarChecker.check_prices_rollover(data)
            [if_idx_between_time(data, *self.task_window, self.task_closed, self.task_timezone)],
            BarChecker.check_high_low(data_cap),
            BarChecker.check_pclv_order(data_cap),
            BarChecker.check_bid_ask(data_cap),
            BarChecker.check_volume(data_cap),
            BarChecker.check_vol_on_lv(data_cap),
            BarChecker.check_vwap(data_cap)]

        results = pd.concat(checks, axis=1)

        return results[~results[BarChecker.CHECK_COLS].all(axis=1)]

    def bar_checks_xml(self, data, xsl=None, outpath=None):
        xml = self.task_report_etree
        prods = set(data[Tags.PRODUCT])
        missing_prods = [(self.PRODUCT, p) for p in to_iter(self.task_product) if p not in prods]
        if missing_prods:
            xml.append(rcsv_addto_etree(missing_prods, self.MISSING_PRODS))

        for bar, group in data.groupby(Tags.values()):
            root = rcsv_addto_etree(BarId(*bar)._asdict(), self.BAR)
            results = self.run_bar_checks(group)
            if not results.empty:
                xml.append(df_to_xmletree(root, self.RECORD, results[BarChecker.CHECK_COLS], TIME_IDX))

        if outpath is not None and xsl is not None:
            to_xsl_instructed_xml(xml, xsl, outpath)

        return xml

    def run_timeseries_checks(self, data):
        window, closed = self.task_window, self.task_closed
        return self.schecker.validate(data, window, closed, self.task_timezone)

    def timeseries_checks_xmls(self, data, xsl=None, outpath=None):
        xml = self.task_report_etree
        window = self.task_window
        xml.set(self.START_TIME, str(window[0]))
        xml.set(self.END_TIME, str(window[1]))
        results = self.run_timeseries_checks(data)
        for record in self.schecker.to_dated_results(results):
            xml.append(rcsv_addto_etree(record, self.RECORD))

        if outpath is not None and xsl is not None:
            to_xsl_instructed_xml(xml, xsl, outpath)

        return xml

    def set_schecker(self):
        if self._schedule != self.task_args.schedule:
            self._schedule = self.task_args.schedule
            settings = get_schedule(self._schedule) \
                if isinstance(self._schedule, str) else get_schedule(*self._schedule)

            scheduler = BScheduler(settings.calendar, (settings.open_time, settings.close_time),
                                   settings.tzinfo, settings.custom_schedule)
            self.schecker = SeriesChecker(scheduler)

    def set_taskargs(self, **kwargs):
        self.task_args = self.aparser.parse_args()
        for kw in kwargs:
            self.task_args.__dict__[kw] = kwargs[kw]

        self.set_schecker()

    def run_checks(self, **kwargs):  # TODO report the symbols if not found
        self.set_taskargs(**kwargs)
        data = self.get_data(self.task_dtfrom, self.task_product, self.task_ptype,
                             self.task_dtto, self.task_expiry)

        if data is not None:
            barxml = self.bar_checks_xml(data, self.task_barxsl, self.task_barxml)
            tsxml = self.timeseries_checks_xmls(data, self.task_tsxsl, self.task_tsxml)

            to_styled_xml(barxml, self.task_barxsl, self.task_barhtml)
            to_styled_xml(tsxml, self.task_tsxsl, self.task_tshtml)

        else:
            msg = {'product': self.task_product, 'ptype': self.task_ptype, 'expiry': self.task_expiry,
                   'dfrom': self.task_dtfrom, 'dto': self.task_dtfrom}
            logging.INFO('No data selected for {}'.format({k: v for k, v in msg.items() if v is not None}))

    def email(self, files, subjects):
        smtp = smtplib.SMTP_SSL('smtp.gmail.com')
        password = getpass('Enter the password for: '.format(self.task_sender))
        smtp.login(self.task_sender, password)

        for f, subject in zip(files, subjects):
            with open(f) as fh:
                html = fh.read()

            inline = premailer.transform(html)
            report = MIMEText(inline, 'html')

            msg = MIMEMultipart('alternative')
            msg['From'] = self.task_sender
            recipients = ', '.join(self.task_recipients)
            msg['To'] = recipients
            msg['Subject'] = subject
            msg.attach(report)
            smtp.sendmail(self.task_sender, recipients, msg.as_string())

        smtp.quit()


if __name__ == '__main__':
    task = CheckTask()
    # products = ['ZF', 'ZN', 'TN', 'ZB', 'UB', 'ES', 'NQ', 'YM', 'EMD', 'RTY', '6A', '6B', '6C', '6E', '6J', '6M', '6N',
    #             '6S', 'BTC', 'GC', 'SI', 'HG', 'CL', 'HO', 'RB']
    products = 'ES'
    task.run_checks(product=products, ptype='F', dtfrom=dt.date(2018, 6, 1), schedule='CMESchedule')
    # task.email([task.task_barhtml, task.task_tshtml], [BAR_TITILE, TS_TITLE])
    # c.run_checks('CL', 'F', closed='right', dfrom=dt.date(2018, 6, 1), dto=dt.date(2018, 6, 23))
