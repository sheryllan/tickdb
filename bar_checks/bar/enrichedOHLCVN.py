import argparse
from collections import namedtuple
from types import MappingProxyType
from influxdb import DataFrameClient

from bar.taskconfig import *
from bar.dbconfig import *
from dataframeutils import *
from timeseriesutils import *
from schedulefactory import *
from influxcommon import *
from xmlconverter import *

import logging


def set_dbconfig(server):
    global Server, Enriched
    global Fields, Tags

    if server == 'quantdb1':
        Server = Quantdb1
        Enriched = Server.ENRICHEDOHLCVN
    elif server == 'quantsim1':
        Server = Quantsim1
        Enriched = Server.ENRICHEDOHLCVN

    Fields = Enriched.Fields
    Tags = Enriched.Tags


def create_BarId():

    class BarId(namedtuple('BarId', Tags.__members__.keys())):
        def __init__(self, *args, **kwargs):
            self.id = self.__hash__()

        def _asdict(self):
            return {**super()._asdict(), 'id': self.id}

    return BarId

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
        df = df.replace(Server.UNDEFINED, np.nan)
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
        self.BarId = create_BarId()

    def validate(self, data, window, closed, tzinfo=None):
        for tz, tz_df in data.groupby(lambda x: x.tz):
            start_date, end_date = tz_df.index[0].date(), tz_df.index[-1].date()
            schedules = list(self.scheduler.get_schedules(start_date, end_date, *window, tz if tzinfo is None else tzinfo))
            for (clock_type, width), bars_df in tz_df.groupby([Tags.CLOCK_TYPE, Tags.WIDTH]):
                step, unit = width, self.OFFSET_MAPPING[clock_type]
                tsgenerator = StepTimestampGenerator(schedules, step, unit, closed=closed)
                validation = KnownTimestampValidation(tsgenerator)
                for barid, barid_df in map(lambda x: (self.BarId(*x[0]), x[1]), bars_df.groupby(Tags.values())):
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
        self.client = DataFrameClient(host=Server.HOSTNAME, port=Server.PORT, database=Server.DBNAME)
        self._schedule = None
        self.schecker = None
        self.BarId = create_BarId()

        self.aparser = argparse.ArgumentParser()

        self.aparser.add_argument('--product', nargs='*', type=str,
                                  help='the product(s) for checking, all if not set')
        self.aparser.add_argument('--ptype', nargs='*', type=str,
                                  help='the product type(s) for checking, all if not set')
        self.aparser.add_argument('--expiry', nargs='*', type=str,
                                  help='the expiry(ies) for checking, all if not set')

        self.aparser.add_argument('--schedule', nargs='*', type=str, default=SCHEDULE,
                                  help='the schedule name(or and the refdata file) for the time series check')
        self.aparser.add_argument('--window', nargs='*', type=str, default=WINDOW,
                                  help='the check timeslot window, please define start/end time in mm:ss')
        self.aparser.add_argument('--closed', nargs='?', type=str,
                                  help="""defines how the window will be closed: "left" or "right", 
                                  defaults to None(both sides)""")
        self.aparser.add_argument('--dtfrom', nargs='*', type=int, default=(last_n_days().timetuple()[0:3]),
                                  help='the check start date as 3 ints(yyyy, M, D), defaults to yesterday')
        self.aparser.add_argument('--dtto', nargs='*', type=int, default=(last_n_days(0).timetuple()[0:3]),
                                  help='the check end date as 3 ints(yyyy, M, D), defaults to today')
        self.aparser.add_argument('--timezone', nargs='*', type=str, default=TIMEZONE,
                                  help='the timezone for the check to run')

        self.aparser.add_argument('--barxml', nargs='?', type=str,
                                  help='the xml output path of bar check')
        self.aparser.add_argument('--barxsl', nargs='?', type=str, default=BARXSL,
                                  help='the path of xsl file for styling the bar check output xml')
        self.aparser.add_argument('--tsxml', nargs='?', type=str,
                                  help='the xml output path of timeseries check')
        self.aparser.add_argument('--tsxsl', nargs='?', type=str, default=TSXSL,
                                  help='the path of xsl file for styling the timeseries check output xml')
        self.aparser.add_argument('--barhtml', nargs='?', type=str, default=BARHTML,
                                  help='the html output path of bar check after xsl transformation')
        self.aparser.add_argument('--tshtml', nargs='?', type=str, default=TSHTML,
                                  help='the html output path of time series check after xsl transformation')

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
        try:
            if nontypes_iterable(dtfrom):
                dtfrom = dt.datetime(*dtfrom)
            dtfrom = pd.Timestamp(dtfrom)
            return self.task_timezone.localize(dtfrom) if self.task_timezone is not None else dtfrom
        except Exception as ex:
            raise TypeError('Invalid dfrom: must be 3 ints(yyyy, M, D) '
                            'or a datetime.date/datetime/pandas.Timestamp object') from ex

    @property
    def task_dtto(self):
        dtto = self.task_args.dtto
        try:
            if nontypes_iterable(dtto):
                dtto = pd.Timestamp(*dtto)
            dtto = pd.Timestamp(dtto)
            return self.task_timezone.localize(dtto) if self.task_timezone is not None else dtto
        except Exception as ex:
            raise TypeError('Invalid dfrom: must be 3 ints(yyyy, M, D) '
                            'or a datetime.date/datetime/pandas.Timestamp object') from ex

    @property
    def task_timezone(self):
        if self.task_args.timezone is None:
            return pytz.UTC
        return pytz.timezone(self.task_args.timezone)

    @property
    def task_bar_etree(self):
        return rcsv_addto_etree({self.START_DT: self.task_dtfrom,
                                 self.END_DT: self.task_dtto},
                                self.REPORT)

    @property
    def task_ts_etree(self):
        et = self.task_bar_etree
        window = self.task_window
        et.set(self.START_TIME, str(window[0]))
        et.set(self.END_TIME, str(window[1]))
        return et

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

    def get_data(self, tbname, time_from=None, time_to=None, include_from=True, include_to=True,
                 others=None, empty=None, **kwargs):
        terms = [where_term(k, v) for k, v in kwargs.items()]
        terms = terms + time_terms(time_from, time_to, include_from, include_to)
        clauses = where_clause(terms) if others is None else [where_clause(terms)] + to_iter(others)
        qstring = select_query(tbname, clauses=clauses)
        return self.client.query(qstring).get(tbname, empty)

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

        return results[BarChecker.CHECK_COLS][~results.all(axis=1)]

    def missing_products(self, to_element=True):
        missing_prods = [p for p in to_iter(self.task_product)
                         if self.get_data(Enriched.name(), self.task_dtfrom, self.task_dtto, **{Tags.PRODUCT: p}) is None]

        # prods = set(data[Tags.PRODUCT])
        # missing_prods = [p for p in to_iter(self.task_product) if p not in prods]
        # if missing_prods:
        #     msg = {'product': missing_prods, 'ptype': self.task_ptype, 'expiry': self.task_expiry,
        #            'dfrom': self.task_dtfrom, 'dto': self.task_dtto}
        #     logging.info('No data selected for {}'.format({k: v for k, v in msg.items() if v}))

        if to_element and missing_prods:
            return rcsv_addto_etree(map(lambda x: (self.PRODUCT, x), missing_prods), self.MISSING_PRODS)

    def bar_checks_xml(self, data, root=None, outpath=None):
        xml = self.task_bar_etree if root is None else root

        for bar, group in data.groupby(Tags.values()):
            bar_ele = rcsv_addto_etree(self.BarId(*bar)._asdict(), self.BAR)
            results = self.run_bar_checks(group)
            if not results.empty:
                xml.append(df_to_xmletree(results[BarChecker.CHECK_COLS], self.RECORD, bar_ele, TIME_IDX))

        if outpath is not None:
            etree_tostr(xml, outpath, self.task_barxsl)
        return xml

    def timeseries_checks_xml(self, data, root=None, outpath=None):
        xml = self.task_ts_etree if root is None else root

        results = self.schecker.validate(data, self.task_window, self.task_closed, self.task_timezone)
        for record in self.schecker.to_dated_results(results):
            xml.append(rcsv_addto_etree(record, self.RECORD))

        if outpath is not None:
            etree_tostr(xml, outpath, self.task_tsxsl)
        return xml


    # def check(self, data):
    #     barxml = self.bar_checks_xml(data, self.task_barxml)
    #     tsxml = self.timeseries_checks_xml(data, self.task_tsxml)
    #
    #     barhtml = etree_tostr(to_styled_xml(barxml, self.task_barxsl), self.task_barhtml)
    #     tshtml = etree_tostr(to_styled_xml(tsxml, self.task_tsxsl), self.task_tshtml)
    #
    #     return barhtml, tshtml
    #
    #
    # def run_checks(self, **kwargs):
    #     self.set_taskargs(**kwargs)
    #     fields = {Tags.PRODUCT: self.task_product, Tags.TYPE: self.task_ptype, Tags.EXPIRY: self.task_expiry}
    #     data = self.get_data(Enriched.name(), self.task_dtfrom, self.task_dtto,
    #                          empty=pd.DataFrame(columns=Fields.values() + Tags.values()), **fields)
    #     return self.check(data)



