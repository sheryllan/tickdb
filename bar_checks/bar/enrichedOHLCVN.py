import argparse
import logging
import os
from collections import namedtuple
from itertools import zip_longest, groupby
from types import MappingProxyType
import re

from bar.bardataaccess import BarAccessor, ResultData
from dataframeutils import *
from timeutils.timeseries import *
from xmlconverter import *


def set_dbconfig(server):
    global DataAccessor
    global Server, Enriched
    global Fields, Tags
    global Barid

    DataAccessor = BarAccessor.factory(server)
    Server = DataAccessor.config
    Enriched = Server.TABLES[Server.EnrichedOHLCVN.name()]
    Fields = Enriched.Fields
    Tags = Enriched.Tags
    Barid = create_BarId()


def create_BarId():
    class BarId(namedtuple('BarId', Tags.values())):
        def __new__(cls, values, fill_value=None):
            if isinstance(values, Mapping):
                values = dict(values)
                filled_values = {field: fill_value if values.get(field) is None else values[field]
                                 for field in cls._fields}
                return super().__new__(cls, **filled_values)
            elif nontypes_iterable(values):
                filled_values = [fill_value if x is None else x for _, x in zip_longest(cls._fields, values)]
                return super().__new__(cls, *filled_values)

        def __init__(self, *args, **kwargs):
            self.id = self.__hash__()

        @classmethod
        def fields(cls):
            return cls._fields + ('id',)

        def asdict(self):
            return {**self._asdict(), 'id': self.id}

    return BarId


class BarCheckInfo(Mapping):
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

        mapped = df_tmp.apply(lambda x: BarCheckInfo(*x, pass_msg=pass_msg), axis=1)
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
    def check_prices_rollover(cls, df, by=None):
        volume = Fields.VOLUME
        fopen, fclose, fhigh, flow = Fields.OPEN, Fields.CLOSE, Fields.HIGH, Fields.LOW
        errmsgs = pd.Series(['open not rolled over', 'close not rolled over',
                             'high not rolled over', 'low not rolled over'])
        by = df.index.to_series().map(by) if callable(by) else by

        def check():
            for _, gdf in iter_groupby(df, by):
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
    GAP = 'gap'  # tag
    REVERSION = 'reversion'  # tag
    INVALID = 'invalid'  # tag

    DATE = 'date'  # attribute
    TIMEZONE = 'timezone'  # attribute

    TIMESTAMP = 'timestamp'
    EPOCH = 'epoch'
    PRIOR_TS = 'prior_ts'
    CURR_TS = 'current_ts'
    START_TS = 'start_ts'
    END_TS = 'end_ts'

    ERRORTYPE = 'error_type'
    ERRORVAL = 'error_value'

    UNIT_MAPPING = {'M': 'T'}

    @classmethod
    def error_dict(cls, date, tz, error_type, error_val):
        return {cls.DATE: str(date),
                cls.TIMEZONE: tz.zone,
                cls.ERRORTYPE: error_type,
                cls.ERRORVAL: error_val}

    @classmethod
    def invalid_dict(cls, irow):
        ts, _ = irow
        date, tz = ts.date(), ts.tz
        return cls.error_dict(date, tz, cls.INVALID, {cls.TIMESTAMP: ts})

    @classmethod
    def reversion_dict(cls, irow, irow_pre):
        ts, row = irow
        ts_pre, row_pre = irow_pre
        date, tz = ts.date(), ts.tz
        return cls.error_dict(date, tz,
                              cls.REVERSION,
                              {cls.PRIOR_TS: {cls.TIMESTAMP: ts_pre, cls.EPOCH: to_num(ts_pre)},
                               cls.CURR_TS: {cls.TIMESTAMP: ts, cls.EPOCH: to_num(ts)}})

    @classmethod
    def gap_dict(cls, gap):
        date, tz = gap[0].date(), gap[0].tz
        return cls.error_dict(date, tz, cls.GAP,
                              {cls.START_TS: gap[0].round('s'),
                               cls.END_TS: gap[1].round('s')})

    # TODO: differentiate back-filled data
    @classmethod
    def gaps(cls, df, tsgenerator: StepTimestampGenerator, schedule_bound: ScheduleBound,
                  max_interval: dt.timedelta = None):
        
        max_interval = dt.timedelta(minutes=5) if max_interval is None else max_interval
        closed, tz, slist = schedule_bound.closed, schedule_bound.tz, schedule_bound.schedule_list
        seen = {b: {v: False for v in tsgenerator.valid_date_range(*b, closed, tz)} for b in slist}

        trigger_time = df[Fields.TRIGGER_TIME]
        not_backfill = (dt.timedelta(0) <= trigger_time - df.index) & (trigger_time - df.index < tsgenerator.freq)
        not_gappy = [not_backfill]
        if tsgenerator.freq < max_interval:
            trigger_time_prev = SeriesValidation.rolling_pre_max(trigger_time, schedule_bound)
            is_less_frequent = SeriesValidation.is_within_freq(trigger_time, trigger_time_prev,
                                                               max_interval=max_interval, closed=(False, True), shift=0)
            not_gappy.append(is_less_frequent)

        for ts, *eval in zip(df.index, *not_gappy):
            bound = schedule_bound.enclosing_schedule(ts)
            if all(eval) and bound in seen and ts in seen[bound]:
                seen[bound][ts] = True
                
        for _, valids in seen.items():
            for is_seen, grouper in groupby(valids.items(), lambda x: x[1]):
                if not is_seen:
                    ts_chunk = list(map(lambda x: x[0], grouper))
                    yield ts_chunk[0], ts_chunk[-1]


    @classmethod
    def validate_sequential(cls, df, tsgenerator: StepTimestampGenerator):
        is_valid = SeriesValidation.is_valid(df.index, tsgenerator)
        is_incremental = SeriesValidation.is_within_freq(df.index)
        timestamps = SeriesValidation.zip_with_shift(df.iterrows())

        for (irow, irow_pre), valid, incremental in zip(timestamps, is_valid, is_incremental):
            if not valid:
                yield cls.invalid_dict(irow)
            elif not incremental:
                yield cls.reversion_dict(irow, irow_pre)

    @classmethod
    def validate_aggregated(cls, df, tsgenerator: StepTimestampGenerator, schedule_bound: ScheduleBound, **kwargs):
        for gap in cls.gaps(df, tsgenerator, schedule_bound, **kwargs):
            yield cls.gap_dict(gap)

    @classmethod
    def validate_bar_series(cls, barid, bardf: pd.DataFrame, schedule_bound: ScheduleBound, **kwargs):
        if any(x is None for x in [barid.width, barid.clock_type, barid.offset]):
            return iter('')

        unit = cls.UNIT_MAPPING[barid.clock_type]
        offset, offset_unit = re.match('^(.*?)([A-Za-z]*)$', str(barid.offset)).groups()
        offset_unit = cls.UNIT_MAPPING.get(offset_unit, unit)
        tsgenerator = StepTimestampGenerator(barid.width, unit, (offset, offset_unit))
        yield from cls.validate_sequential(bardf, tsgenerator)
        yield from cls.validate_aggregated(bardf, tsgenerator, schedule_bound, **kwargs)


class TaskArguments(argparse.ArgumentParser):
    PRODUCT = 'product'
    TYPE = 'ptype'
    EXPIRY = 'expiry'
    SCHEDULE = 'schedule'
    WINDOW = 'window'
    WINDOW_TZ = 'window_tz'
    CLOSED = 'closed'
    DTFROM = 'dtfrom'
    DTTO = 'dtto'
    TIMEZONE = 'timezone'

    CHECK = 'check'

    def __init__(self, *args, **kwargs):
        self._arg_dict = {}
        super().__init__(*args, **kwargs)

        self.add_argument('--' + self.PRODUCT, nargs='*', type=str,
                          help='the product(s) for checking, all if not set')
        self.add_argument('--' + self.TYPE, nargs='*', type=str,
                          help='the product type(s) for checking, all if not set')
        self.add_argument('--' + self.EXPIRY, nargs='*', type=str,
                          help='the expiry(ies) for checking, all if not set')

        self.add_argument('--' + self.SCHEDULE, nargs='*', type=str, default='BaseSchedule',
                          help='the schedule name(or and the refdata file) for the time series check')
        self.add_argument('--' + self.WINDOW, nargs='*', type=str, default=(MIN_TIME, MAX_TIME),
                          help='the bounded check window, please define start/end time in mm:ss')
        self.add_argument('--' + self.WINDOW_TZ, nargs='?', type=str, default=pytz.UTC,
                          help='the timezone name for the window')
        self.add_argument('--' + self.CLOSED, nargs='?', type=str,
                          help="""defines how the window will be closed: "left" or "right", 
                                          defaults to None(both sides)""")

        self.add_argument('--' + self.TIMEZONE, nargs='*', type=str, default=pytz.UTC,
                          help='the timezone for the check to run')
        self.add_argument('--' + self.DTFROM, nargs='*', type=int, default=last_n_days(),
                          help='the start time in integers(yyyy, M, D, [optional HH, mm, ss]), yesterday by default')
        self.add_argument('--' + self.DTTO, nargs='*', type=int, default=last_n_days(0),
                          help='the end time in integers(yyyy, M, D, [optional HH, mm, ss]), today by default')

        self.add_argument('--' + self.CHECK, nargs='*', type=str,
                          help='the type(s) of check to run, all if not set')

    def add_argument(self, *args, **kwargs):
        action = super().add_argument(*args, **kwargs)
        if not hasattr(self.__class__, action.dest):
            setattr(self.__class__, action.dest, property(lambda x: x.arg_dict.get(action.dest)))
        self.update_args(**{action.dest: action.default})

    def parse_args(self, args=None, namespace=None):
        parsed = super().parse_args(args, namespace)
        self.update_args(**parsed.__dict__)
        parsed.__dict__ = self._arg_dict
        return parsed

    def update_args(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                self._arg_dict[k] = getattr(self, '_' + k, lambda x: x)(v)

    def max_dtto(self):
        return to_tz_datetime(last_n_days(), MAX_TIME, to_tz=self.timezone, to_orig=False)

    @property
    def arg_dict(self):
        return self._arg_dict

    def _window(self, value):
        window = to_iter(value, ittype=tuple)
        return validate_time(window[0]), validate_time(window[1])

    def _window_tz(self, value):
        return pytz.timezone(value) if isinstance(value, str) else value

    def _closed(self, value):
        if value not in ['left', 'right', None]:
            raise ValueError('The value for argument "closed" must be "left"/"right", or if not set, None by default')
        return value

    def _dtfrom(self, value):
        try:
            dtfrom = pd.Timestamp(*value) if nontypes_iterable(value) else pd.Timestamp(value)
            return to_tz_datetime(dtfrom, to_tz=self.timezone)
        except Exception as ex:
            raise TypeError('Invalid dfrom: must be 3 ints(yyyy, M, D) '
                            'or a datetime.date/datetime/pandas.Timestamp object') from ex

    def _dtto(self, value):
        try:
            dtto = pd.Timestamp(*value) if nontypes_iterable(value) else pd.Timestamp(value)
            return min(to_tz_datetime(dtto, to_tz=self.timezone), self.max_dtto())
        except Exception as ex:
            raise TypeError('Invalid dfrom: must be 3 ints(yyyy, M, D) '
                            'or a datetime.date/datetime/pandas.Timestamp object') from ex

    def _timezone(self, value):
        return pytz.timezone(value) if isinstance(value, str) else value

    def _check(self, value):
        return self.get_default(self.CHECK) if value is None else to_iter(value, dtype=str)


class SubCheckTask(object):
    REPORT = 'report'  # tag
    TIME = 'time'
    WINDOW = 'window'  # attribute

    PRODUCT = 'product'  # tag

    RECORD = 'record'  # tag
    BAR = 'bar'  # tag

    def __init__(self, args: TaskArguments, xsl=None):
        self.args = args
        self.xsl_pi = None if xsl is None else xsl_pi(xsl)

    @property
    def xml_etree(self):
        root = rcsv_addto_element({self.PRODUCT: ', '.join(to_iter(self.args.product, dtype=str)),
                                   self.TIME: f'{self.args.dtfrom.round("s")} - {self.args.dtto.round("s")}',
                                   self.WINDOW: '{} - {} ({})'.format(*self.args.window, str(self.args.window_tz))},
                                  self.REPORT)
        return to_elementtree(root, self.xsl_pi)

    def barid_element(self, value, fill_value='*'):
        bar = Barid(value, fill_value).asdict()
        return rcsv_addto_element(bar, self.BAR)

    def bar_series_check_xml(self, data, bar, root=None):
        raise NotImplementedError


class BarCheckTask(SubCheckTask):
    XSL = os.path.join(os.path.dirname(__file__), 'bar_check.xsl')
    OUT_COLS = [BarChecker.PRICES_ROLLOVER_CHECK,
                BarChecker.HIGH_LOW_CHECK,
                BarChecker.PCLV_ORDER_CHECK,
                BarChecker.BID_ASK_CHECK,
                BarChecker.VOLUME_CHECK,
                BarChecker.VOL_ON_LV1_CHECK,
                BarChecker.VOL_ON_LV2_CHECK,
                BarChecker.VOL_ON_LV3_CHECK,
                # BarChecker.VWAP_CHECK
                ]

    def __init__(self, args):
        super().__init__(args, self.XSL)

    def run_check(self, data):
        index_schedule = data.index.to_series().map(self.args.schedule_bound.enclosing_schedule)
        index_mask = index_schedule.notna()
        data_cap = data.loc[index_mask]
        if data_cap.empty:
            results = data_cap
        else:
            data_cap = BarChecker.nullify_undefined(data_cap)
            checks = [
                BarChecker.check_prices_rollover(data_cap, index_schedule[index_mask]),
                BarChecker.check_high_low(data_cap),
                BarChecker.check_pclv_order(data_cap),
                BarChecker.check_bid_ask(data_cap),
                BarChecker.check_volume(data_cap),
                BarChecker.check_vol_on_lv(data_cap),
                # BarChecker.check_vwap(data_cap)
            ]
            results = pd.concat(checks, axis=1)

        return results[self.OUT_COLS][~results.all(axis=1)] \
            if not results.empty else pd.DataFrame(columns=self.OUT_COLS)

    def bar_series_check_xml(self, data, bar, root=None):
        xml = to_elementtree(self.xml_etree if root is None else root)
        root = xml.getroot()
        barid = Barid(bar)

        logging.info('Running bar check for {}'.format(barid))
        results = self.run_check(data)
        if not results.empty:
            root.append(df_to_element(results, self.barid_element(barid), self.RECORD, None))

        return xml


class TimeSeriesCheckTask(SubCheckTask):
    XSL = os.path.join(os.path.dirname(__file__), 'timeseries_check.xsl')

    def __init__(self, args, checker=SeriesChecker):
        super().__init__(args, self.XSL)
        self.checker = checker

    def bar_series_check_xml(self, data, bar, root=None):
        xml = to_elementtree(self.xml_etree if root is None else root)
        root = xml.getroot()
        barid = Barid(bar)

        logging.info('Running time series check for {}'.format(barid))
        kwargs = dict(max_interval=getattr(self.args, 'max_interval', None))
        validated = self.checker.validate_bar_series(barid, data, self.args.schedule_bound, **kwargs)

        curr_pos = 0
        for date_dict, dated_df in to_grouped_df(validated, [self.checker.DATE, self.checker.TIMEZONE]):
            error_df = dated_df.set_index(self.checker.ERRORTYPE)
            new_subele = rcsv_addto_element(error_df[self.checker.ERRORVAL], self.barid_element(barid))

            while curr_pos < len(root) and root[curr_pos].tag != self.RECORD:
                curr_pos += 1

            if curr_pos >= len(root) or date_dict[self.checker.DATE] < root[curr_pos].get(self.checker.DATE):
                new_ele = rcsv_addto_element(date_dict, self.RECORD)
                new_ele.append(new_subele)
                root.insert(curr_pos, new_ele)
            elif date_dict[self.checker.DATE] == root[curr_pos].get(self.checker.DATE):
                root[curr_pos].append(new_subele)

            curr_pos += 1

        return xml


# class CheckTaskArgs(argparse.Namespace):
#     def __init__(self, **kwargs):
#         for k, v in kwargs.items():
#             
#             setattr(self, name, kwargs[name])
#             
#             
#     
            

class CheckTask(object):
    BAR_CHECK = 'bar'
    TIMESERIESE_CHECK = 'timeseries'

    def __init__(self, data_accessor, taskargs_cls=TaskArguments):
        self.accessor = data_accessor
        self.args = self.expand_taskargs(taskargs_cls)()
        self.args.set_defaults(**{self.args.CHECK: [self.BAR_CHECK, self.TIMESERIESE_CHECK]})

        self.map_to_enriched = {self.args.PRODUCT: Tags.PRODUCT,
                                self.args.TYPE: Tags.TYPE,
                                self.args.EXPIRY: Tags.EXPIRY,
                                self.args.DTFROM: self.accessor.TIME_FROM,
                                self.args.DTTO: self.accessor.TIME_TO}

        self.subtasks = {self.BAR_CHECK: BarCheckTask(self.args),
                         self.TIMESERIESE_CHECK: TimeSeriesCheckTask(self.args)}

    def expand_taskargs(self, cls: type(TaskArguments)):
        new_arg = 'update_schedule'

        def wrapper(obj, attr_name, func):
            def func_new(value):
                val_old = getattr(obj, attr_name)
                val_new = func(value)
                val_new_arg = getattr(obj, new_arg)
                setattr(obj, new_arg, val_new_arg or val_old != val_new)
                return val_new

            return func_new

        class TaskArgumentsExtra(cls):
            SCHEDULE_ARGS = [cls.SCHEDULE, cls.DTFROM, cls.DTTO, cls.WINDOW, cls.WINDOW_TZ, cls.TIMEZONE, cls.CLOSED]

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                setattr(self.__class__, new_arg, False)

                for sarg in self.SCHEDULE_ARGS:
                    setter_name = '_' + sarg
                    setattr(self, setter_name, wrapper(self, sarg, getattr(self, setter_name, lambda x: x)))

                self._schedule_bound = ScheduleBound(None, self.closed, self.timezone)

            @property
            def schedule_bound(self):
                if getattr(self, new_arg):
                    schedule_times = BScheduler(self.schedule).get_schedules(
                        self.dtfrom, self.dtto, *self.window, self.window_tz, self.timezone)
                    self._schedule_bound.set_schedules(schedule_times)
                    setattr(self, new_arg, False)

                return self._schedule_bound

        return TaskArgumentsExtra

    def set_taskargs(self, parse_args=False, **kwargs):
        if parse_args:
            self.args.parse_args()
        self.args.update_args(**kwargs)

    def get_bar_data(self, **kwargs):
        new_kwargs = {self.map_to_enriched.get(k, k): v for k, v in kwargs.items()}
        taskargs = {k: new_kwargs[self.map_to_enriched.get(k, k)] for k, v in self.args.arg_dict.items()
                    if self.map_to_enriched.get(k, k) in new_kwargs}
        self.set_taskargs(**taskargs)
        new_kwargs.update({self.map_to_enriched.get(k, k): v for k, v in self.args.arg_dict.items()})
        return self.accessor.get_data(Enriched.name(), **new_kwargs)

    def check_integrated(self, data: ResultData, check_xmls):
        for bar, bar_df in data:
            for check, xml in check_xmls.items():
                check_xmls[check] = self.subtasks[check].bar_series_check_xml(bar_df, bar, xml)
        return check_xmls
