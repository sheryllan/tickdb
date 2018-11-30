from itertools import groupby
from timeutils.scheduler import *


class StepTimestampGenerator(object):
    INITIAL_OFFSET = dt.timedelta(0)

    def __init__(self, step, unit, offset=0):
        self.unit = unit
        self.freq = self._to_timedelta(step)
        self._offset = None
        self.offset = offset

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, value):
        offset = self._to_timedelta(value)
        if offset < dt.timedelta(0):
            offset = offset % self.freq
        self._offset = offset

    def reset_freq(self, step, unit=None):
        if unit is not None:
            self.unit = unit
        self.freq = self._to_timedelta(step)

    def _to_timedelta(self, offset):
        if isinstance(offset, (int, float, str)):
            offset = self.unit(float(offset))
        return dt.timedelta(0) + offset

    def is_valid(self, ts: pd.Timestamp):
        baseline = ts.normalize() + self.INITIAL_OFFSET
        delta = ts - self.offset - baseline
        return delta % self.freq == dt.timedelta(0)

    def _ceildiv_delta_freq(self, from_ts: pd.Timestamp, to_ts: pd.Timestamp):
        delta = to_ts - from_ts
        ceiling = ceildiv(delta, self.offset)
        return ceiling if ceiling > dt.timedelta(0) else False

    def valid_date_range(self, start: pd.Timestamp, end: pd.Timestamp, closed=None, tz=None):
        init_ts = start.normalize() + self.INITIAL_OFFSET + self.offset

        ceiling = self._ceildiv_delta_freq(init_ts, start)
        if not ceiling:
            raise ValueError('Invalid argument start: must be >= initial offset for positive freq, or vice versa')

        start_ts = to_tz_datetime(init_ts + ceiling * self.freq, to_tz=tz)
        end_ts = to_tz_datetime(end, to_tz=tz)
        return pd.date_range(start_ts, end_ts, freq=self.freq, closed=closed)



class SeriesValidation(object):
    ERRORTYPE = 'error_type'
    ERRORVAL = 'error_value'

    # Validation type
    GAPS = 'gaps'
    REVERSIONS = 'reversions'
    INVALIDS = 'invalids'


    def __init__(self, tsgenerator: StepTimestampGenerator, schedule_bound: ScheduleBound):
        self.tsgenerator = tsgenerator
        self._schedule_bound = schedule_bound
        self._tz = schedule_bound.tz
        self._closed = schedule_bound.closed
        self.valfunc_dict = {self.GAPS: self.gaps,
                             self.REVERSIONS: self.is_time_increasing,
                             self.INVALIDS: self.is_valid}

    #     self._timestamps = None
    #     self._timestamps_orig = None
    #
    #
    # @property
    # def timestamps(self):
    #     return self._timestamps
    #
    # @timestamps.setter
    # def timestamps(self, value):
    #     self._timestamps_orig = [] if value is None else pd.DatetimeIndex(value)
    #     self._timestamps = self._closed_bound(to_tz_series(self._timestamps_orig, to_tz=self._tz))
    #
    #
    # def _closed_bound(self, dtindex: pd.DatetimeIndex):
    #     start, stop = self._schedule_bound.bound_indices(dtindex)
    #     return dtindex[start: stop]

    @classmethod
    def is_time_increasing(cls, timestamps):
        max_pre = pd.Timestamp.min
        for curr in timestamps:
            if max_pre <= curr:
                max_pre = curr
                yield True
            else:
                yield False


    # def delimit_by_schedules(self, timestamps):
    #     for schedule, ts_seq in groupby(timestamps, self._schedule_bound.enclosing_schedule):
    #         if schedule is not None:
    #             yield schedule, to_tz_series(ts_seq, to_tz=self._tz)

    def gaps(self, timestamps: pd.DatetimeIndex):
        grouped = SortedDict(timestamps.groupby(timestamps.map(self._schedule_bound.enclosing_schedule)))
        first, last = grouped.keys()[0], grouped.keys()[-1]

        for bound in self._schedule_bound.schedule_dict.values()[]:
            if bound is None:
                continue

            valids = self.tsgenerator.valid_date_range(*bound, self._closed, self._tz)
            for contains, grouper in groupby(valids, lambda x: x in ts_seq):
                if not contains:
                    ts_chunk = list(grouper)
                    yield ts_chunk[0], ts_chunk[-1]

            # bounded_series = self.delimit_by_schedules(timestamps)
            # valids = flatten_iter(self.tsgenerator.valid_date_range(*schedule, self._closed, self._tz)
            #                       for schedule, ts_seq in bounded_series)



    def is_valid(self, timestamps):
        for ts in timestamps:
            return self.tsgenerator.is_valid(ts)


    # def invalids_reversions(self, timestamps: pd.DatetimeIndex):
    #     for i, is_in_order in enumerate(self.is_time_increasing(timestamps)):
    #         yield to_tz_datetime(timestamps[i], to_tz=self._tz), \
    #               (not self.tsgenerator.is_valid(timestamps[i]), not is_in_order)

    def error_dict(self, errortype, errorval):
        return {self.ERRORTYPE: errortype, self.ERRORVAL: errorval}


    def compound_validations(self, df: pd.DataFrame, valtypes):
        timestamps = to_tz_series(df.index, to_tz=self._tz)
        valfuncs = {vt: self.valfunc_dict[vt](timestamps) for vt in valtypes if vt in self.valfunc_dict}

        if self.GAPS in valfuncs:
            for gap in valfuncs[self.GAPS]:
                yield self.error_dict(self.GAPS, gap)

        for ts in timestamps:
            if self.INVALIDS in valfuncs:
                value = next(valfuncs[self.INVALIDS])
                if not value:
                    yield self.error_dict(self.INVALIDS, ts)

            if self.REVERSIONS in valfuncs:
                value = next(valfuncs[self.REVERSIONS])
                if not value:
                    yield self.error_dict(self.REVERSIONS, ts)








# region unused
# def to_nanosecond(time_in_unit, unit):
#     time_in_unit = float(time_in_unit)
#     if unit == 'sec':
#         return time_in_unit * 1E+9
#
#     elif unit == 'min':
#         return time_in_unit * 6E+10
#
#     elif unit == 'hr':
#         return time_in_unit * 3.6E+12
#
#     elif unit == 'day':
#         return time_in_unit * 24 * 3.6E+12

# def dtrange_between_time(dtrange, start, end, closed):
#     include_start, include_end = closed_convert(closed)
#     return pd.Series(index=dtrange).between_time(start, end, include_start, include_end).index
#
#
# def all_valid_timestamps(start_dtime, end_dtime, offset, interval, unit=offsets.Minute, closed=None):
#     offset, freq = unit(float(offset)), unit(float(interval))
#     offset_start = start_dtime + offset
#     dtrange = pd.date_range(offset_start, end_dtime, freq=freq)
#     return dtrange_between_time(dtrange, start_dtime.time(), end_dtime.time(), closed)
# endregion