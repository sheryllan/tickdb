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
        try:
            offset_unit = self.unit(float(offset))
        except ValueError:
            offset_unit = offset

        return dt.timedelta(0) + offset_unit

    def is_valid(self, ts: pd.Timestamp):
        baseline = ts.normalize() + self.INITIAL_OFFSET
        delta = ts - self.offset - baseline
        return delta % self.freq == dt.timedelta(0)

    def _ceildiv_delta_freq(self, from_ts: pd.Timestamp, to_ts: pd.Timestamp):
        delta = to_ts - from_ts
        ceiling = ceildiv(delta, self.freq)
        return ceiling if ceiling >= 0 else False

    def valid_date_range(self, start: pd.Timestamp, end: pd.Timestamp, closed=None, tz=None):
        init_ts = start.normalize() + self.INITIAL_OFFSET + self.offset

        ceiling = self._ceildiv_delta_freq(init_ts, start)
        if ceiling is False:
            raise ValueError('Invalid argument start: must be >= initial offset for positive freq, or vice versa')

        start_ts = to_tz_datetime(init_ts + ceiling * self.freq, to_tz=tz)
        end_ts = to_tz_datetime(end, to_tz=tz)
        return pd.date_range(start_ts, end_ts, freq=self.freq, closed=closed)


class SeriesValidation(object):
    # Validation type
    GAPS = 'gaps'
    REVERSIONS = 'reversions'
    INVALIDS = 'invalids'

    TIMESTAMP = 'timestamp'
    START_TS = 'start_ts'
    END_TS = 'end_ts'

    AGGR_TYPES = {GAPS}
    FALSE_MASK_TYPES = {REVERSIONS, INVALIDS}

    def __init__(self, tsgenerator: StepTimestampGenerator, schedule_bound: ScheduleBound):
        self.tsgenerator = tsgenerator
        self._schedule_bound = schedule_bound
        self._tz = schedule_bound.tz
        self._closed = schedule_bound.closed
        self.valfunc_dict = {self.GAPS: self.gaps,
                             self.REVERSIONS: self.is_time_increasing,
                             self.INVALIDS: self.is_valid}

    @classmethod
    def is_time_increasing(cls, timestamps):
        max_pre = None
        for curr in timestamps:
            if max_pre is None or max_pre <= curr:
                max_pre = curr
                yield True
            else:
                yield False


    # def delimit_by_schedules(self, timestamps):
    #     for schedule, ts_seq in groupby(timestamps, self._schedule_bound.enclosing_schedule):
    #         if schedule is not None:
    #             yield schedule, to_tz_series(ts_seq, to_tz=self._tz)

    def gaps(self, timestamps):
        grouped = normal_group_by(timestamps, self._schedule_bound.enclosing_schedule, True)
        for bound in self._schedule_bound.schedule_list:
            if bound not in grouped:
                yield {self.START_TS: bound[0], self.END_TS: bound[1]}
            else:
                valids = self.tsgenerator.valid_date_range(*bound, self._closed, self._tz)
                for contains, grouper in groupby(valids, lambda x: x in grouped[bound]):
                    if not contains:
                        ts_chunk = list(grouper)
                        yield {self.START_TS: ts_chunk[0],  self.END_TS: ts_chunk[-1]}

    def is_valid(self, timestamps):
        for ts in timestamps:
            yield self.tsgenerator.is_valid(ts)

    def compound_validation(self, timestamps: pd.DatetimeIndex, valtypes):
        timestamps = to_tz_series(timestamps, to_tz=self._tz)
        valfuncs = {vt: self.valfunc_dict[vt](timestamps) for vt in valtypes}

        for ts in timestamps:
            for vtype, vfunc in valfuncs.items():
                vresult = next(vfunc, None)
                if vresult is None:
                    continue
                elif vtype in self.AGGR_TYPES:
                    yield vtype, vresult
                elif vtype in self.FALSE_MASK_TYPES and not vresult:
                    yield vtype, {self.TIMESTAMP: ts}




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