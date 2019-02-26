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

    @staticmethod
    def is_incremental(timestamps):
        max_pre = None
        for curr in timestamps:
            yield max_pre is None or max_pre <= curr
            max_pre = curr

    @staticmethod
    def gaps(timestamps, tsgenerator, schedule_bound: ScheduleBound):
        grouped = normal_group_by(timestamps, schedule_bound.enclosing_schedule, True)
        for bound in schedule_bound.schedule_list:
            valids = tsgenerator.valid_date_range(*bound, schedule_bound.closed, schedule_bound.tz)
            if valids.empty:
                continue

            if bound not in grouped:
                yield bound
            else:
                for contains, grouper in groupby(valids, lambda x: x in grouped[bound]):
                    if not contains:
                        ts_chunk = list(grouper)
                        yield ts_chunk[0], ts_chunk[-1]

    @staticmethod
    def is_valid(timestamps, tsgenerator: StepTimestampGenerator):
        for ts in timestamps:
            yield tsgenerator.is_valid(ts)


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
