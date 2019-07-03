from itertools import tee, chain
from pandas.tseries.offsets import prefix_mapping, DateOffset
from pandas.tseries.frequencies import to_offset
from timeutils.scheduler import *


class StepTimestampGenerator(object):

    def __init__(self, step, unit, offset=0):
        self.unit = unit
        self.freq = self._to_timedelta(step)
        self.offset = offset

    @property
    def unit(self):
        return self._unit

    @unit.setter
    def unit(self, value):
        self._unit = prefix_mapping[value] if isinstance(value, str) else value
        if not issubclass(self._unit, DateOffset):
            raise TypeError('Invalid value for unit: must be a str or subtype of pandas.tseries.offsets.DateOffset')

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, value):
        offset = self._to_timedelta(*to_iter(value))
        if offset < dt.timedelta(0):
            offset = offset % self.freq
        self._offset = offset

    def reset_freq(self, step, unit=None):
        if unit is not None:
            self.unit = unit
        self.freq = self._to_timedelta(step)

    def _to_timedelta(self, value, unit=None):
        try:
            value = float(value)
            unit = self.unit if unit is None else unit
            unit = unit._prefix if isinstance(unit, type) and issubclass(unit, DateOffset) else str(unit)
            offset = to_offset(f'{value}{unit}')
        except ValueError:
            offset = to_offset(value)

        return dt.timedelta(0) + offset

    def is_valid(self, ts: pd.Timestamp):
        baseline = ts.normalize()
        delta = ts - self.offset - baseline
        return delta % self.freq == dt.timedelta(0)

    # calculates the ceiling multiple of the frequency between the timestamps, default relative to the offset of the day
    def _ceildiv_freq(self, from_ts: pd.Timestamp, to_ts: pd.Timestamp):
        return ceildiv(to_ts - from_ts, self.freq)

    def valid_date_range(self, start: pd.Timestamp, end: pd.Timestamp, closed=None, tz=None):
        init_ts = start.normalize() + self.offset
        ceiling = self._ceildiv_freq(init_ts, start)
        start_ts = to_tz_datetime(init_ts + ceiling * self.freq, to_tz=tz)
        end_ts = to_tz_datetime(end, to_tz=tz)
        return pd.date_range(start_ts, end_ts, freq=self.freq, closed=closed)


class SeriesValidation(object):
    @staticmethod
    def zip_with_shift(iter1, iter2=None, fill_value=None, shift=-1):
        if iter2 is None:
            iter1, iter2 = tee(iter1)
        
        iter_fill = [fill_value] * abs(shift)
        iter2 = chain(iter_fill, iter2) if shift < 0 else chain(iter2, iter_fill)
        yield from zip(iter1, iter2)

    @staticmethod
    def is_valid(timestamps, tsgenerator: StepTimestampGenerator, schedule_bound: ScheduleBound = None):
        for ts in timestamps:
            yield tsgenerator.is_valid(ts) and \
                  (schedule_bound is None or schedule_bound.is_on_schedule(ts))


    @staticmethod
    def is_within_freq(timestamps, timestamps_prev=None,
                       min_interval: dt.timedelta = dt.timedelta(0), max_interval: dt.timedelta = None,
                       closed=(False, True), **kwargs):
        # default behaviour is the original is_incremental()
        for ts, ts_prev in SeriesValidation.zip_with_shift(timestamps, timestamps_prev, **kwargs):
            yield pd.isna(ts_prev) or isin_closed(ts - ts_prev, min_interval, max_interval, closed)
            
    @staticmethod
    def rolling_pre_max(timestamps, schedule_bound: ScheduleBound = None):
        pre_max = {}
        in_schedule = (lambda x: 1) if schedule_bound is None else schedule_bound.enclosing_schedule
        for ts in timestamps:
            bound = in_schedule(ts)
            if bound is None:
                yield None
                continue
                
            ts_max = pre_max.get(bound)
            yield ts_max
            if ts_max is None or ts > ts_max:
                pre_max[bound] = ts
            





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
