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

    def valid_date_range(self, start: pd.Timestamp, end: pd.Timestamp, closed, tz=None):
        init_ts = start.normalize() + self.INITIAL_OFFSET + self.offset

        ceiling = self._ceildiv_delta_freq(init_ts, start)
        if not ceiling:
            raise ValueError('Invalid argument start: must be >= initial offset for positive freq, or vice versa')

        start_ts = to_tz_datetime(init_ts + ceiling * self.freq, to_tz=tz)
        end_ts = to_tz_datetime(end, to_tz=tz)
        return pd.date_range(start_ts, end_ts, freq=self.freq, closed=closed)


class KnownTimestampValidation(object):
    def __init__(self, tsgenerator: StepTimestampGenerator, schedule_bound: ScheduleBound, timestamps=None, tz=None):
        self.tsgenerator = tsgenerator
        self.schedule_bound = schedule_bound
        self.tz = tz
        self._timestamps = None
        self._timestamps_orig = None

        self.timestamps = timestamps


    @property
    def timestamps(self):
        return self._timestamps

    @timestamps.setter
    def timestamps(self, value):
        self._timestamps_orig = [] if value is None else pd.DatetimeIndex(value)
        self._timestamps = self._closed_bound(self._timestamps_orig)
            # self._timestamps = defaultdict(list)
            # for i, ts in enumerate(self._timestamps_orig):
            #     self._timestamps[ts].append(i)

    def _closed_bound(self, timestamps):
        dtindex = pd.DatetimeIndex(timestamps)
        start, stop = self.schedule_bound.bound_indices(dtindex)
        return dtindex[start: stop]

    # def invalids(self):
    #     for ts in self.timestamps:
    #         if not self.tsgenerator.is_valid(ts):
    #             yield ts

    def gaps(self):
        if self.timestamps.empty:
            return self._timestamps_orig

        bound = self.schedule_bound.enclosing_schedule(self.timestamps[0])
        valids = self.tsgenerator.valid_date_range(*bound, self.schedule_bound.closed, self.timestamps.tz)
        for contains, grouper in groupby(valids, lambda x: x in self.timestamps):
            if not contains:
                ts_chunk = list(grouper)
                yield to_tz_datetime(ts_chunk[0], to_tz=self.tz), to_tz_datetime(ts_chunk[-1], to_tz=self.tz)

    def invalids_reversions(self):
        # i_expected = 0
        # # expected index of the current ts in the actual timestamps after each loop
        # # equal to the number of the preceding ts with lower value than the current ts
        #
        # for ts in self.tsgenerator.valid_date_range(*self.timestamps_bound, self.schedule_bound.closed, self.to_tz):
        #     if ts not in self.timestamps:
        #         continue
        #
        #     for i_actual in self.timestamps[ts]:
        #         if i_expected == i_actual:
        #             i_expected += 1
        #         elif i_actual > i_expected:
        #             # find the expected index of next unvisited ts in the actual sequence
        #             # any preceding ts smaller than current ts should have been visited
        #             ts_at_expected = self._timestamps_orig[i_expected]
        #             while ts_at_expected < ts or (not self.tsgenerator.is_valid(ts_at_expected)):
        #                 i_expected += 1
        #                 ts_at_expected = self._timestamps_orig[i_expected]
        #
        #             if i_expected != i_actual:
        #                 yield (ts, i_actual)
        #         else:
        #             raise ValueError('Error in the generation of index dict of timestamps')

        max_pre = pd.Timestamp.min
        for curr in self.timestamps:
            if not self.tsgenerator.is_valid(curr):
                yield False, to_tz_datetime(curr, to_tz=self.tz)
            else:
                if max_pre <= curr:
                    max_pre = curr
                else:
                    yield True, to_tz_datetime(curr, to_tz=self.tz)




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