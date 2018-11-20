from itertools import groupby, islice
from timeutils.scheduler import *



# def if_idx_between_time(data, start, end, closed, window_tz=None):
#     delta = timedelta_between(end, start)
#     iter_data = data.iterrows() if isinstance(data, pd.DataFrame) else data.items()
#     for (date, tz), rows in groupby(iter_data, lambda x: (x[0].date(), x[0].tz)):
#         db_start = to_tz_datetime(date=date, time=start, from_tz=window_tz, to_tz=tz)
#         db_end = db_start + delta
#         yield from (isin_closed(row[0], db_start, db_end, closed) for row in rows)


class StepTimestampGenerator(object):
    def __init__(self, schedules, step, unit, offset=0, closed=None):
        self.schedules = {s[0].date(): s for s in schedules}
        self.unit = unit
        self.freq = self.to_timedelta(step)
        self._offset = None
        self.offset = offset
        self.closed = closed
        self._include_start, self._include_end = TimeBound.closed_convert(closed)

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, value):
        offset = self.to_timedelta(value)
        if offset < dt.timedelta(0):
            offset = offset % self.freq
        self._offset = offset

    def reset_freq(self, step, unit=None):
        if unit is not None:
            self.unit = unit
        self.freq = self.to_timedelta(step)

    def to_timedelta(self, offset):
        if isinstance(offset, (int, float, str)):
            offset = self.unit(float(offset))
        return dt.timedelta(0) + offset

    def is_valid(self, ts):
        start_ts, end_ts = self.schedules[ts.date()]
        if not TimeBound.isin_closed(ts, start_ts, end_ts, (self._include_start, self._include_end)):
            return False
        delta = ts - start_ts - self.offset
        return delta % self.freq == dt.timedelta(0)

    def gen_intraday_range(self, date):
        if date not in self.schedules:
            return iter(())
        start_ts, end_ts = self.schedules[date]
        curr = start_ts + self.offset
        while curr <= end_ts:
            if TimeBound.isin_closed(curr, start_ts, end_ts, (self._include_start, self._include_end)):
                yield curr
            curr += self.freq

    def closed_bound(self, timestamps):
        for date, date_timestamps in groupby(timestamps, lambda x: x.date()):
            start, end = self.schedules[date]

            # the first to yield should be the one right after the consecutive isin_closed() == False at the start
            # the last to yield should be the one right before the consecutive isin_closed() == False at the end
            groupby_closed = groupby(date_timestamps,
                                     lambda x: TimeBound.isin_closed(x, start, end, (self._include_start, self._include_end)))
            groupby_closed = [(is_closed, list(seq)) for is_closed, seq in groupby_closed]
            first = None if groupby_closed[0][0] else 1
            last = None if groupby_closed[-1][0] else len(groupby_closed) - 1

            for is_closed, ts_seq in islice(groupby_closed, first, last):
                yield from ts_seq


class KnownTimestampValidation(object):
    def __init__(self, tsgenerator, timestamps=None):
        self.tsgenerator = tsgenerator
        self._timestamps_orig, self._timestamps = None, None
        self.timestamps = timestamps

    @property
    def timestamps(self):
        return self._timestamps

    @timestamps.setter
    def timestamps(self, value):
        if value is None:
            self._timestamps_orig, self._timestamps = value, value
        else:
            self._timestamps_orig = list(value)
            self._timestamps = defaultdict(list)
            for i, v in enumerate(self.tsgenerator.closed_bound(value)):
                self._timestamps[v].append(i)

    def invalids(self):
        for ts in self.timestamps:
            if not self.tsgenerator.is_valid(ts):
                yield ts

    def gaps(self, date):
        valids = self.tsgenerator.gen_intraday_range(date)
        for contains, grouper in groupby(valids, lambda x: x in self.timestamps):
            if not contains:
                ts_chunk = list(grouper)
                yield (ts_chunk[0], ts_chunk[-1])

    def reversions(self, date):
        i_expected = 0  # expected index of the current ts in the actual timestamps after each loop
        # equal to the number of the preceding ts with lower value than the current ts

        i_iter = 0
        for ts in self.tsgenerator.gen_intraday_range(date):
            if ts not in self.timestamps:
                continue

            for i_actual in self.timestamps[ts]:
                if i_expected == i_actual:
                    i_expected += 1
                elif i_actual > i_expected:
                    # find the expected index of next unvisited ts in the actual sequence
                    # any preceding ts smaller than current ts should have been visited
                    ts_at_expected = self._timestamps_orig[i_expected]
                    while ts_at_expected < ts or (not self.tsgenerator.is_valid(ts_at_expected)):
                        i_expected += 1
                        ts_at_expected = self._timestamps_orig[i_expected]

                    if i_expected != i_actual:
                        yield (ts, i_actual, i_iter)
                else:
                    raise ValueError('Error in the generation of index dict of timestamps')

            i_iter += 1


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