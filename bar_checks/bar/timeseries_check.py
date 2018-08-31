import pandas as pd
from pandas.tseries import offsets
from itertools import groupby


def to_nanosecond(time_in_unit, unit):
    time_in_unit = float(time_in_unit)
    if unit == 'sec':
        return time_in_unit * 1E+9

    elif unit == 'min':
        return time_in_unit * 6E+10

    elif unit == 'hr':
        return time_in_unit * 3.6E+12

    elif unit == 'day':
        return time_in_unit * 24 * 3.6E+12


def dtrange_between_time(dtrange, start, end, closed):
    include_start, include_end = True, True
    if closed == 'left':
        include_end = False
    elif closed == 'right':
        include_start = False

    return pd.Series(index=dtrange).between_time(start, end, include_start, include_end).index


def all_valid_timestamps(start_dtime, end_dtime, offset, interval, unit=offsets.Minute, closed=None):
    offset, freq = unit(float(offset)), unit(float(interval))
    offset_start = start_dtime + offset
    dtrange = pd.date_range(offset_start, end_dtime, freq=freq)
    return dtrange_between_time(dtrange, start_dtime.time(), end_dtime.time(), closed)


class KnownTimestampValidation(object):
    def __init__(self, timestamps=None, valid_timestamps=None):
        self._valid_timestamps, self._actual_timestamps = None, None
        self.valid_timestamps = valid_timestamps
        self.actual_timestamps = timestamps


    @property
    def valid_timestamps(self):
        return self._valid_timestamps

    @valid_timestamps.setter
    def valid_timestamps(self, value):
        if value is not None:
            valid_timestamps = pd.Index(value)
            if not valid_timestamps.is_unique:
                raise ValueError('Argument error: each of valid_timestamps must be unique')
            self._valid_timestamps = pd.Series(range(0, len(valid_timestamps)), index=valid_timestamps.sort_values())

    @property
    def actual_timestamps(self):
        return self._valid_timestamps

    @actual_timestamps.setter
    def actual_timestamps(self, value):
        if value is not None:
            self._actual_timestamps = pd.Series(range(0, len(value)), index=value)

    def invalids(self):
        valid_dtindex = self.valid_timestamps.index
        for ts in self.actual_timestamps.index:
            if not valid_dtindex.contains(ts):
                yield ts

    def gaps(self):
        actual_dtindex = self.actual_timestamps.index
        valid_dtindex = self.valid_timestamps.index
        for contains, grouper in groupby(valid_dtindex, lambda x: actual_dtindex.contains(x)):
            if not contains:
                ts_chunk = list(grouper)
                yield (ts_chunk[0], ts_chunk[-1])

    def reversions(self):
        rv_count = 0
        for ts in self.actual_timestamps.index:
            i_expected = self.valid_timestamps[ts]
            actual_indices = self.actual_timestamps[[ts]]
            for i_actual in actual_indices:
                if i_expected + rv_count != i_actual:
                    yield (ts, i_expected, i_actual)
                    rv_count += 1



# def validate_timestamps(timestamps, start_dtime, end_dtime, offset, interval, unit=offsets.Minute, closed=None):
#     valid_timestamps = pd.Series(index=all_valid_timestamps(start_dtime, end_dtime, offset, interval, unit, closed))
#
#     invalid_timestamps = []
#     for i, ts in enumerate(timestamps):
#         if ts not in valid_timestamps:
#             invalid_timestamps.append(ts)
#         elif pd.isnull(valid_timestamps[ts]):
#             valid_timestamps[ts] =
#         else:
#             valid_timestamps[ts] = i
#
#     return valid_timestamps, invalid_timestamps
#
#
# def check_intraday_gaps(valid_timestamps):
#     for isnull, grouper in groupby(valid_timestamps.index, lambda x: pd.isnull(valid_timestamps[x])):
#         if isnull:
#             ts_chunk = list(grouper)
#             yield (ts_chunk[0], ts_chunk[-1])
#
#
# def check_time_reversion(valid_timestamps):
#     actual, expected = pd.Series(), pd.Series()
#     sorted_tstamps = (for vts in valid_timestamps.index if not pd.isnull(valid_timestamps[vts]))
#     for i, ts in enumerate(sorted_tstamps):
#         expected.set_value(ts, i)
#         actual.set_value(valid_timestamps[ts], ts)
#
#     rv_count = 0
#     expected = valid_timestamps.index
#     for i_actual in range(0, len(actual)):
#         ts = actual[i_actual]
#         i_expected = expected[ts]
#         if i_expected + rv_count != i_actual:
#             yield ts
#             rv_count += 1



    # na_count, rv_count = 0, 0
    # for i_expected, ts in enumerate(valid_timestamps.index):
    #     i_actual = valid_timestamps[ts]
    #     if pd.isnull(i_actual):
    #         na_count += 1
    #     elif i_actual + na_count + rv_count != i_expected:
    #         yield ts
    #         rv_count += 1



