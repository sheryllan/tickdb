from collections import OrderedDict
import pandas as pd
from pandas.tseries import offsets


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


def all_valid_timestamps(start_dtime, end_dtime, offset, interval, unit=offsets.Minute, closed=None):
    offset, interval = float(offset), float(interval)
    start_dtime = start_dtime + unit(offset)
    return pd.date_range(start_dtime, end_dtime, freq=unit(interval), closed=closed)


def validate_timestamps(timestamps, start_dtime, end_dtime, offset, interval, unit=offsets.Minute, closed=None):
    valid_timestamps = OrderedDict({ts: None for ts in
                                    all_valid_timestamps(start_dtime, end_dtime, offset, interval, unit, closed=closed)})

    invalid_timestamps = []
    for i, ts in enumerate(timestamps):
        if ts not in valid_timestamps:
            invalid_timestamps.append(ts)
        else:
            valid_timestamps[ts] = i

    return valid_timestamps, invalid_timestamps


def _slice_to_chunks(items, matchfunc):
    chunk = list()
    for item in items:
        if matchfunc(item):
            chunk.append(item)
        elif chunk:
            yield chunk
            chunk = list()
    if chunk:
        yield chunk


def check_intraday_gaps(valid_timestamps):
    for ts_chunk in _slice_to_chunks(valid_timestamps, lambda x: valid_timestamps[x] is None):
        yield (ts_chunk[0], ts_chunk[-1])


def check_time_reversion(valid_timestamps):
    na_count = 0
    for i_expected, (ts, i_actual) in enumerate(valid_timestamps.items()):
        if i_actual is None:
            na_count += 1
        elif i_actual + na_count < i_expected:
            yield ts



