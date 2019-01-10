import datetime as dt
import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta
import re


MIN_TIME = dt.time(0)
MAX_TIME = dt.time(23, 59, 59, 999999)


def last_n_days(n=1, d=dt.date.today()):
    return d + relativedelta(days=-n)


def fmt_date(year, month=None, day=1, fmt='%Y%m'):
    if month is None:
        return str(year)
    return dt.date(int(year), int(month), int(day)).strftime(fmt)


def validate_time(time):
    if isinstance(time, str):
        return dt.time(*map(int, re.findall('[0-9]+', time)))
    elif isinstance(time, (tuple, list)):
        return dt.time(*time)
    elif isinstance(time, dt.time):
        return time
    else:
        raise TypeError('Invalid time type: must be type of str, or tuple/list, or datetime.time')


def to_tz_datetime(dttm=None, date=None, time=None, from_tz=None, to_tz=pytz.UTC):
    if dttm is not None and date is not None:
        raise ValueError('Argument datetime and date and time together should not both be set')
    elif dttm is not None:
        from_dttm = dttm
    elif date is not None:
        time = MIN_TIME if time is None else validate_time(time)
        from_dttm = dt.datetime.combine(date, time)
    else:
        return None

    if from_tz is not None:
        from_dttm = from_tz.localize(from_dttm.replace(tzinfo=None))

    if to_tz is None:
        return from_dttm

    return from_dttm.astimezone(to_tz) if from_dttm.tzinfo is not None else to_tz.localize(from_dttm)


def to_tz_series(timeseries, from_tz=None, to_tz=pytz.UTC):
    dtindex = pd.DatetimeIndex(timeseries)
    if dtindex.tz == to_tz:
        return dtindex

    # if from_tz is None, do nothing
    if from_tz is not None:
        if dtindex.tz is None:
            dtindex.tz_localize(from_tz)
        elif dtindex.tz is not None:
            dtindex.tz_convert(from_tz)

    if dtindex.tz is None:
        return dtindex.tz_localize(to_tz)
    elif dtindex.tz != to_tz:
        return dtindex.tz_convert(to_tz)
    else:
        return dtindex


def timedelta_between(time1, time2, allow_negative=False):
    time1, time2 = validate_time(time1), validate_time(time2)
    tdiff = dt.datetime.combine(dt.date.today(), time1) - dt.datetime.combine(dt.date.today(), time2)
    total_seconds = tdiff.total_seconds() if allow_negative else tdiff.total_seconds() % (3600 * 24)
    return dt.timedelta(seconds=total_seconds)


def ceildiv(a, b):
    return -(-a // b)


def closed_convert(closed):
    if isinstance(closed, tuple):
        return tuple(x if x is not None else True for x in closed)

    include_start, include_end = True, True
    if closed == 'left':
        include_end = False
    elif closed == 'right':
        include_start = False

    return include_start, include_end


def isin_closed(value: dt.datetime, start: dt.datetime=None, end: dt.datetime=None, closed=None):
    if value.tzinfo is None:
        value = to_tz_datetime(value, to_tz=start.tzinfo)
    include_start, include_end = closed_convert(closed)

    left = True if start is None else (value >= start if include_start else value > start)
    right = True if end is None else (value <= end if include_end else value < end)
    return left and right

# def last_n_years(n=1, d=dt.date.today()):
#     return d + relativedelta(years=-n)
#
#
# def last_n_months(n=1, d=dt.date.today()):
#     return d + relativedelta(months=-n)