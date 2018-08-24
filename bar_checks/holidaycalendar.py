import datetime as dt
from pandas.tseries.holiday import *
from pandas.tseries.offsets import CustomBusinessDay
import pandas._libs.tslibs.offsets as liboffsets
import pandas as pd
import pytz


def to_tz_datetime(dttm=None, date=None, time=None, from_tz=None, to_tz=pytz.UTC):
    if dttm is not None and date is not None:
        raise ValueError('Argument datetime and date and time together should not both be set')
    elif dttm is not None:
        from_dttm = dttm
    elif date is not None:
        time = dt.time(0) if time is None else time
        from_dttm = datetime.combine(date, time)
    else:
        raise ValueError('Either argument datetime or date and time together should be set)')

    if from_dttm.tzinfo is None and from_tz is None:
        return to_tz.localize(from_dttm)
    elif from_tz is not None:
        return from_tz.localize(from_dttm.replace(tzinfo=None)).astimezone(to_tz)
    else:
        return from_dttm.astimezone(to_tz)


def timedelta_between(time1, time2):
    tdiff = dt.datetime.combine(dt.date.today(), time1) - dt.datetime.combine(dt.date.today(), time2)
    return dt.timedelta(seconds=tdiff.total_seconds() % (3600 * 24))


class BScheduler(object):
    def __init__(self, calendar, default, tzinfo, custom=None):

        self.calendar = calendar
        self.tzinfo = tzinfo
        self.open = liboffsets._validate_business_time(default[0])
        self.close = liboffsets._validate_business_time(default[1])
        self.bd = CustomBusinessDay(calendar=calendar)
        self.custom = custom if custom is not None else dict()

    def get_raw_schedules(self, start_date, end_date, tz=None):
        delta = timedelta_between(self.close, self.open)
        open_start = dt.datetime.combine(start_date, self.open)
        open_end = dt.datetime.combine(end_date, self.open)
        for open_ts in pd.bdate_range(open_start, open_end,
                                      normalize=False, freq=self.bd, tz=self.tzinfo):
            close_ts = open_ts + delta
            if self.bd.onOffset(close_ts):
                yield (open_ts, close_ts) if tz is None else (open_ts.astimezone(tz), close_ts.astimezone(tz))

    def captime_by_window(self, schedule_open, schedule_close, start_time, end_time):
        if start_time is None and end_time is None:
            yield (schedule_open, schedule_close)
        elif start_time is None:
            window = timedelta_between(end_time, schedule_open.time())
            yield (schedule_open, min(schedule_close, schedule_open + window))
        elif end_time is None:
            window = timedelta_between(schedule_close.time(), start_time)
            yield (max(schedule_open, schedule_close - window), schedule_close)
        else:
            window = timedelta_between(end_time, start_time)
            start_ts = schedule_open.combine(schedule_open, start_time).tz_localize(schedule_open.tzinfo)
            if start_ts >= schedule_close:
                return iter(())
            for istart in pd.date_range(start_ts, schedule_close, freq='D', closed='left'):
                start = max(schedule_open, istart)
                end = min(schedule_close, istart + window)
                if start < end:
                    yield (start, end)

    def get_schedules(self, start_date, end_date, start_time=None, end_time=None, tz=None):
        if not (isinstance(start_date, dt.date) and isinstance(end_date, dt.date)):
            raise ValueError('Input start_date and end_date must both be datetime.date object')
        if not ((start_time is None or isinstance(start_time, dt.time)) and
                (end_time is None or isinstance(end_time, dt.time))):
            raise ValueError('If start_time or end_time is set, the value must be a datetime.time object')

        local_start = self.tzinfo.localize(dt.datetime.combine(start_date, self.open)) if start_time is None \
            else to_tz_datetime(date=start_date, time=start_time, from_tz=tz, to_tz=self.tzinfo)
        local_end = self.tzinfo.localize(dt.datetime.combine(end_date, self.close)) if end_time is None \
            else to_tz_datetime(date=end_date, time=end_time, from_tz=tz, to_tz=self.tzinfo)

        for schdule_open, schedule_close in self.get_raw_schedules(local_start.date(), local_end.date(), tz):
            yield from self.captime_by_window(schdule_open, schedule_close, start_time, end_time)


NewYear = Holiday('New Years Day', month=1, day=1, observance=nearest_workday)
Xmas = Holiday('Christmas', month=12, day=25, observance=nearest_workday)


class GeneralCalendar(AbstractHolidayCalendar):
    rules = [
        NewYear,
        Xmas
    ]


class CMESchedule(object):
    calendar = GeneralCalendar()
    open_time = '18:00'
    close_time = '17:00'
    tzinfo = pytz.timezone('America/Chicago')
    custom_schedule = None

# class TzCustomBussinessHour(CustomBusinessHour):
#     def __init__(self, tz=None, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.tz = tz
#
#     def update_bshours(self, tzdate):
#         self.start = to_tz_datetime(date=tzdate, time=self.start, from_tz=self.tz)
#         self.end = to_tz_datetime(date=tzdate, time=self.end, from_tz=self.tz)
