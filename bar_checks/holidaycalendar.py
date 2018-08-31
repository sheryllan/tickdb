import datetime as dt
from pandas.tseries.holiday import *
from pandas.tseries import offsets
import pandas._libs.tslibs.offsets as liboffsets
from pandas.core.tools.datetimes import to_datetime
import pandas as pd
import pytz

from commonlib import *



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


class TimestampSchedule(pd.Timestamp):
    def __new__(cls, schedule, ts):
        if not isinstance(ts, pd.Timestamp):
            ts = to_datetime(ts)
        this = ts
        this.__class__ = cls
        this.schedule = schedule
        return this


class BScheduler(object):
    def __init__(self, calendar, default, tzinfo, custom=None):
        self.calendar = calendar
        self.tzinfo = tzinfo
        self.open, self.close = self.validate_schedule(*default)
        self.bd = offsets.CustomBusinessDay(calendar=calendar)

        self.offset_schd, self.date_schd = {}, {}
        if custom is not None:
            self.offset_schd = {k: self.validate_schedule(*v)
                                for k, v in custom.items() if isinstance(k, offsets.DateOffset)}
            self.date_schd = {to_datetime(k).date(): self.validate_schedule(*v)
                              for k, v in custom.items() if k not in self.offset_schd}

    def validate_schedule(self, start_time, end_time):
        return liboffsets._validate_business_time(start_time), \
               liboffsets._validate_business_time(end_time)

    def dates_schedule(self, start, end):
        for ts in pd.date_range(start, end, tz=self.tzinfo):
            if ts.date() in self.date_schd:
                yield TimestampSchedule(self.date_schd[ts.date()], ts)
            else:
                on_offset = find_first_n(self.offset_schd.items(), lambda x: x[0].onOffset(ts))
                if on_offset:
                    yield TimestampSchedule(on_offset[1], ts)
                elif self.bd.onOffset(ts):
                    yield TimestampSchedule((self.open, self.close), ts)

    def get_raw_schedules(self, start_date, end_date, tz=None):
        delta = timedelta_between(self.close, self.open)
        open_start = self.tzinfo.localize(dt.datetime.combine(start_date, self.open))
        open_end = self.tzinfo.localize(dt.datetime.combine(end_date, self.open))
        for open_ts in self.dates_schedule(open_start, open_end):
            close_ts = open_ts + delta
            if self.bd.onOffset(close_ts):
                yield (open_ts, close_ts) if tz is None else (open_ts.astimezone(tz), close_ts.astimezone(tz))

    def captime_by_window(self, schedule_open, schedule_close, start_time, end_time):
        if start_time is None and end_time is None:
            return schedule_open, schedule_close
        elif start_time is None:
            window = timedelta_between(end_time, schedule_open.time())
            return schedule_open, min(schedule_close, schedule_open + window)
        elif end_time is None:
            window = timedelta_between(schedule_close.time(), start_time)
            return max(schedule_open, schedule_close - window), schedule_close
        else:
            window = timedelta_between(schedule_close.time(), schedule_open.time())

            left_diff = timedelta_between(start_time, schedule_open.time())
            start = schedule_open + left_diff if left_diff < window else schedule_open

            right_diff = timedelta_between(schedule_close.time(), end_time)
            end = schedule_close - right_diff if right_diff < window else schedule_close
            return start, end

    def get_schedules(self, start_date, end_date, start_time=None, end_time=None, tz=None):
        if not (isinstance(start_date, dt.date) and isinstance(end_date, dt.date)):
            raise ValueError('Input start_date and end_date must both be datetime.date object')
        if not ((start_time is None or isinstance(start_time, dt.time)) and
                (end_time is None or isinstance(end_time, dt.time))):
            raise ValueError('If start_time or end_time is set, the value must be a datetime.time object')

        local_start_date = start_date if start_time is None \
            else to_tz_datetime(date=start_date, time=start_time, from_tz=tz, to_tz=self.tzinfo).date()
        local_end_date = end_date if end_time is None \
            else to_tz_datetime(date=end_date, time=end_time, from_tz=tz, to_tz=self.tzinfo).date()

        for schedule_open, schedule_close in self.get_raw_schedules(local_start_date, local_end_date, tz):
            start, end = self.captime_by_window(schedule_open, schedule_close, start_time, end_time)
            if end.date() <= end_date:
                yield (start, end)


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
    custom_schedule = {offsets.Week(weekday=6): ('17:00', '16:00')}



# class TzCustomBussinessHour(CustomBusinessHour):
#     def __init__(self, tz=None, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.tz = tz
#
#     def update_bshours(self, tzdate):
#         self.start = to_tz_datetime(date=tzdate, time=self.start, from_tz=self.tz)
#         self.end = to_tz_datetime(date=tzdate, time=self.end, from_tz=self.tz)


# class MultiOffsets(offsets.DateOffset):
#     @property
#     def _prefix(self):
#         pass
#
#     def __init__(self, custom, n=1, normalize=False, **kwds):
#         super().__init__(n, normalize, **kwds)
#         self.offset_schd = {k: custom[k] for k in custom if isinstance(k, offsets.DateOffset)}
#         self.date_schd = {to_datetime(k).date(): custom[k] for k in custom if k not in self.offset_schd}
#
#     def apply(self, other):
#         schedule, ts = min(map(lambda x: (x[1], x[0].apply(other)), self.offset_schd.items()), key=lambda x: x[1])
#         tomorrow = to_datetime(other) + dt.timedelta(1)
#         if self.normalize:
#             tomorrow = tomorrow.normalize()
#         if tomorrow.date() in self.date_schd:
#             schedule, ts = (self.date_schd[tomorrow.date()], tomorrow) if tomorrow < ts else (schedule, ts)
#         return TimestampSchedule(schedule, ts)
#
#     def onOffset(self, dt):
#         return dt in self.date_schd or any(o.onOffset(dt) for o in self.offset_schd)
#
#     def to_timestampschedule(self, ts):
#         if not isinstance(ts, pd.Timestamp):
#             ts = to_datetime(ts)
#         if ts.date() in self.date_schd:
#             return TimestampSchedule(self.date_schd[ts.date()], ts)
#
#         for o in self.offset_schd:
#             if o.onOffset(ts):
#                 return TimestampSchedule(self.offset_schd[o], ts)
#
#         return TimestampSchedule(None, ts)



# sundays = offsets.Week(weekday=6)
# bd = offsets.BusinessDay()
# multi = MultiOffsets({bd: (dt.time(18), dt.time(17)), sundays: (dt.time(17), dt.time(16))})
# ddd = pd.date_range(dt.date(2018, 6, 20), dt.date(2018, 7, 1), freq=multi)
# for d in ddd:
#     print(d)
#     print(getattr(d, 'schedule', None))

# a = list(BScheduler(GeneralCalendar(), (CMESchedule.open_time, CMESchedule.close_time), CMESchedule.tzinfo)
#          .get_schedules(dt.date(2018, 6, 1), dt.date(2018, 7, 2), dt.time(0), dt.time(22), tz=pytz.UTC))
# print()