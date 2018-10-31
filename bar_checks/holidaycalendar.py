from pandas.tseries.holiday import *
from pandas.tseries import offsets
import re
from pandas.core.tools.datetimes import to_datetime
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


def timedelta_between(time1, time2, allow_negative=False):
    tdiff = dt.datetime.combine(dt.date.today(), time1) - dt.datetime.combine(dt.date.today(), time2)
    total_seconds = tdiff.total_seconds() if allow_negative else tdiff.total_seconds() % (3600 * 24)
    return dt.timedelta(seconds=total_seconds)


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

    def validate_time(self, time):
        if isinstance(time, str):
            return dt.time(*map(int, re.findall('[0-9]+', time)))
        elif isinstance(time, (tuple, list)):
            return dt.time(*time)
        elif isinstance(time, dt.time):
            return time
        else:
            raise TypeError('Invalid time type: must be type of str, or tuple/list, or datetime.time')

    def validate_schedule(self, start_time=None, end_time=None):
        start_time = dt.time(0) if start_time is None else start_time
        end_time = dt.time(23, 59, 59, 999999) if end_time is None else end_time
        return self.validate_time(start_time), self.validate_time(end_time)

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
        open_start = to_tz_datetime(date=start_date, time=self.open, to_tz=self.tzinfo)
        open_end = to_tz_datetime(date=end_date, time=self.open, to_tz=self.tzinfo)
        for open_ts in self.dates_schedule(open_start, open_end):
            close_ts = open_ts + delta
            if self.bd.onOffset(close_ts):
                yield (open_ts, close_ts) if tz is None else (open_ts.astimezone(tz), close_ts.astimezone(tz))

    def captime_by_window(self, schedule_open, schedule_close, start_time=None, end_time=None):
        schedule_open, schedule_close = pd.Timestamp(schedule_open), pd.Timestamp(schedule_close)
        start_time, end_time = self.validate_schedule(start_time, end_time)

        tz, date = schedule_open.tz, schedule_open.date()
        window = timedelta_between(end_time, start_time)
        if window < timedelta_between(schedule_open.time(), start_time):
            date += dt.timedelta(days=1)

        start_set = pd.Timestamp(to_tz_datetime(date=date, time=start_time, to_tz=tz))
        end_set = start_set + window

        start = start_set if start_set > schedule_open else schedule_open
        end = end_set if end_set < schedule_close else schedule_close

        while schedule_open <= start <= end <= schedule_close:
            yield start, end
            start_set += dt.timedelta(days=1)
            end_set += dt.timedelta(days=1)
            start = start_set
            end = end_set

        if start <= schedule_close:
            yield start, schedule_close


    def get_schedules(self, start_date, end_date, start_time=None, end_time=None, tz=None):
        start_date, end_date = pd.Timestamp(start_date).date(), pd.Timestamp(end_date).date()

        local_start_date = to_tz_datetime(date=start_date, time=start_time, from_tz=tz, to_tz=self.tzinfo).date()
        local_end_date = to_tz_datetime(date=end_date, time=end_time, from_tz=tz, to_tz=self.tzinfo).date()

        for schedule_open, schedule_close in self.get_raw_schedules(local_start_date, local_end_date, tz):
            for start, end in self.captime_by_window(schedule_open, schedule_close, start_time, end_time):
                if end.date() <= end_date:
                    yield (start, end)


NewYear = Holiday('New Years Day', month=1, day=1, observance=nearest_workday)
Xmas = Holiday('Christmas', month=12, day=25, observance=nearest_workday)


class GeneralCalendar(AbstractHolidayCalendar):
    rules = [
        NewYear,
        Xmas
    ]






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