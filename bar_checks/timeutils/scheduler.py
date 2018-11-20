from pandas.core.tools.datetimes import to_datetime
from timeutils.commonfuncs import *
from commonlib import *


class BScheduler(object):
    def __init__(self, schedules=None):
        self._schedules = None
        self.set_schedule_configs(schedules)

    def set_schedule_configs(self, value):
        self._schedules = [] if not value else to_iter(value, ittype=tuple)


    def validate_schedule_times(self, start_time=None, end_time=None):
        start_time = MIN_TIME if start_time is None else start_time
        end_time = MAX_TIME if end_time is None else end_time
        return validate_time(start_time), validate_time(end_time)


    def primary_schedule(self, date, tz=None):
        if not self._schedules:
            return None
        for schedule in self._schedules:
            schedule_times = schedule.get_schedule_on(date)
            if schedule_times is not None:
                start, end = schedule_times
                return to_tz_datetime(start, to_tz=tz), to_tz_datetime(end, to_tz=tz)


    def get_raw_schedules(self, start_date, end_date, tz=None):
        last_schedule = None
        for ts in pd.date_range(start_date, end_date):
            schedule = self.primary_schedule(ts.date(), tz)
            if schedule is not None:
                if last_schedule is not None and last_schedule[1] > schedule[0]:
                    raise ValueError('Conflicting schedule times: {} > {}'.format(last_schedule[1], schedule[0]))
                yield schedule

            last_schedule = schedule


    def captime_by_window(self, schedule_open, schedule_close, start_time, end_time, window_tz=None, to_tz=pytz.UTC):
        schedule_open = pd.Timestamp(to_tz_datetime(schedule_open, to_tz=to_tz))
        schedule_close = pd.Timestamp(to_tz_datetime(schedule_close, to_tz=to_tz))

        window = timedelta_between(end_time, start_time)
        start_set = pd.Timestamp(to_tz_datetime(date=schedule_open.date(), time=start_time, from_tz=window_tz, to_tz=to_tz))
        if window < schedule_open - start_set:
            start_set += dt.timedelta(days=1)
        end_set = start_set + window

        start = start_set if start_set > schedule_open else schedule_open
        end = end_set if end_set < schedule_close else schedule_close
        while schedule_open <= start <= end <= schedule_close:
            if start != end:
                yield start, end
            start_set += dt.timedelta(days=1)
            end_set += dt.timedelta(days=1)
            start = start_set
            end = end_set

        if start <= schedule_close:
            yield start, schedule_close


    def get_schedules(self, start_date, end_date, start_time=None, end_time=None, window_tz=None, tz=pytz.UTC):
        start_date, end_date = to_datetime(start_date).date(), to_datetime(end_date).date()
        start_time, end_time = self.validate_schedule_times(start_time, end_time)

        lbound = to_tz_datetime(date=start_date, time=start_time, from_tz=window_tz, to_tz=tz)
        ubound = to_tz_datetime(date=end_date, time=end_time, from_tz=window_tz, to_tz=tz)

        for schedule_open, schedule_close in self.get_raw_schedules(last_n_days(1, start_date), last_n_days(-1, end_date)):
            for start, end in self.captime_by_window(schedule_open, schedule_close, start_time, end_time, window_tz, tz):
                if start >= lbound and end <= ubound:
                    yield (start, end)


class TimeBound(object):
    def __init__(self, schedules):
        self.schedule_dict = {s[0].date(): s for s in schedules}

    @staticmethod
    def closed_convert(closed):
        include_start, include_end = True, True
        if closed == 'left':
            include_end = False
        elif closed == 'right':
            include_start = False

        return include_start, include_end

    @staticmethod
    def isin_closed(value, start, end, closed):
        if not isinstance(value, dt.datetime):
            return False

        include_start, include_end = closed if isinstance(closed, tuple) else TimeBound.closed_convert(closed)
        left = value >= start if include_start else value > start
        right = value <= end if include_end else value < end
        return left and right

    def is_on_schedule(self, ts, closed):
        today = ts.date()
        if today in self.schedule_dict and TimeBound.isin_closed(ts, *self.schedule_dict[today], closed):
            return True

        yesterday = last_n_days(d=today)
        if yesterday in self.schedule_dict and TimeBound.isin_closed(ts, *self.schedule_dict[yesterday], closed):
            return True

        return False


    def filter_on_schedule(self, timestamps, closed):
        dtindex = timestamps if isinstance(timestamps, pd.DatetimeIndex) else pd.DatetimeIndex(timestamps)
        return dtindex[[self.is_on_schedule(i, closed) for i in dtindex]]

















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
