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


class ScheduleBound(object):
    def __init__(self, schedules, closed=None, tz=None):
        self.closed = self.closed_convert(closed)
        self.tz = tz
        self._schedule_dict = None
        self.schedule_dict = schedules


    @property
    def schedule_dict(self):
        return self._schedule_dict

    @schedule_dict.setter
    def schedule_dict(self, value):
        self._schedule_dict = defaultdict(SortedList)
        if value is not None:
            for start, end in value:
                tz_start = to_tz_datetime(start, to_tz=self.tz)
                tz_end = to_tz_datetime(end, to_tz=self.tz)
                self._schedule_dict[tz_start.date()].add((tz_start, tz_end))

    @staticmethod
    def closed_convert(closed):
        if isinstance(closed, tuple):
            return closed

        include_start, include_end = True, True
        if closed == 'left':
            include_end = False
        elif closed == 'right':
            include_start = False

        return include_start, include_end

    @staticmethod
    def isin_closed(value: dt.datetime, start: dt.datetime, end: dt.datetime, closed):
        if value.tzinfo is None:
            value = to_tz_datetime(value, to_tz=start.tzinfo)
        include_start, include_end = ScheduleBound.closed_convert(closed)
        left = value >= start if include_start else value > start
        right = value <= end if include_end else value < end
        return left and right

    def schedules_on_date(self, ts):
        ts = to_tz_datetime(ts, to_tz=self.tz)
        today = ts.date()
        if today in self._schedule_dict:
            return self._schedule_dict[today]

        yesterday = last_n_days(d=today)
        if yesterday in self._schedule_dict:
            return self._schedule_dict[yesterday]
        return []

    def enclosing_schedule(self, ts):
        for schedule_time in self.schedules_on_date(ts):
            if schedule_time and ScheduleBound.isin_closed(ts, schedule_time[0], schedule_time[1], self.closed):
                return schedule_time

    def is_on_schedule(self, ts):
        return self.enclosing_schedule(ts) is not None

    def bound_indices(self, dtindex : pd.DatetimeIndex):
        if dtindex.empty:
            return None, None

        i, j = 0, len(dtindex)
        not_head = not self.is_on_schedule(dtindex[i])
        not_tail = not self.is_on_schedule(dtindex[j - 1])
        while i < j and (not_head or not_tail):
            if not_head:
                i += 1
            if not_tail:
                j -= 1
            not_head = not self.is_on_schedule(dtindex[i])
            not_tail = not self.is_on_schedule(dtindex[j - 1])

        return i, j



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
