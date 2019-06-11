from pandas.core.tools.datetimes import to_datetime
from .commonfuncs import *
from .holidayschedule import get_schedule
from ..commonlib import *


class BScheduler(object):
    def __init__(self, schedules=None):
        self.schedules = schedules

    @property
    def schedules(self):
        return self._schedules

    @schedules.setter
    def schedules(self, value):
        self._schedules = [get_schedule(*to_iter(v, ittype=tuple)) for v in to_iter(value, ittype=iter)] \
            if value else []

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

    def get_raw_schedules(self, start_date: dt.date, end_date: dt.date, tz=None):
        last_schedule = None
        for ts in pd.date_range(start_date, end_date):
            schedule = self.primary_schedule(ts.date(), tz)
            if schedule is not None:
                if last_schedule is not None and last_schedule[1] > schedule[0]:
                    raise ValueError('Conflicting schedule times: {} > {}'.format(last_schedule[1], schedule[0]))
                yield schedule

            last_schedule = schedule

    def captime_by_window(self, schedule_open, schedule_close, window_start, window_end, window_tz=None, to_tz=pytz.UTC):
        schedule_open = to_tz_datetime(schedule_open, to_tz=to_tz, to_orig=False)
        schedule_close = to_tz_datetime(schedule_close, to_tz=to_tz, to_orig=False)

        window = timedelta_between(window_end, window_start)
        start_set = to_tz_datetime(date=schedule_open.date(), time=window_start, from_tz=window_tz, to_tz=to_tz)
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

    def get_schedules(self, start_dt, end_dt, window_start=None, window_end=None, window_tz=None, tz=pytz.UTC):
        start_dt, end_dt = to_datetime(start_dt), to_datetime(end_dt)
        start_dt, end_dt = to_tz_datetime(start_dt, to_tz=tz), to_tz_datetime(end_dt, to_tz=tz)
        start_date, end_date = last_n_days(1, start_dt.date()), last_n_days(-1, end_dt.date())
        window_start, window_end = self.validate_schedule_times(window_start, window_end)

        lbound = to_tz_datetime(date=start_date, time=window_start, from_tz=window_tz, to_tz=tz)
        ubound = to_tz_datetime(date=end_date, time=window_end, from_tz=window_tz, to_tz=tz)
        lbound, ubound = max(lbound, start_dt), min(ubound, end_dt)

        for raw_open, raw_close in self.get_raw_schedules(start_date, end_date, tz):
            for start, end in self.captime_by_window(raw_open, raw_close, window_start, window_end, window_tz, tz):
                if end < lbound or start > ubound:
                    continue

                yield max(start, lbound), min(end, ubound)


class ScheduleBound(object):
    def __init__(self, schedules, closed=None, tz=None):
        self.closed = closed
        self._tz = tz
        self._schedule_list, self._schedule_dict = None, None
        self.set_schedules(schedules)

    @property
    def schedule_list(self):
        return self._schedule_list.copy()

    @property
    def tz(self):
        return self._tz

    @tz.setter
    def tz(self, value):
        if isinstance(value, str):
            value = pytz.timezone(value)
        if value != self.tz:
            self._tz = value
            self.set_schedules(self._schedule_list)

    def set_schedules(self, schedules):
        self._schedule_list = SortedList()
        self._schedule_dict = defaultdict(SortedList)
        self.update_schedules(schedules)

    def update_schedules(self, schedules):
        if not schedules:
            return

        for s in to_iter(schedules, ittype=iter):
            s_tz = to_tz_datetime(s[0], to_tz=self.tz), to_tz_datetime(s[1], to_tz=self.tz)
            start_date, end_date = s_tz[0].date(), s_tz[1].date()

            if start_date in self._schedule_dict:
                if s_tz in self._schedule_dict[start_date] and s_tz in self._schedule_dict[end_date]:
                    continue

                if end_date in self._schedule_dict:
                    if s_tz in self._schedule_dict[start_date] != s_tz in self._schedule_dict[end_date]:
                        raise KeyError('Inconsistent schedule time found in _schedule_dict')

            self._schedule_list.add(s_tz)
            self._schedule_dict[start_date].add(s_tz)
            if start_date != end_date:
                self._schedule_dict[end_date].add(s_tz)

    def enclosing_schedule(self, ts):
        ts = to_tz_datetime(ts, to_tz=self.tz)
        today = ts.date()
        if today in self._schedule_dict:
            for schedule in self._schedule_dict[today]:
                if isin_closed(ts, schedule[0], schedule[1], self.closed):
                    return schedule

    def is_on_schedule(self, ts):
        return self.enclosing_schedule(ts) is not None



