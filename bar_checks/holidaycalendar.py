from pandas.tseries.holiday import *
from pandas.tseries.offsets import CustomBusinessHour
import pytz


class TzCustomBussinessHour(CustomBusinessHour):
    def __init__(self, tz=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        local_start = tz.localize(self.start)
        local_end = tz.localize(self.end)
        self.start = local_start.astimezone(pytz.UTC).replace(tzinfo=None)
        self.end = local_end.astimezone(pytz.UTC).



class BSchedule(object):
    def __init__(self, calendar, default, custom, tzinfo=None):

        self.calendar = calendar
        if tzinfo is not None:
            pass
        # local_start, local_end = liboffsets._validate_business_time(default[0],)
        # utc_start, utc_end =
        # self.cbh = CustomBusinessHour(start=default[0], end=default[1], calendar=calendar)




NewYear = Holiday('New Years Day', month=1, day=1, observance=nearest_workday)
Xmas = Holiday('Christmas', month=12, day=25, observance=nearest_workday)


class GeneralCalendar(AbstractHolidayCalendar):
    rules = [
        NewYear,
        Xmas
    ]