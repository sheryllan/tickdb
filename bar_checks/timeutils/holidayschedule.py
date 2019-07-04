from pandas.tseries.holiday import *
from pandas.tseries.offsets import CustomBusinessDay
from .commonfuncs import *


NewYear = Holiday('New Years Day', month=1, day=1, observance=nearest_workday)
Xmas = Holiday('Christmas', month=12, day=25, observance=nearest_workday)


class GeneralCalendar(AbstractHolidayCalendar):
    rules = [
        NewYear,
        Xmas
    ]


def add_metaclass(metaclass):
    """Class decorator for creating a class with a metaclass."""
    def wrapper(cls):
        return metaclass(cls.__name__, cls.__bases__, dict(cls.__dict__))
    return wrapper


rschedules = {}


def register(cls):
    try:
        name = cls.name
    except AttributeError:
        name = cls.__name__
    rschedules[name] = cls


def get_schedule(name, filename=None):
    if isinstance(name, BaseSchedule):
        return name

    if name not in rschedules:
        build_schedule(name, filename)
    return rschedules[name]()


# TODO  build a schedule class from reference data
def build_schedule(name, filename):
    pass


class ScheduleMetaClass(type):

    def __new__(cls, name, bases, attrs):
        schedule_class = super(ScheduleMetaClass, cls).__new__(cls, name, bases, attrs)
        register(schedule_class)
        return schedule_class


@add_metaclass(ScheduleMetaClass)
class BaseSchedule(object):

    __metaclass__ = ScheduleMetaClass
    calendar = None
    open_time = MIN_TIME
    close_time = MAX_TIME
    tzinfo = pytz.UTC
    weekmask = 'Mon Tue Wed Thu Fri'

    def __init__(self):
        self.cbd = CustomBusinessDay(calendar=self.calendar, weekmask=self.weekmask)
        self.delta = timedelta_between(self.close_time, self.open_time)

    def get_schedule_on(self, date):
        start = to_tz_datetime(date=date, time=self.open_time, to_tz=self.tzinfo)
        end = start + self.delta
        if self.cbd.onOffset(start) and self.cbd.onOffset(end):
            return start, end


class CMESchedule(BaseSchedule):
    Dec22nd = Holiday('Dec 22nd', month=12, day=22)
    Dec23rd = Holiday('Dec 23rd', month=12, day=23)
    Dec24th = Holiday('Dec 24th', month=12, day=24)
    Dec26th = Holiday('Dec 26th', month=12, day=26)
    Dec27th = Holiday('Dec 27th', month=12, day=27)
    Dec28th = Holiday('Dec 28th', month=12, day=28)
    Dec29th = Holiday('Dec 29th', month=12, day=29)
    Dec30th = Holiday('Dec 30th', month=12, day=30)
    Dec31st = Holiday('Dec 31st', month=12, day=31)

    calendar = GeneralCalendar(
        'CMECalendar',
        [Dec22nd, Dec23rd, Dec24th, Xmas, Dec26th, Dec27th,
         Dec28th, Dec29th, Dec29th, Dec30th, Dec31st, NewYear])

    open_time = dt.time(17)
    close_time = dt.time(16)
    tzinfo = pytz.timezone('America/Chicago')
    weekmask = 'Sun Mon Tue Wed Thu Fri'


class ChinaSchedule(BaseSchedule):
    calendar = GeneralCalendar('ChinaCalendar')
    open_time = dt.time(9, 30)
    close_time = dt.time(15)
    tzinfo = pytz.timezone('Asia/Shanghai')


class EurexSchedule(BaseSchedule):
    calendar = GeneralCalendar('EurexCalendar')
    open_time = dt.time(8, 5)
    close_time = dt.time(18, 55)
    tzinfo = pytz.timezone('CET')


class OSESchedule(BaseSchedule):
    calendar = GeneralCalendar('OSECalendar')
    open_time = dt.time(16, 30)
    close_time = dt.time(15, 15)
    tzinfo = pytz.timezone('Asia/Tokyo')


class ASXSchedule(BaseSchedule):
    calendar = GeneralCalendar('ASXCalendar')
    open_time = dt.time(17)
    close_time = dt.time(16, 30)
    tzinfo = pytz.timezone('Australia/Sydney')

