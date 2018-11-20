from pandas.tseries.holiday import *
from pandas.tseries.offsets import CustomBusinessDay
from timeutils.commonfuncs import *



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
    calendar = GeneralCalendar()
    open_time = dt.time(17)
    close_time = dt.time(16)
    tzinfo = pytz.timezone('America/Chicago')
    weekmask = 'Sun Mon Tue Wed Thu Fri'



