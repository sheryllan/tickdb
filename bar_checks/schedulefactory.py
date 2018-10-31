from holidaycalendar import *


def add_metaclass(metaclass):
    """Class decorator for creating a class with a metaclass."""
    def wrapper(cls):
        return metaclass(cls.__name__, cls.__bases__, dict(cls.__dict__))
    return wrapper


schedules = {}


def register(cls):
    try:
        name = cls.name
    except AttributeError:
        name = cls.__name__
    schedules[name] = cls


def get_schedule(name, filename=None):
    if name not in schedules:
        build_schedule(name, filename)
    return schedules[name]()


# TODO  build a schedule class from reference data
def build_schedule(name, filename):
    pass


class ScheduleMetaClass(type):

    def __new__(cls, name, bases, attrs):
        schedule_class = super(ScheduleMetaClass, cls).__new__(cls, name, bases, attrs)
        register(schedule_class)
        return schedule_class


@add_metaclass(ScheduleMetaClass)
class ScheduleBase(object):

    __metaclass__ = ScheduleMetaClass
    calendar = GeneralCalendar()
    open_time = '00:00'
    close_time = '00:00'
    tzinfo = pytz.UTC
    custom_schedule = {}


class CMESchedule(ScheduleBase):
    open_time = '18:00'
    close_time = '17:00'
    tzinfo = pytz.timezone('America/Chicago')
    custom_schedule = {offsets.Week(weekday=6): ('17:00', '16:00')}


