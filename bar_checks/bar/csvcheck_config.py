import os
from enum import Enum
from typing import NamedTuple


SERVERNAME = 'lcmquantldn1'


class SourceSpecificConfigs(NamedTuple):
    window: tuple = ('00:00', '23:59:59.999999')
    window_tz: str = 'UTC'
    schedule: str = 'BaseSchedule'


CME_CONFIGS = SourceSpecificConfigs(('18:00', '15:00'), 'America/Chicago', 'CMESchedule')
CHINA_CONFIGS = SourceSpecificConfigs(window_tz='Asia/Hong_Kong', schedule='ChinaSchedule')
EUREX_CONFIGS = SourceSpecificConfigs(window_tz='US/Central', schedule='EurexSchedule')
OSE_CONFIGS = SourceSpecificConfigs(window_tz='Asia/Tokyo', schedule='OSESchedule')
ASX_CONFIGS = SourceSpecificConfigs(window_tz='Australia/Sydney', schedule='ASXSchedule')


# WINDOW = ('18:00', '15:00')
# WINDOW_TZ = 'America/Chicago'
# SCHEDULE = 'BaseSchedule'

TIMEZONE = 'UTC'
SOURCE = 'qtg'

DIR = os.path.dirname(__file__)
REPORTS = 'reports'
BAR_REPORT_NAME = 'bar_check'
TS_REPORT_NAME = 'series_check'

BAR_TITLE = 'Bar Integrity Check Report'
TS_TITLE = 'Timeseries Integrity Check Report'

LOGIN = ('slan@liquidcapital.com', 'atnahqxjoemdtpqa')
RECIPIENTS = ['slan@liquidcapital.com']


class Report(str, Enum):
    ANNUAL = 'annual'
    DAILY = 'daily'
    DATES = 'dates'

    def __str__(self):
        return self._value_
