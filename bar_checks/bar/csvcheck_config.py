import os
from enum import Enum
from typing import NamedTuple

# SERVERNAME = 'lcmquantldn1'
SERVERNAME = 'lcmint-quantsim1'


class SourceSpecificConfigs(NamedTuple):
    window: tuple = ('00:00', '23:59:59')
    window_tz: str = 'UTC'
    schedule: str = 'BaseSchedule'


CME_CONFIGS = SourceSpecificConfigs(('18:00', '15:00'), 'America/Chicago', 'CMESchedule')
CHINA_CONFIGS = SourceSpecificConfigs(('09:30', '15:00'), 'Asia/Shanghai', 'ChinaSchedule')
EUREX_CONFIGS = SourceSpecificConfigs(('08:05', '18:55'), 'CET', 'EurexSchedule')
OSE_CONFIGS = SourceSpecificConfigs(window_tz='Asia/Tokyo', schedule='OSESchedule')
ASX_CONFIGS = SourceSpecificConfigs(window_tz='Australia/Sydney', schedule='ASXSchedule')


TIMEZONE = 'UTC'
SOURCE = 'cme_reactor'

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
