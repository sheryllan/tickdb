import os
from enum import Enum

SERVERNAME = 'lcmquantldn1'

TIMEZONE = 'UTC'
WINDOW = ('18:00', '15:00')
WINDOW_TZ = 'America/Chicago'
SCHEDULE = 'ScheduleBase'

DIR = os.path.dirname(__file__)
REPORTS = 'reports'
BAR_REPORT_NAME = 'bar_check'
TS_REPORT_NAME = 'series_check'

BAR_TITLE = 'Bar Integrity Check Report'
TS_TITLE = 'Timeseries Integrity Check Report'

LOGIN = ('slan@liquidcapital.com', 'atnahqxjoemdtpqa')
RECIPIENTS = ['slan@liquidcapital.com']


class Report(Enum):
    ANNUAL = 'annual'
    DAILY = 'daily'
    DATES = 'dates'

