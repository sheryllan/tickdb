import os
from enum import Enum

DIR = os.path.dirname(os.path.realpath(__file__))

SERVERNAME = 'lcmquantldn1'

TIMEZONE = 'UTC'
WINDOW = ('18:00', '15:00')
WINDOW_TZ = 'America/Chicago'
SCHEDULE = 'ScheduleBase'

BARHTML = 'bar_check.html'
TSHTML = 'timeseries_check.html'

BAR_TITLE = 'Bar Integrity Check Report'
TS_TITLE = 'Timeseries Integrity Check Report'

LOGIN = ('slan@liquidcapital.com', 'atnahqxjoemdtpqa')
RECIPIENTS = ['slan@liquidcapital.com']

REPORT_DIR = os.path.join(DIR, 'reports')
os.makedirs('reports', exist_ok=True)


class Report(Enum):
    ANNUAL = 'annual'
    DAILY = 'daily'
    DATES = 'dates'

