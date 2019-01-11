import os

DIR = os.path.dirname(os.path.realpath(__file__))

SERVERNAME = 'lcmquantldn1'

TIMEZONE = 'UTC'
WINDOW = ('18:00', '15:00')
WINDOW_TZ = 'America/Chicago'
SCHEDULE = 'ScheduleBase'


BARXSL = os.path.join(DIR, 'bar_check.xsl')
TSXSL = os.path.join(DIR, 'timeseries_check.xsl')
BARHTML = os.path.join(DIR, 'bar_check.html')
TSHTML = os.path.join(DIR, 'timeseries_check.html')

BAR_TITLE = 'Bar Integrity Check Report'
TS_TITLE = 'Timeseries Integrity Check Report'

LOGIN = ('slan@liquidcapital.com', 'atnahqxjoemdtpqa')
RECIPIENTS = ['slan@liquidcapital.com']
