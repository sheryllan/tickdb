import os

DIR = os.getcwd()

BARXSL = os.path.join(DIR, 'bar_check.xsl')
TSXSL = os.path.join(DIR, 'timeseries_check.xsl')
BARHTML = os.path.join(DIR, 'bar_check.html')
TSHTML = os.path.join(DIR, 'timeseries_check.html')

TIMEZONE = None
WINDOW = ('00:00', '21:00')
SCHEDULE = 'ScheduleBase'

BAR_TITILE = 'Bar Integrity Check Report'
TS_TITLE = 'Timeseries Integrity Check Report'

SENDER = 'slan@liquidcapital.com'
RECIPIENTS = ['slan@liquidcapital.com']
