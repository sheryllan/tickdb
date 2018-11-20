import os

DIR = os.path.dirname(os.path.realpath(__file__))

BARXSL = os.path.join(DIR, 'bar_check.xsl')
TSXSL = os.path.join(DIR, 'timeseries_check.xsl')
BARHTML = os.path.join(DIR, 'bar_check.html')
TSHTML = os.path.join(DIR, 'timeseries_check.html')

TIMEZONE = None
WINDOW = ('18:00', '15:00')
WINDOW_TZ = 'America/Chicago'
SCHEDULE = 'ScheduleBase'


