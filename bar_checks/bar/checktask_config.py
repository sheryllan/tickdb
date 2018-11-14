import os

DIR = os.path.dirname(os.path.realpath(__file__))

BARXSL = os.path.join(DIR, 'bar_check.xsl')
TSXSL = os.path.join(DIR, 'timeseries_check.xsl')
BARHTML = os.path.join(DIR, 'bar_check.html')
TSHTML = os.path.join(DIR, 'timeseries_check.html')

TIMEZONE = None
WINDOW = ('00:00', '21:00')
SCHEDULE = 'ScheduleBase'

