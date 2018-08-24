import unittest as ut
from holidaycalendar import *


class BScheduleTests(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        holidays = [Holiday('h1', month=7, day=1),
                    Holiday('h2', month=7, day=2),
                    Holiday('h3', month=8, day=9)]

        cls.calendar = GeneralCalendar()
        cls.calendar.rules.extend(holidays)

    def test_get_schedules(self):
        tz = pytz.timezone('America/Chicago')
        bscheduler = BScheduler(self.calendar, default=('18:00', '17:00'), tzinfo=tz)
        actual = list(bscheduler.get_schedules(dt.date(2018, 7, 1), dt.date(2018, 7, 9), dt.time(1), dt.time(22)))
        expected = [(pd.Timestamp('2018-07-03 18:00:00-0500'), pd.Timestamp('2018-07-03 22:00:00-0500')),
                    (pd.Timestamp('2018-07-04 01:00:00-0500'), pd.Timestamp('2018-07-04 17:00:00-0500')),
                    (pd.Timestamp('2018-07-04 18:00:00-0500'), pd.Timestamp('2018-07-04 22:00:00-0500')),
                    (pd.Timestamp('2018-07-05 01:00:00-0500'), pd.Timestamp('2018-07-05 17:00:00-0500')),
                    (pd.Timestamp('2018-07-05 18:00:00-0500'), pd.Timestamp('2018-07-05 22:00:00-0500')),
                    (pd.Timestamp('2018-07-06 01:00:00-0500'), pd.Timestamp('2018-07-06 17:00:00-0500'))
                    ]
        self.assertListEqual(expected, actual)
