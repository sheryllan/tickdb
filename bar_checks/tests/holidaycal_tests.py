import unittest as ut
from unittest.mock import Mock, MagicMock

from timeutils.scheduler import *
from timeutils.holidayschedule import *


class ScheduleTests(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cme_schedule = get_schedule('CMESchedule')


    def test_get_schedule_not_onOffset(self):
        actual = self.cme_schedule.get_schedule_on(dt.date(2018, 7, 7))
        self.assertTrue(actual is None)

        actual = self.cme_schedule.get_schedule_on(dt.date(2025, 12, 25))
        self.assertTrue(actual is None)

        actual = self.cme_schedule.get_schedule_on(dt.date(2000, 1, 1))
        self.assertTrue(actual is None)

    def test_get_schedule_onOffset(self):
        tz = pytz.timezone('America/Chicago')

        actual = self.cme_schedule.get_schedule_on(dt.date(2018, 7, 8))
        expected = tz.localize(dt.datetime(2018, 7, 8, 17)), tz.localize(dt.datetime(2018, 7, 9, 16))
        self.assertEqual(actual, expected)



class BScheduleTests(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.holidays_extra = [dt.date(2018, 7, 1), dt.date(2018, 7, 2), dt.date(2018, 7, 9)]


    def mock_get_schedule_on(self, date, open_time, close_time, tz):
        delta = timedelta_between(close_time, open_time)
        start = to_tz_datetime(date=date, time=open_time, to_tz=tz)
        end = start + delta

        onOffset = start.date() not in self.holidays_extra and end.date() not in self.holidays_extra
        onOffset = onOffset and start.date().weekday() != 5 and end.date().weekday() != 5

        if onOffset:
            return start, end

    def get_mock_schedule(self, bushours, tz):
        mock = Mock(spec=BaseSchedule)
        mock.get_schedule_on = MagicMock(side_effect=lambda x: self.mock_get_schedule_on(x, *bushours, tz))
        return mock


    def test_get_1day_schedule_mocked(self):
        mock_schedule = self.get_mock_schedule(('18:00', '17:00'), pytz.timezone('America/Chicago'))
        bscheduler = BScheduler(mock_schedule)

        actual = list(bscheduler.get_schedules(dt.date(2018, 7, 1), dt.date(2018, 7, 1), dt.time(1), dt.time(22), pytz.UTC))
        self.assertTrue(not actual)

        actual = list(bscheduler.get_schedules(dt.date(2018, 7, 4), dt.date(2018, 7, 4), dt.time(1), dt.time(22), pytz.UTC))
        expected = [(pd.Timestamp('2018-07-04 01:00:00+0000'), pd.Timestamp('2018-07-04 22:00:00+0000'))]
        self.assertListEqual(expected, actual)


    def test_get_multi_intraday_schedules_mocked(self):
        tz = pytz.timezone('America/Chicago')
        mock_schedule = self.get_mock_schedule(('18:00', '17:00'), tz)
        bscheduler = BScheduler(mock_schedule)
        actual = list(bscheduler.get_schedules(dt.date(2018, 7, 1), dt.date(2018, 7, 9), dt.time(1), dt.time(22), tz, tz))
        expected = [(pd.Timestamp('2018-07-03 18:00:00-0500'), pd.Timestamp('2018-07-03 22:00:00-0500')),
                    (pd.Timestamp('2018-07-04 01:00:00-0500'), pd.Timestamp('2018-07-04 17:00:00-0500')),
                    (pd.Timestamp('2018-07-04 18:00:00-0500'), pd.Timestamp('2018-07-04 22:00:00-0500')),
                    (pd.Timestamp('2018-07-05 01:00:00-0500'), pd.Timestamp('2018-07-05 17:00:00-0500')),
                    (pd.Timestamp('2018-07-05 18:00:00-0500'), pd.Timestamp('2018-07-05 22:00:00-0500')),
                    (pd.Timestamp('2018-07-06 01:00:00-0500'), pd.Timestamp('2018-07-06 17:00:00-0500'))]
        self.assertListEqual(expected, actual)


    def test_cme_schedule_with_nonlocal_window(self):
        bscheduler = BScheduler(get_schedule('CMESchedule'))

        # Mar 9 - March 12 CST -06:00, after March 12 CST -05:00
        actual = list(bscheduler.get_schedules(dt.date(2018, 3, 9), dt.date(2018, 3, 19), dt.time(1), dt.time(21), pytz.UTC))
        expected = [(pd.Timestamp('2018-03-09 01:00:00+0000'), pd.Timestamp('2018-03-09 21:00:00+0000')),
                    (pd.Timestamp('2018-03-12 01:00:00+0000'), pd.Timestamp('2018-03-12 21:00:00+0000')),
                    (pd.Timestamp('2018-03-13 01:00:00+0000'), pd.Timestamp('2018-03-13 21:00:00+0000')),
                    (pd.Timestamp('2018-03-14 01:00:00+0000'), pd.Timestamp('2018-03-14 21:00:00+0000')),
                    (pd.Timestamp('2018-03-15 01:00:00+0000'), pd.Timestamp('2018-03-15 21:00:00+0000')),
                    (pd.Timestamp('2018-03-16 01:00:00+0000'), pd.Timestamp('2018-03-16 21:00:00+0000')),
                    (pd.Timestamp('2018-03-19 01:00:00+0000'), pd.Timestamp('2018-03-19 21:00:00+0000'))]
        self.assertListEqual(expected, actual)

        actual = list(bscheduler.get_schedules(dt.date(2018, 3, 9), dt.date(2018, 3, 19), dt.time(1), dt.time(22), pytz.UTC))
        expected = [(pd.Timestamp('2018-03-09 01:00:00+0000'), pd.Timestamp('2018-03-09 22:00:00+0000')),
                    (pd.Timestamp('2018-03-12 01:00:00+0000'), pd.Timestamp('2018-03-12 21:00:00+0000')),
                    (pd.Timestamp('2018-03-13 01:00:00+0000'), pd.Timestamp('2018-03-13 21:00:00+0000')),
                    (pd.Timestamp('2018-03-14 01:00:00+0000'), pd.Timestamp('2018-03-14 21:00:00+0000')),
                    (pd.Timestamp('2018-03-15 01:00:00+0000'), pd.Timestamp('2018-03-15 21:00:00+0000')),
                    (pd.Timestamp('2018-03-16 01:00:00+0000'), pd.Timestamp('2018-03-16 21:00:00+0000')),
                    (pd.Timestamp('2018-03-19 01:00:00+0000'), pd.Timestamp('2018-03-19 21:00:00+0000'))]
        self.assertListEqual(expected, actual)


    def test_test_cme_schedule_with_local_window(self):
        cme_schedule = get_schedule('CMESchedule')
        bscheduler = BScheduler(cme_schedule)

        actual = list(
            bscheduler.get_schedules(dt.date(2018, 3, 8), dt.date(2018, 3, 19), dt.time(18), dt.time(15), cme_schedule.tzinfo))
        expected = [(pd.Timestamp('2018-03-09 00:00:00+0000'), pd.Timestamp('2018-03-09 21:00:00+0000')),
                    (pd.Timestamp('2018-03-11 23:00:00+0000'), pd.Timestamp('2018-03-12 20:00:00+0000')),
                    (pd.Timestamp('2018-03-12 23:00:00+0000'), pd.Timestamp('2018-03-13 20:00:00+0000')),
                    (pd.Timestamp('2018-03-13 23:00:00+0000'), pd.Timestamp('2018-03-14 20:00:00+0000')),
                    (pd.Timestamp('2018-03-14 23:00:00+0000'), pd.Timestamp('2018-03-15 20:00:00+0000')),
                    (pd.Timestamp('2018-03-15 23:00:00+0000'), pd.Timestamp('2018-03-16 20:00:00+0000')),
                    (pd.Timestamp('2018-03-18 23:00:00+0000'), pd.Timestamp('2018-03-19 20:00:00+0000'))]
        self.assertListEqual(expected, actual)
