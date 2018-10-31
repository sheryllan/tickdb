import unittest as ut
from timeseriesutils import *
import pandas as pd
from pandas.tseries import offsets


class KnownTimestampValidationTests(ut.TestCase):
    def setUp(self):
        self.schedules = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.tsgenerator = StepTimestampGenerator(self.schedules, 60, offsets.Minute)
        self.validation = KnownTimestampValidation(self.tsgenerator)

    def test_reversions_all_reverse(self):
        timestamps = pd.date_range(pd.Timestamp('2018-07-03 22:00:00'), pd.Timestamp('2018-07-03 00:00:00'),
                                   freq=offsets.Minute(-60))
        self.validation.timestamps = timestamps

        date = dt.date(2018, 7, 3)
        actual = list(self.validation.reversions(date))
        expected = [(timestamps[23 - i], 23 - i, i - 1) for i in range(1, 23)]
        self.assertListEqual(expected, actual)

    def test_reversions_random_without_duplication(self):
        timestamps = [pd.Timestamp('2018-07-03 01:00:00'),
                      pd.Timestamp('2018-07-03 07:00:00'),
                      pd.Timestamp('2018-07-03 03:00:00'),
                      pd.Timestamp('2018-07-03 11:00:00'),
                      pd.Timestamp('2018-07-03 10:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00')]

        self.validation.timestamps = timestamps
        date = dt.date(2018, 7, 3)
        actual = list(self.validation.reversions(date))
        expected = [(pd.Timestamp('2018-07-03 03:00:00'), 2, 1),
                    (pd.Timestamp('2018-07-03 08:00:00'), 5, 3),
                    (pd.Timestamp('2018-07-03 10:00:00'), 4, 4)]

        self.assertListEqual(expected, actual)

    def test_reversions_random_with_duplication(self):
        timestamps = [pd.Timestamp('2018-07-03 01:00:00'),
                      pd.Timestamp('2018-07-03 07:00:00'),
                      pd.Timestamp('2018-07-03 03:00:00'),
                      pd.Timestamp('2018-07-03 11:00:00'),
                      pd.Timestamp('2018-07-03 11:00:00'),
                      pd.Timestamp('2018-07-03 07:00:00'),
                      pd.Timestamp('2018-07-03 10:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00'),
                      pd.Timestamp('2018-07-03 12:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00')]

        self.validation.timestamps = timestamps
        date = dt.date(2018, 7, 3)
        actual = list(self.validation.reversions(date))
        expected = [(pd.Timestamp('2018-07-03 03:00:00'), 2, 1),
                    (pd.Timestamp('2018-07-03 07:00:00'), 5, 2),
                    (pd.Timestamp('2018-07-03 08:00:00'), 7, 3),
                    (pd.Timestamp('2018-07-03 08:00:00'), 9, 3),
                    (pd.Timestamp('2018-07-03 08:00:00'), 10, 3),
                    (pd.Timestamp('2018-07-03 08:00:00'), 11, 3),
                    (pd.Timestamp('2018-07-03 10:00:00'), 6, 4)]

        self.assertListEqual(expected, actual)

    def test_reversions_with_invalids(self):
        timestamps = [pd.Timestamp('2018-07-03 01:00:00'),
                      pd.Timestamp('2018-07-03 03:30:00'),
                      pd.Timestamp('2018-07-03 07:00:00'),
                      pd.Timestamp('2018-07-03 03:00:00'),
                      pd.Timestamp('2018-07-03 11:00:00'),
                      pd.Timestamp('2018-07-03 11:00:00'),
                      pd.Timestamp('2018-07-03 07:00:00'),
                      pd.Timestamp('2018-07-03 10:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00'),
                      pd.Timestamp('2018-07-03 12:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00'),
                      pd.Timestamp('2018-07-03 23:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00')]

        self.validation.timestamps = timestamps
        date = dt.date(2018, 7, 3)
        actual = list(self.validation.reversions(date))
        expected = [(pd.Timestamp('2018-07-03 03:00:00'), 3, 1),
                    (pd.Timestamp('2018-07-03 07:00:00'), 6, 2),
                    (pd.Timestamp('2018-07-03 08:00:00'), 8, 3),
                    (pd.Timestamp('2018-07-03 08:00:00'), 10, 3),
                    (pd.Timestamp('2018-07-03 08:00:00'), 12, 3),
                    (pd.Timestamp('2018-07-03 08:00:00'), 13, 3),
                    (pd.Timestamp('2018-07-03 10:00:00'), 7, 4)]

        self.assertListEqual(expected, actual)
