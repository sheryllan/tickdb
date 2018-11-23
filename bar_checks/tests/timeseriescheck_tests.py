import unittest as ut
from timeutils.timeseries import *
import pandas as pd
from pandas.tseries import offsets


class KnownTimestampValidationTests(ut.TestCase):
    def setUp(self):
        self.schedules = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.schedule_bound = ScheduleBound(self.schedules)
        self.tsgenerator = StepTimestampGenerator(60, offsets.Minute)
        self.validation = KnownTimestampValidation(self.tsgenerator, self.schedule_bound)

    def test_reversions_all_reverse(self):
        timestamps = pd.date_range(pd.Timestamp('2018-07-03 22:00:00'), pd.Timestamp('2018-07-03 00:00:00'),
                                   freq=offsets.Minute(-60))
        self.validation.timestamps = timestamps
        actual = list(self.validation.invalids_reversions())
        expected = list(map(lambda x: (True, x), timestamps[1:]))
        self.assertListEqual(expected, actual)

    def test_reversions_random_without_duplication(self):
        timestamps = [pd.Timestamp('2018-07-03 01:00:00'),
                      pd.Timestamp('2018-07-03 07:00:00'),
                      pd.Timestamp('2018-07-03 03:00:00'),
                      pd.Timestamp('2018-07-03 11:00:00'),
                      pd.Timestamp('2018-07-03 10:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00')]

        self.validation.timestamps = timestamps
        actual = list(self.validation.invalids_reversions())
        expected = [(True, pd.Timestamp('2018-07-03 03:00:00')),
                    (True, pd.Timestamp('2018-07-03 10:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00'))]

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
        actual = list(self.validation.invalids_reversions())
        expected = [(True, pd.Timestamp('2018-07-03 03:00:00')),
                    (True, pd.Timestamp('2018-07-03 07:00:00')),
                    (True, pd.Timestamp('2018-07-03 10:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00'))]

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
        actual = list(self.validation.invalids_reversions())
        expected = [(False, pd.Timestamp('2018-07-03 03:30:00')),
                    (True, pd.Timestamp('2018-07-03 03:00:00')),
                    (True, pd.Timestamp('2018-07-03 07:00:00')),
                    (True, pd.Timestamp('2018-07-03 10:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00'))]

        self.assertListEqual(expected, actual)


        timestamps = [pd.Timestamp('2018-07-02 21:00:00'),
                      pd.Timestamp('2018-07-02 22:00:00'),
                      pd.Timestamp('2018-07-03 01:00:00'),
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
                      pd.Timestamp('2018-07-02 20:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00')]

        self.validation.timestamps = timestamps
        actual = list(self.validation.invalids_reversions())
        expected = [(False, pd.Timestamp('2018-07-03 03:30:00')),
                    (True, pd.Timestamp('2018-07-03 03:00:00')),
                    (True, pd.Timestamp('2018-07-03 07:00:00')),
                    (True, pd.Timestamp('2018-07-03 10:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-02 20:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00')),
                    (True, pd.Timestamp('2018-07-03 08:00:00'))]

        self.assertListEqual(expected, actual)



    def test_reversions_of_different_tz(self):
        schedules = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.schedule_bound = ScheduleBound(self.schedules)
        self.tsgenerator = StepTimestampGenerator(60, offsets.Minute)
        self.validation = KnownTimestampValidation(self.tsgenerator, self.schedule_bound)
        timestamps = [pd.Timestamp('2018-07-02 21:00:00'),
                      pd.Timestamp('2018-07-03 01:00:00'),
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
        actual = list()
        expected = [pd.Timestamp('2018-07-03 03:30:00'),
                    pd.Timestamp('2018-07-03 23:00:00')]
        self.assertListEqual(expected, actual)

