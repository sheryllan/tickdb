import unittest as ut
from pandas.tseries import offsets

from ..timeseries import *


class SeriesValidationTests(ut.TestCase):
    def setUp(self):
        self.schedules = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.schedule_bound = ScheduleBound(self.schedules)
        self.tsgenerator = StepTimestampGenerator(60, offsets.Minute)
        self.validation = SeriesValidation()

    #TODO test_SeriesValidation_schedule_list

    def test_reversions_all_reverse(self):
        timestamps = pd.date_range(pd.Timestamp('2018-07-03 22:00:00'), pd.Timestamp('2018-07-03 00:00:00'),
                                   freq=offsets.Minute(-60))
        actual = list(self.validation.is_within_freq(timestamps))
        expected = [True] + [False] * (len(actual) - 1)
        self.assertListEqual(expected, actual)

    def test_reversions_random_without_duplication(self):
        timestamps = [pd.Timestamp('2018-07-03 01:00:00'),
                      pd.Timestamp('2018-07-03 07:00:00'),
                      pd.Timestamp('2018-07-03 03:00:00'),
                      pd.Timestamp('2018-07-03 11:00:00'),
                      pd.Timestamp('2018-07-03 10:00:00'),
                      pd.Timestamp('2018-07-03 08:00:00')]

        actual = list(self.validation.is_within_freq(timestamps))
        expected = [True, True, False, True, False, False]

        self.assertListEqual(expected, actual)

    def test_reversions_random_with_duplication(self):
        timestamps = pd.DatetimeIndex([pd.Timestamp('2018-07-03 01:00:00'),
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
                                       pd.Timestamp('2018-07-03 08:00:00')])

        actual = list(self.validation.is_within_freq(timestamps))
        expected = [True, True, False, True, False, False, True, False, True, False, False, False]

        self.assertListEqual(expected, actual)

    def test_reversions_with_invalids(self):
        timestamps = pd.DatetimeIndex([pd.Timestamp('2018-07-03 01:00:00'),
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
                                       pd.Timestamp('2018-07-03 08:00:00')])

        actual_ordinal = list(self.validation.is_within_freq(timestamps))
        expected_ordinal = [True, True, True, False, True, False, False, True, False, True, False, True, False, False]
        actual_valid = list(self.validation.is_valid(timestamps, self.tsgenerator, self.schedule_bound))
        expected_valid = [True, False, True, True, True, True, True, True, True, True, True, False, True, True]

        self.assertListEqual(expected_ordinal, actual_ordinal)
        self.assertListEqual(expected_valid, actual_valid)

    def test_valid_closed(self):
        schedule_bound = ScheduleBound([(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 22:00:00')),
                                        (pd.Timestamp('2018-07-01 00:00:00'), pd.Timestamp('2018-07-02 16:00:00'))],
                                       closed='right', tz=pytz.timezone('America/Chicago'))
        tsgenerator = StepTimestampGenerator(60, offsets.Minute)
        timestamps = [pd.Timestamp('2018-07-02 21:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 01:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 03:30:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 05:00:00', tz=pytz.UTC)]

        actual_right = list(self.validation.is_valid(timestamps, tsgenerator, schedule_bound))
        expected_right = [True, False, False, False]
        self.assertListEqual(expected_right, actual_right)

        schedule_bound.closed = 'left'
        actual_left = list(self.validation.is_valid(timestamps, tsgenerator, schedule_bound))
        expected_left = [False, False, False, True]
        self.assertListEqual(expected_left, actual_left)

    def test_valid_of_different_tz(self):
        schedule_bound = ScheduleBound([(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 22:00:00')),
                                        (pd.Timestamp('2018-07-01 00:00:00'), pd.Timestamp('2018-07-02 16:00:00'))],
                                       closed='right', tz=pytz.timezone('America/Chicago'))
        tsgenerator = StepTimestampGenerator(60, offsets.Minute)
        timestamps = [pd.Timestamp('2018-07-02 21:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 01:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 03:30:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 07:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 03:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 11:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 11:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 07:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 10:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 08:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 12:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 08:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 23:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 08:00:00', tz=pytz.UTC),
                      pd.Timestamp('2018-07-03 08:00:00', tz=pytz.UTC)]

        actual_valid = list(self.validation.is_valid(timestamps, tsgenerator, schedule_bound))
        expected_valid = [True, False, False, True, False, True, True, True, True, True, True, True, True, True, True]
        self.assertListEqual(expected_valid, actual_valid)


