import unittest as ut
import pandas as pd
from pandas.tseries import offsets

# from ...timeutils.timeseries import StepTimestampGenerator
# from ...timeutils.scheduler import ScheduleBound
# from ..enrichedOHLCVN import SeriesChecker
from timeutils.timeseries import StepTimestampGenerator
from timeutils.scheduler import ScheduleBound

import bar.enrichedOHLCVN as enriched
from bar.enrichedOHLCVN import SeriesChecker
from bar.datastore_config import EnrichedOHLCVN


class SeriesCheckerTests(ut.TestCase):
    Fields = EnrichedOHLCVN.Fields
    enriched.Fields = Fields

    def setUp(self):
        self.schedules = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.schedule_bound = ScheduleBound(self.schedules)
    
    def test_gaps_at_lower_freq(self):
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
        df = pd.DataFrame(timestamps, index=timestamps, columns=[self.Fields.TRIGGER_TIME])

        tsgenerator = StepTimestampGenerator(60, offsets.Minute)
        actual = list(SeriesChecker.gaps(df, tsgenerator, self.schedule_bound))
        expected = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 00:00:00')),
                    (pd.Timestamp('2018-07-03 02:00:00'), pd.Timestamp('2018-07-03 02:00:00')),
                    (pd.Timestamp('2018-07-03 04:00:00'), pd.Timestamp('2018-07-03 06:00:00')),
                    (pd.Timestamp('2018-07-03 09:00:00'), pd.Timestamp('2018-07-03 09:00:00')),
                    (pd.Timestamp('2018-07-03 13:00:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.assertListEqual(expected, actual)

    def test_gaps_at_higher_freq(self):
        # timestamps = [pd.Timestamp('2018-07-03 00:21:00'),
        #               pd.Timestamp('2018-07-03 00:22:00'),
        #               pd.Timestamp('2018-07-03 00:01:00'),
        #               pd.Timestamp('2018-07-03 00:03:30'),
        #               pd.Timestamp('2018-07-03 00:07:00'),
        #               pd.Timestamp('2018-07-03 00:03:00'),
        #               pd.Timestamp('2018-07-03 00:11:00'),
        #               pd.Timestamp('2018-07-03 00:11:00'),
        #               pd.Timestamp('2018-07-03 00:07:00'),
        #               pd.Timestamp('2018-07-03 00:10:00'),
        #               pd.Timestamp('2018-07-03 00:08:00'),
        #               pd.Timestamp('2018-07-03 00:12:00'),
        #               pd.Timestamp('2018-07-03 00:08:00'),
        #               pd.Timestamp('2018-07-02 00:20:00'),
        #               pd.Timestamp('2018-07-03 00:08:00'),
        #               pd.Timestamp('2018-07-03 00:08:00')]
        # df = pd.DataFrame(timestamps, index=timestamps, columns=[self.Fields.TRIGGER_TIME])
        tsgenerator = StepTimestampGenerator(1, offsets.Minute)
        # actual = list(SeriesChecker.gaps(df, tsgenerator, self.schedule_bound))
        # expected = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 00:20:00')),
        #             (pd.Timestamp('2018-07-03 00:23:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        # self.assertListEqual(expected, actual)

        timestamps = [pd.Timestamp('2018-07-02 00:21:00'),
                      pd.Timestamp('2018-07-02 00:22:00'),
                      pd.Timestamp('2018-07-03 00:01:00'),
                      pd.Timestamp('2018-07-03 00:03:30'),
                      pd.Timestamp('2018-07-03 00:07:00'),
                      pd.Timestamp('2018-07-03 00:03:00'),
                      pd.Timestamp('2018-07-03 00:11:00'),
                      pd.Timestamp('2018-07-03 00:11:00'),
                      pd.Timestamp('2018-07-03 00:07:00'),
                      pd.Timestamp('2018-07-03 00:10:00'),
                      pd.Timestamp('2018-07-03 00:08:00'),
                      pd.Timestamp('2018-07-03 00:12:00'),
                      pd.Timestamp('2018-07-03 00:08:00'),
                      pd.Timestamp('2018-07-02 00:20:00'),
                      pd.Timestamp('2018-07-03 00:08:00'),
                      pd.Timestamp('2018-07-03 00:08:00')]
        df = pd.DataFrame(timestamps, index=timestamps, columns=[self.Fields.TRIGGER_TIME])
        actual = list(SeriesChecker.gaps(df, tsgenerator, self.schedule_bound))
        expected = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 00:00:00')),
                    (pd.Timestamp('2018-07-03 00:02:00'), pd.Timestamp('2018-07-03 00:06:00')),
                    (pd.Timestamp('2018-07-03 00:08:00'), pd.Timestamp('2018-07-03 00:10:00')),
                    (pd.Timestamp('2018-07-03 00:13:00'), pd.Timestamp('2018-07-03 00:19:00')),
                    (pd.Timestamp('2018-07-03 00:21:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.assertListEqual(expected, actual)

