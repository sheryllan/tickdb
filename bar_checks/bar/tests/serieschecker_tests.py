import unittest as ut
import pandas as pd
from pandas.tseries import offsets
import pytz

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
        timestamps = [pd.Timestamp('2018-07-03 00:21:00'),
                      pd.Timestamp('2018-07-03 00:22:00'),
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
        tsgenerator = StepTimestampGenerator(1, offsets.Minute)
        actual = list(SeriesChecker.gaps(df, tsgenerator, self.schedule_bound))
        expected = [(pd.Timestamp('2018-07-03 00:00:00'), pd.Timestamp('2018-07-03 00:20:00')),
                    (pd.Timestamp('2018-07-03 00:23:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.assertListEqual(expected, actual)

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
                    (pd.Timestamp('2018-07-03 00:13:00'), pd.Timestamp('2018-07-03 22:00:00'))]
        self.assertListEqual(expected, actual)

    def test_gaps_with_backfill(self):
        fname = 'NQ-20190612-5M.csv'
        df = pd.read_csv(fname, sep=',', parse_dates=[0, 1],
                         date_parser=lambda x: pytz.utc.localize(pd.to_datetime(int(x))), index_col=0)
        tsgenerator = StepTimestampGenerator(5, offsets.Minute)
        schedule_bound = ScheduleBound(
            [(pd.Timestamp('2019-06-11 19:00:00-0500'), pd.Timestamp('2019-06-12 15:00:00-0500')),
             (pd.Timestamp('2019-06-12 18:00:00-0500'), pd.Timestamp('2019-06-12 19:00:00-0500'))],
            closed='left', tz=pytz.utc)

        actual = list(SeriesChecker.gaps(df, tsgenerator, schedule_bound))
        expected = [(pd.Timestamp('2019-06-12 14:25:00+0000'), pd.Timestamp('2019-06-12 19:20:00+0000'))]
        self.assertListEqual(expected, actual)

    def test_gaps_with_different_tz(self):
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

        df = pd.DataFrame(timestamps, index=timestamps, columns=[self.Fields.TRIGGER_TIME])
        actual_gaps = list(SeriesChecker.gaps(df, tsgenerator, schedule_bound))
        expected_gaps = [(pd.Timestamp('2018-07-01 01:00:00', tz=pytz.timezone('America/Chicago')),
                          pd.Timestamp('2018-07-02 15:00:00', tz=pytz.timezone('America/Chicago'))
                          ),
                         (pd.Timestamp('2018-07-03 01:00:00', tz=pytz.timezone('America/Chicago')),
                          pd.Timestamp('2018-07-03 01:00:00', tz=pytz.timezone('America/Chicago'))
                          ),
                         (pd.Timestamp('2018-07-03 04:00:00', tz=pytz.timezone('America/Chicago')),
                          pd.Timestamp('2018-07-03 04:00:00', tz=pytz.timezone('America/Chicago'))
                          ),
                         (pd.Timestamp('2018-07-03 08:00:00', tz=pytz.timezone('America/Chicago')),
                          pd.Timestamp('2018-07-03 17:00:00', tz=pytz.timezone('America/Chicago'))
                          ),
                         (pd.Timestamp('2018-07-03 19:00:00', tz=pytz.timezone('America/Chicago')),
                          pd.Timestamp('2018-07-03 22:00:00', tz=pytz.timezone('America/Chicago'))
                          )]
        self.assertListEqual(expected_gaps, actual_gaps)

