import sys
import os
import json
import pandas as pd
from collections import deque
import epoch_utils

class PretendAlphaFrameworkInst():

    def __init__( self,
            name):
        self.name = name

class PretendAlphaFrameworkDefinition():

    def __init__(self, clock, width, offset):
        self.clock = clock
        self.width = width
        self.offset = offset

class PretendAlphaFrameworkBar():

    def __init__(self,
                 inst,
                 definition,
                 bar_time,
                 open,
                 high,
                 low,
                 close,
                 volume,
                 net_volume=0):

        self._inst = inst
        self._definition = definition
        self.bar_time = bar_time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.net_volume = net_volume

    def inst(self):
        return self._inst

    def definition(self):
        return self._definition

    def __repr__(self):
        self.__str__()

    def __str__(self):
        return '[%s][%s %s %s %s][%s] (%s)' % (epoch_utils.epoch_to_human_datetime(self.bar_time), self.open, self.high, self.low, self.close, self.volume, self.bar_time)

class BarsFeed():

    def __init__(self, inst_name, clock, width, offset, history_len, bar_file_path):

        print('BarFeed: len[%s] bars[%s]' % (history_len, bar_file_path))

        self._inst = PretendAlphaFrameworkInst(inst_name)
        self._definition = PretendAlphaFrameworkDefinition(clock, width, offset)

        # ASSUMPTION: all histories are the same length
        self._history = deque(maxlen=history_len)
        self._bar_file_path = bar_file_path

    def _file_to_dataframe(self):
        # am doing this for column ordering protection whilst the protocol is in dev, realise this is an issue for large files
        df_bars = pd.read_csv(self._bar_file_path, header=[0], compression='gzip')
        df_bars.rename(columns={df_bars.columns[0]:'bar_time'}, inplace=True)
        print('Got Bars...')
        print(df_bars.describe())

        return df_bars

    def _bar_row_to_bar(self, index, bar_row):
        return PretendAlphaFrameworkBar(self._inst, self._definition, bar_row['bar_time'], bar_row['open'], bar_row['high'], bar_row['low'], bar_row['close'], bar_row['volume'])

    def run(self, alpha_framework_python_component, verbose=False):

        df_bars = self._file_to_dataframe()
        print('Triggering marketsim-like run from python. Remember you have no order routing or execution simulation...')

        for index, bar_row in df_bars.iterrows():

            new_bar = self._bar_row_to_bar(index, bar_row)
            self._history.append(new_bar)

            if verbose:
                print(bar_row)
                print(new_bar)

            if not alpha_framework_python_component is None:
                alpha_framework_python_component.on_bar_update_with_history(new_bar, self._history)

    @staticmethod
    def bar_feed_factory(argv):
        if not len(argv) == 3:
            print('Usage: bars_feed <path-to-market-sim-json> <path-to-bar-file>')
        elif not os.path.exists(argv[1]):
            print('Error: cannot locate market sim json file [%s]' % argv[1])
        elif not os.path.exists(argv[2]):
            print('Error: cannot locate csv bar file file [%s]' % argv[2])
        else:
            return BarFeed(argv[1], argv[2])

if __name__ == "__main__":

    bf = BarFeed.bar_feed_factory(sys.argv)
    bf.run(None, True)
    print('BarFeed: done')
