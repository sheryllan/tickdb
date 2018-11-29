import sys
import os
import json
import pandas as pd
from collections import deque
import epoch_utils


class PretendAlphaFrameworkBar(object):

    def __init__(self,
                 bar_time,
                 open,
                 high,
                 low,
                 close,
                 volume):
        
        self.bar_time = bar_time
        self.open = open 
        self.high = high 
        self.low = low 
        self.close = close
        self.volume = volume

    def __repr__(self):
        self.__str__()

    def __str__(self):
        return '[%s][%s %s %s %s][%s] (%s)' % (epoch_utils.epoch_to_human_nano_time(self.bar_time), self.open, self.high, self.low, self.close, self.volume, self.bar_time)


class BarFeed(object):

    def __init__(self, sim_json_path, bar_file_path):

        print('BarFeed: json[%s] bars[%s]' % (sim_json_path, bar_file_path))

        with open(sim_json_path) as sim_config_file:
            self._sim_config = json.loads(sim_config_file.read())
            
            with open(self._sim_config['strategy_json']) as strat_config_file:
                self._config = json.loads(strat_config_file.read())
                print(self._config)
                assert len(self._config['bars']) == 1, ('Only single bar series supported right now len(bars)=[%s]'%len(self._config['bars']))

        # ASSUMPTION: all histories are the same length
        self._history = deque(maxlen=self._config['bars'][0]['internal_history'])
        self._bar_file_path = bar_file_path

    def _bar_row_to_bar(self, index, bar_row):
        return PretendAlphaFrameworkBar(bar_row['bar_time'], bar_row['open'], bar_row['high'], bar_row['low'], bar_row['close'], bar_row['volume'])

        
    def run(self, alphaframework_python_component, verbose=False):

        print('Running, initialize PythonComponent...')
        if not alphaframework_python_component is None:
            alphaframework_python_component.initialize(self._config)

        # am doing this for column ordering protection whilst the protocol is in dev, realise this is an issue for large files
        df_bars = pd.read_csv(self._bar_file_path, header=[0])
        df_bars.rename(columns={df_bars.columns[0]:'bar_time'}, inplace=True)
        print('Got Bars...')
        print(df_bars.describe())

        print('Triggering marketsim-like run from python. Remember you have no order routing or execution simulation...')

        for index, bar_row in df_bars.iterrows():

            new_bar = self._bar_row_to_bar(index, bar_row)
            self._history.append(new_bar)

            if verbose:
                print(bar_row)
                print(new_bar)

            if not alphaframework_python_component is None:
                alphaframework_python_component.on_bar_update_histor(new_bar, self._history)

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

