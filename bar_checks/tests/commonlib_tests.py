import unittest as ut
import datetime as dt
from ..commonlib import *



class FuncGrouperTests(ut.TestCase):
    def func(self, x):
        return len(str(x))

    def test_one_or_zero_elements(self):
        actual = list(func_grouper(iter(''), 5, self.func))
        self.assertFalse(actual)

        items = ['lllllllllllll']
        expected = [items]
        actual = list(func_grouper(items, 5, self.func))
        self.assertListEqual(expected, actual)


    def test_last_less_than(self):
        items = ['efe', 'eea', 'jiofe', 'f', 'yy', ';']
        expected = [('efe',), ('eea',), ('jiofe',),  ('f', 'yy', ';')]
        actual = list(func_grouper(items, 5, self.func, tuple))
        self.assertListEqual(expected, actual)

    def test_last_greater_than(self):
        items = ['efe', 'eea', 'jiofe', 'f', 'yyt', ' ;opjpi']
        expected = [('efe',), ('eea',), ('jiofe',), ('f', 'yyt'), (' ;opjpi',)]
        actual = list(func_grouper(items, 5, self.func, tuple))
        self.assertListEqual(expected, actual)


class GroupbyTests(ut.TestCase):

    def test_schedules_hierarchical_group_by(self):
        schedules = [(pd.Timestamp(2018, 5, 5, 10), pd.Timestamp(2018, 5, 5, 11)),
                     (pd.Timestamp(2018, 5, 4, 1), pd.Timestamp(2018, 5, 4, 3)),
                     (pd.Timestamp(2018, 5, 4, 12), pd.Timestamp(2018, 5, 5, 0)),
                     (pd.Timestamp(2018, 5, 5, 12), pd.Timestamp(2018, 5, 5, 23)),
                     (pd.Timestamp(2018, 5, 5, 1), pd.Timestamp(2018, 5, 5, 3))]
        keys = [lambda x: x[0].date(), lambda x: x[1].date()]

        actual = hierarchical_group_by(schedules, keys, itemfunc=lambda x: [x],
                                       sort_keys=[True, True], sort_value=True)
        expected = SortedDict({
            dt.date(2018, 5, 4): SortedDict({
                dt.date(2018, 5, 4): SortedList([(pd.Timestamp(2018, 5, 4, 1), pd.Timestamp(2018, 5, 4, 3))]),
                dt.date(2018, 5, 5): SortedList([(pd.Timestamp(2018, 5, 4, 12), pd.Timestamp(2018, 5, 5, 0))])
            }),
            dt.date(2018, 5, 5): SortedDict({
                dt.date(2018, 5, 5): SortedList([(pd.Timestamp(2018, 5, 5, 1), pd.Timestamp(2018, 5, 5, 3)),
                                                 (pd.Timestamp(2018, 5, 5, 10), pd.Timestamp(2018, 5, 5, 11)),
                                                 (pd.Timestamp(2018, 5, 5, 12), pd.Timestamp(2018, 5, 5, 23))])
            })
        })

        self.assertListEqual(list(expected.keys()), list(actual.keys()))

        for k1 in expected:
            self.assertListEqual(list(expected[k1].keys()), list(actual[k1].keys()))
            for k2 in expected[k1]:
                self.assertListEqual(list(expected[k1][k2]), list(actual[k1][k2]))

