import unittest as ut
from commonlib import *


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

