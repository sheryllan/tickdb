import datetime as dt
from dateutil.relativedelta import relativedelta
from collections import Iterable, defaultdict
from sortedcontainers import SortedDict, SortedList
import pandas as pd


def last_n_years(n=1, d=dt.date.today()):
    return d + relativedelta(years=-n)


def last_n_months(n=1, d=dt.date.today()):
    return d + relativedelta(months=-n)


def last_n_days(n=1, d=dt.date.today()):
    return d + relativedelta(days=-n)


def fmt_date(year, month=None, day=1, fmt='%Y%m'):
    if month is None:
        return str(year)
    return dt.date(int(year), int(month), int(day)).strftime(fmt)


def rreplace(s, old, new, occurrence=1):
    li = s.rsplit(old, occurrence)
    return new.join(li)


def nontypes_iterable(arg, excl_types=(str,)):
    return isinstance(arg, Iterable) and not isinstance(arg, excl_types)


def to_iter(x, excl_types=(str,), ittype=list):
    return ittype([x]) if not nontypes_iterable(x, excl_types) else ittype(x)


def find_first_n(arry, condition, n=1):
    result = list()
    for a in arry:
        if n <= 0:
            break
        if condition(a):
            result.append(a)
            n -= 1
    return result if len(result) != 1 else result[0]


def flatten_iter(items, level=None, excl_types=(str,)):
    if nontypes_iterable(items, excl_types):
        level = None if level is None else level + 1
        for sublist in items:
            yield from flatten_iter(sublist, level, excl_types)
    else:
        level_item = items if level is None else (level - 1, items)
        yield level_item


def normal_group_by(items, key=lambda x: x):
    d = defaultdict(list)
    for item in items:
        d[key(item)].append(item)
    return d


def hierarchical_group_by(items, keys, itemfunc=lambda x: x, sort_keys=None, sort_value=False):
    keys = list(keys)
    dict_types = [SortedDict if sort else dict for sort in sort_keys] \
        if sort_keys is not None else [dict for _ in keys]
    value_type = SortedList if sort_value else list

    results = dict_types[0]()
    for item in items:
        parent, key = results, keys[0](item)
        for i, keyfunc in enumerate(keys[1:]):
            child = parent.get(key)
            if child is None:
                child = dict_types[i]()
                parent[key] = child
            key = keyfunc(item)
            parent = child

        value = to_iter(itemfunc(item))
        if parent.get(key) is None:
            parent[key] = value_type()

        if isinstance(parent[key], list):
            parent[key].extend(value)
        else:
            parent[key].update(value)

    return results


# def hierarchical_group_by(items, keys, itemfunc=None):
#     if isinstance(items, pd.DataFrame):
#         flat_keys = list(flatten_iter(keys))
#         return items.set_index(flat_keys)
#
#     else:
#         results = defaultdict(dict)
#         for item in items:
#             record = results
#             for keyfunc in keys[:-1]:
#                 key = keyfunc(item)
#                 if key not in record:
#                     record[key] = defaultdict(dict)
#                 record = record[key]
#
#             last_key = keys[-1](item)
#             if last_key not in record:
#                 record[last_key] = []
#             value = item if itemfunc is None else itemfunc(item)
#             if isinstance(value, list):
#                 record[last_key].extend(value)
#             else:
#                 record[last_key].append(value)
#
#         return results


