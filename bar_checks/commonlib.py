from collections import Iterable, defaultdict
from sortedcontainers import SortedDict, SortedList
import pandas as pd
import os



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


def normal_group_by(items, key=lambda x: x, unique=False):
    value_type = set if unique else list
    d = defaultdict(value_type)
    for item in items:
        if value_type == list:
            d[key(item)].append(item)
        elif value_type == set:
            d[key(item)].add(item)
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

        value = itemfunc(item)
        if parent.get(key) is None:
            parent[key] = value_type()

        if isinstance(parent[key], list):
            parent[key].extend(value)
        elif isinstance(parent[key], SortedList):
            parent[key].update(value)

    return results


def na_equal(v1, v2):
    return (pd.isna(v1) & pd.isna(v2)) | (v1 == v2)


def func_grouper(iterable, n, func=lambda x: 1, chunk_type=list):
    iteritems = iter(iterable)
    prev = next(iteritems, None)
    curr = next(iteritems, None)

    def slice():
        nonlocal prev, curr
        count = func(prev)

        while prev is not None:
            yield prev

            count += 0 if curr is None else func(curr)
            prev = curr
            curr = next(iteritems, None)
            if count >= n:
                break

    while prev is not None:
        yield chunk_type(slice())


def source_from(src):
    if not isinstance(src, str):
        raise TypeError('Invalid source: must be type of str')

    if os.path.isdir(src):
        with open(src) as fh:
            return fh.read()
    else:
        return src


def bound_indices(items, bound_func):
    if not hasattr(items, '__getitem__'):
        raise ValueError("'items' object must be subscriptable")

    i, j = 0, len(items)
    not_head = not bound_func(items[i])
    not_tail = not bound_func(items[j - 1])
    while i < j and (not_head or not_tail):
        if not_head:
            i += 1
            not_head = not bound_func(items[i])
        if not_tail:
            j -= 1
            not_tail = not bound_func(items[j - 1])

    return i, j

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


