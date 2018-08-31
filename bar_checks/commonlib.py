from datetime import date
from dateutil.relativedelta import relativedelta
from collections import Iterable


def last_n_years(n=1, d=date.today()):
    return d + relativedelta(years=-n)


def last_n_months(n=1, d=date.today()):
    return d + relativedelta(months=-n)


def last_n_days(n=1, d=date.today()):
    return d + relativedelta(days=-n)


def fmt_date(year, month=None, day=1, fmt='%Y%m'):
    if month is None:
        return str(year)
    return date(int(year), int(month), int(day)).strftime(fmt)


def nontypes_iterable(arg, excl_types=(str,)):
    return isinstance(arg, Iterable) and not isinstance(arg, excl_types)


def to_iter(x, excl_types=(str,), ittype=list):
    return [x] if not nontypes_iterable(x, excl_types) else ittype(x)


def find_first_n(arry, condition, n=1):
    result = list()
    for a in arry:
        if n <= 0:
            break
        if condition(a):
            result.append(a)
            n -= 1
    return result if len(result) != 1 else result[0]
