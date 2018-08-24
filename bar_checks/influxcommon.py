import pandas as pd

from bar.quantdb1_config import *
from commonlib import *


def format_logic_join(terms, relation='AND'):
    separator = ' ' + relation.strip() + ' '
    return separator.join(filter(None, terms))


def format_arith_terms(fields, operator='=', relation='AND'):
    return format_logic_join(
        ('"{}" {} \'{}\''.format(k, operator, v) for k, v in fields.items() if v is not None),
        relation)


def format_where_clause(terms, relation='AND'):
    return 'WHERE {}'.format(format_logic_join(terms, relation))


def select_query(measurement, fields=None, clauses=None):
    fstring = '*' if fields is None else ', '.join(to_iter(fields))
    cstring = '' if clauses is None else ' '.join(to_iter(clauses))
    qselect = 'SELECT {} FROM {}'.format(fstring, measurement)
    return qselect + ' ' + cstring


def select_where_time_bound(measurement, time_from, time_to=None, fields=None, where_terms=None):
    ts_from = format_arith_terms({TIME_IDX: pd.Timestamp(time_from)}, '>=')
    ts_to = None if time_to is None else format_arith_terms({TIME_IDX: pd.Timestamp(time_to)}, '<=')
    where_clause = format_where_clause([ts_from, ts_to, where_terms])
    return select_query(measurement, fields, [where_clause])

