from commonlib import *


TIME_IDX = 'time'

def format_logic_join(terms, relation='AND'):
    separator = ' ' + relation.strip() + ' '
    return separator.join(filter(None, terms))


def format_where_value(value):
    if nontypes_iterable(value):
        return '/{}/'.format('|'.join(value))
    return "'{}'".format(value)


def format_arith_terms(fields, operator='=', relation='AND'):
    return format_logic_join(
        ('"{}" {} {}'.format(k, '=~' if nontypes_iterable(v) else operator, format_where_value(v))
         for k, v in fields.items() if v is not None), relation)


def format_where_clause(terms, relation='AND'):
    return 'WHERE {}'.format(format_logic_join(terms, relation))


def select_query(measurement, fields=None, clauses=None):
    fstring = '*' if fields is None else ', '.join(filter(None, to_iter(fields)))
    cstring = '' if clauses is None else ' '.join(filter(None, flatten_iter(clauses)))
    qselect = 'SELECT {} FROM {}'.format(fstring, measurement)
    return qselect + ' ' + cstring


def select_where_time_bound(measurement, time_from=None, time_to=None, fields=None, where_terms=None, others=None,
                            include_from=True, include_to=True):

    ts_from, ts_to = None, None

    if time_from is not None:
        str_from = time_from.isoformat() if time_from.tz is not None else str(time_from)
        ts_from = format_arith_terms({TIME_IDX: str_from}, '>=' if include_from else '>')

    if time_to is not None:
        str_to = time_to.isoformat() if time_to.tz is not None else str(time_to)
        ts_to = format_arith_terms({TIME_IDX: str_to}, '<=' if include_to else '<')

    where_clause = format_where_clause([ts_from, ts_to, where_terms])
    return select_query(measurement, fields, [where_clause, others])


def limit(limit=None):
    if limit is not None:
        return 'LIMIT ' + str(limit)


def order_by(order):
    return 'ORDER BY ' + order