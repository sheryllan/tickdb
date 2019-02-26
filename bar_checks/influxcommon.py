from commonlib import *

TIME_IDX = 'time'


def logic_join(terms, relation='AND'):
    separator = ' ' + relation.strip() + ' '
    return separator.join(filter(None, terms))


def where_term(field, value, operator='='):
    if field is None or value is None:
        return None

    if nontypes_iterable(value):
        formatted_value = '/{}/'.format('|'.join(value))
        operator = '=~'
    else:
        formatted_value = "'{}'".format(value)
    return '"{}" {} {}'.format(field, operator, formatted_value)


def where_clause(terms, relation='AND'):
    return 'WHERE {}'.format(logic_join(terms, relation))


def time_terms(time_from=None, time_to=None, include_from=True, include_to=True):
    terms = []
    try:
        if time_from is not None:
            ts_from = time_from.isoformat() if time_from.tzinfo is not None else str(time_from)
            terms.append(where_term(TIME_IDX, ts_from, '>=' if include_from else '>'))
        if time_to is not None:
            ts_to = time_to.isoformat() if time_to.tzinfo is not None else str(time_to)
            terms.append(where_term(TIME_IDX, ts_to, '<=' if include_to else '<'))
    except AttributeError as ex:
        raise ValueError('Invalid time value: must be of datetime.datetime type') from ex

    return terms


def select_query(measurement, fields=None, clauses=None):
    fstring = '*' if fields is None else ', '.join(filter(None, to_iter(fields, ittype=iter)))
    cstring = '' if clauses is None else ' '.join(filter(None, to_iter(clauses, ittype=iter)))
    qselect = 'SELECT {} FROM {} {}'.format(fstring, measurement, cstring)
    return qselect.strip()


def limit(limit=None):
    if limit is not None:
        return 'LIMIT ' + str(limit)


def order_by(order):
    return 'ORDER BY ' + order
