import numpy as np
from .commonlib import *


def format_expression(*args):
    if not args:
        return ''

    func_indicators = ('()', '[]')
    capsule = {'(': ')', '[': ']'}
    closed = []
    sym_closing = None

    expr = ''
    args = iter(args)
    for arg in args:
        arg = str(arg)

        if arg in capsule:
            sym_closing = capsule[arg]
            closed.append(sym_closing)
            to_append = ' ' + arg
        elif arg == sym_closing:
            closed.pop()
            sym_closing = closed[-1] if closed else None
            to_append = ' ' + arg
        elif arg.endswith(func_indicators):
            try:
                next_op = str(next(args))
                if next_op in func_indicators or next_op in capsule or next_op in capsule.values():
                    raise ValueError('Invalid expression arguments: invalid symbols following a parametric '
                                     'evaluator')
                to_append = rreplace(arg, arg[-2:], arg[-2] + next_op + arg[-1])
            except StopIteration:
                raise ValueError('Invalid expression arguments: an evaluator must be followed by an operand')
        else:
            to_append = ' ' + arg

        expr = expr + to_append

    if closed:
        closed.reverse()
        raise ValueError('Invalid expression arguments: expression is not closed by {}'.format(closed))
    return expr.strip()


def split_expr(expr, sep=' '):
    return tuple(expr.split(sep))


def chained_expr(f1, f2, symbol):
    return '( {} ) {} ( {} )'.format(f1, symbol, f2)


def not_expr(orig, symbol='~'):
    return '{} ( {} )'.format(symbol, orig)


def single_comp_expr(f1, f2, symbol):
    return '{} {} {}'.format(f1, symbol, f2)


def notna_expr(field):
    return '{} .notna() '.format(field)


def isna_expr(field):
    return '{} .isna() '.format(field)


def vector_eval(df, rname='result', *args):
    cols = list(set([arg for arg in args if arg in df]))
    statement = '{} = {}'.format(rname, format_expression(*args))
    return df[cols].eval(statement)


def vector_map(vector, map_true='', map_false='', mask=None, mask_value=''):
    results = vector.map(lambda x: map_true if x else map_false)
    if mask is not None:
        results[mask] = mask_value
    return results


def join_row(row, sep='\n', na_rep=None, filter_none=True):
    if na_rep is None:
        return sep.join(filter(None, row.dropna().astype(str)) if filter_none else row.dropna().astype(str))
    return sep.join(filter(None, row.replace(np.nan, na_rep).astype(str))
                    if filter_none else row.replace(np.nan, na_rep).astype(str))


def vectors_join(vectors, sep='\n', na_rep=None, filter_none=True):
    df_cat = vectors
    if not isinstance(df_cat, pd.DataFrame):
        df_cat = pd.concat(vectors, axis=1)
    return df_cat.apply(lambda x: join_row(x, sep, na_rep, filter_none), axis=1)


def iter_groupby(data, by):
    by_series = pd.Series(by, data.index) if not isinstance(by, pd.Series) else by
    diff = by_series != by_series.shift(1)
    return data.groupby(diff.cumsum())


def to_grouped_df(data, keys):
    keys = to_iter(keys)
    data_df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data
    if data_df.empty:
        return iter('')
    data_df.set_index(keys, inplace=True)
    for group_key, grouped_df in data_df.groupby(level=keys):
        yield {key: val for key, val in zip(keys, to_iter(group_key, ittype=iter))}, grouped_df
