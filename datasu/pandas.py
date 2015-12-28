__author__ = 'sashaostr'

import pandas as pd
import numpy as np


def flatten_columns(df, prefix=''):
    return [prefix + '_'.join(t) for t in df.columns]


def get_agg(grpby_columns, grp_columns, grouping):
    col_prefix = '_'.join(grpby_columns)
    gr_aggs = lambda pref,grps:  {'_'.join((pref ,gr[0])):gr[1] for gr in grps.iteritems()}
    agg = {gc:gr_aggs(col_prefix, grouping) for gc in grp_columns}
    return agg


def columns_to_type(df, columns, totype):
    for col in columns:
        df[col] = df[col].astype(totype)


def merge_data(left, rights=[], how='inner'):
    print len(left)
    for i, (df, on) in enumerate(rights):
        left = left.merge(df, how=how, on=on)
        print len(left)
    return left


def explode_column(df, column):
    df_items_explode = pd.concat([pd.DataFrame(v, index=np.repeat(k,len(v)), columns=['explode_column']) for k,v in df[column].to_dict().items()])
    df_explode = df_items_explode.join(df)
    df_explode.rename(columns={column: 'old_item_column','explode_column':column}, inplace=True)
    df_explode.drop(['old_item_column'], axis=1, inplace=True)
    return df_explode


def repeat_rows_for_values(df, values_column, values=None, inplace=False):
    if not inplace:
        df = df.copy()

    if values is None:
        values = df[values_column].unique()

    values_series = pd.Series([values]*len(df), index=df.index, name='values_array')
    df[values_column] = values_series
    return explode_column(df, values_column)
