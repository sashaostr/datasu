__author__ = 'sashaostr'

import pandas as pd
import numpy as np
import doctest

def flatten_columns(df, prefix=''):
    """
    flatten hierarchical columns in pandas dataframe
    :param df: input dataframe
    :param prefix: optional prefix for flattened column name
    :return: array of flatten columns names

    Example:
    df_trans_grp_customer_category.columns = flatten_columns(df_trans_grp_customer_category, prefix='agg_')
    """
    return [prefix + '_'.join(t) for t in df.columns]


def build_df_agg(grpby_columns, agg_columns, agg_funcs):
    """
    generates aggregations
    :param grpby_columns: columns to groupby with
    :param agg_columns: columns to aggregate
    :param agg_funcs: aggregations dict to enable on agg_columns: {'total':np.sum, 'average':np.average }
    :return {'productsize': {'id_brand_average': <function pyspark.sql.functions.avg>,
             'id_brand_total': <function pyspark.sql.functions.sum>},
             'purchasequantity': {'id_brand_average': <function pyspark.sql.functions.avg>,
             'id_brand_total': <function pyspark.sql.functions.sum>}}:

    Example:

    count_agg = partial(build_df_agg, agg_columns=['customer_id'],
                                 agg_funcs={'count':np.count_nonzero })
    total_avg_agg = partial(build_df_agg, agg_columns=['productsize','purchasequantity',],
                                     agg_funcs={'total':np.sum, 'average':np.average })

    grpby_columns = ['customer_id','brand']

    df_trans_grp_customer_brand = df_trans.groupby(by=grpby_columns, axis=0)
                                          .agg(merge_dicts(count_agg(grpby_columns),
                                                           total_avg_agg(grpby_columns)))
    """
    col_prefix = '_'.join(grpby_columns)
    gr_aggs = lambda pref,grps:  {'_'.join((pref ,gr[0])):gr[1] for gr in grps.iteritems()}
    agg = {gc:gr_aggs(col_prefix, agg_funcs) for gc in agg_columns}
    return agg


def columns_to_type(df, columns, totype):
    """
    change columns types of dataframe inplace
    :param df: target dataframe
    :param columns: columns to transform
    :param totype: target type
    :return:
    """
    for col in columns:
        df[col] = df[col].astype(totype)


def merge_data(left, rights=[], how='inner'):
    """

    :param left:
    :param rights:
    :param how:
    :return:
    """
    print len(left)
    for i, (df, on) in enumerate(rights):
        left = left.merge(df, how=how, on=on)
        print len(left)
    return left


def explode_column(df, column):
    """
    explode array column - creates row for each value in array in column
    :param df: target dataframe
    :param column: columnn to explode by
    :return: exploded dataframe
    """
    df_items_explode = pd.concat([pd.DataFrame(v, index=np.repeat(k,len(v)), columns=['explode_column']) for k,v in df[column].to_dict().items()])
    df_explode = df_items_explode.join(df)
    df_explode.rename(columns={column: 'old_item_column','explode_column':column}, inplace=True)
    df_explode.drop(['old_item_column'], axis=1, inplace=True)
    return df_explode


def repeat_rows_for_values(df, values_column, values=None, inplace=False):
    """
    create row for each of value in 'values_column' (explode it) or for each of values provided
    :param df: target dataframe
    :param values_column: column to explode and put values into
    :param values: optional values to repeat rows for
    :param inplace:
    :return:

    Example:
    df_customers_offers = repeat_for_items(df_customers, 'offer', values=['offer1', 'offer2'])
    """
    if not inplace:
        df = df.copy()

    if values is None:
        values = df[values_column].unique()

    values_series = pd.Series([values]*len(df), index=df.index, name='values_array')
    df[values_column] = values_series
    return explode_column(df, values_column)
