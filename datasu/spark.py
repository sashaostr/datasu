__author__ = 'sashaostr'

import numpy as np
import doctest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, UserDefinedFunction
from pyspark.sql.types import *

def get_ddf_aggs(grpby_columns, agg_columns, agg_funcs, prefix=None, suffix=None, cast_to=None):
    """
    generates aggregations for spark dataframe
    :param grpby_columns: columns to groupby with: ['id','brand']
    :param agg_columns: columns to aggregate: ['productsize','purchasequantity']
    :param agg_funcs: aggregations dict to enable on agg_columns: { 'total':F.sum, 'average':F.avg }
    :param cast_to: cast aggregation result column to type (e.g. cast_to='double')
    :return [Column<avg(productsize) AS id_brand_productsize_average#59>,
             Column<sum(productsize) AS id_brand_productsize_total#60>,
             Column<avg(purchasequantity) AS id_brand_purchasequantity_average#61>,
             Column<sum(purchasequantity) AS id_brand_purchasequantity_total#62>]:

    Example:

    total_avg_agg = partial(get_ddf_aggs, agg_columns=['productsize','purchasequantity',],
                                     agg_funcs={'total':np.sum, 'average':np.average })

    grpby_columns = ['customer_id','brand']

    df_trans_grp_customer_brand = dff_trans.groupby(grpby_columns)
                                          .agg(**total_avg_agg)
    """
    aggs = []
    col_prefix = prefix + '_'.join(grpby_columns)
    for col in agg_columns:
        for agg_name, agg_func in agg_funcs.iteritems():
            agg_f = agg_func(col)
            if cast_to:
                agg_f = agg_f.cast(cast_to)
            agg = agg_f.alias("_".join([s for s in [col_prefix, col, agg_name, suffix] if s]))
            aggs.append(agg)
    return aggs


def write_ddf_to_csv(df, path):
    df.write.format("com.databricks.spark.csv").save(path=path, mode='overwrite', header='true')


def read_ddf_from_csv(context, path):
    csv_reader = context.read.format('com.databricks.spark.csv').options(header='true', inferschema='true')
    return csv_reader.load(path, samplingRatio=None)


def convert_columns_to_type(df, columns, target_type):
    asType = udf(lambda x: x, target_type())
    new_df = df.select(*[asType(column).alias(column) if column in columns else column for column in df.columns])
    return new_df


def drop_columns(df, columns=[], prefix=None):
    for c in columns:
        df = df.drop(c)

    if prefix is not None:
        df_columns = df.columns
        for c in df_columns:
            if c.startswith(prefix):
                df = df.drop(c)

    return df



def vector_to_array(elements_type=DoubleType):
    return UserDefinedFunction(lambda x: x.values.tolist(), ArrayType(elements_type()), 'vector_to_array')


def get_index_from_vector(element_type=DoubleType):
    """
    returns element from array by index
    :param element_type: vector
    :return: element at index
    """
    return UserDefinedFunction(lambda x, index: x.values.tolist()[index], element_type(), 'get_index_from_vector')


def rename_columns(df, prefix='', suffix='', separator='_', columns=None):
    prefix = prefix + separator if prefix else prefix
    suffix = separator + suffix if suffix else suffix
    columns = df.columns if columns is None else columns
    df1 = df.select('*')
    for c in columns:
        df1 = df1.withColumnRenamed(c, prefix + c + suffix)
    return df1


def filter_columns(expr, df):
    import re
    return filter(lambda c: re.match(expr,c), df.columns)


def pivot_aggregate(ddf, grpby_columns, pivot_column, aggs, pivot_filter_values=None, pivot_filter_support=None):
    if pivot_filter_support and not pivot_filter_values:
        frequent = ddf.freqItems([pivot_column], support=pivot_filter_support).first().asDict()[pivot_column+'_freqItems']
        pivot_filter_values = map(str,frequent)

    ddf_gr = ddf.groupBy(*grpby_columns)
    ddf_pivot = ddf_gr.pivot(pivot_column, pivot_filter_values)
    ddf_agg = ddf_pivot.agg(*aggs)
    return ddf_agg