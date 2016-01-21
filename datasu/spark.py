__author__ = 'sashaostr'

import numpy as np
import doctest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *

def get_ddf_aggs(grpby_columns, agg_columns, agg_funcs, prefix='', suffix=''):
    """
    generates aggregations for spark dataframe
    :param grpby_columns: columns to groupby with: ['id','brand']
    :param agg_columns: columns to aggregate: ['productsize','purchasequantity']
    :param agg_funcs: aggregations dict to enable on agg_columns: { 'total':F.sum, 'average':F.avg }
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
            agg = agg_func(col).alias("_".join([col_prefix, col, agg_name, suffix]))
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
    return UserDefinedFunction(lambda x, index: x.values.tolist()[index], element_type(), 'get_index_from_vector')

# def rename_columns(df, fromcolumns=[], prefix=None):

