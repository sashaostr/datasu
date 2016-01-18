__author__ = 'sashaostr'

import numpy as np
import doctest
from pyspark.sql import functions as F


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
    col_prefix = prefix + '_'.join(grpby_columns) + suffix
    for col in agg_columns:
        for agg_name, agg_func in agg_funcs.iteritems():
            agg = agg_func(col).alias("_".join([col_prefix, col, agg_name]))
            aggs.append(agg)
    return aggs


def write_ddf_to_csv(df, path):
    df.write.format("com.databricks.spark.csv").save(path=path, mode='overwrite', header='true')


def read_ddf_from_csv(context, path):
    csv_reader = context.read.format('com.databricks.spark.csv').options(header='true', inferschema='true')
    return csv_reader.load(path, samplingRatio=None)