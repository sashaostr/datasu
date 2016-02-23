__author__ = 'sashaostr'

import numpy as np
import doctest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, UserDefinedFunction
from pyspark.sql.types import *

def get_ddf_aggs(grpby_columns, agg_columns, agg_funcs, prefix=None, suffix=None, cast_to=None, return_columns_names=False):
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
    col_names = []
    col_prefix = prefix + '_'.join(grpby_columns)
    for col in agg_columns:
        for agg_name, agg_func in agg_funcs.iteritems():
            agg_f = agg_func(col)
            if cast_to:
                agg_f = agg_f.cast(cast_to)
            alias = "_".join([s for s in [col_prefix, col, agg_name, suffix] if s])
            agg = agg_f.alias(alias)
            aggs.append(agg)
            col_names.append(alias)

    if return_columns_names:
        return aggs, col_names
    else:
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


def rename_columns(ddf, prefix='', suffix='', separator='_', columns=None):
    """
    rename multiple columns in DataFrame by adding prefix and suffix
    :param ddf: DataFrame
    :param prefix:
    :param suffix:
    :param separator:
    :param columns: columns to rename. Use all columns in DataFrame if None.
    :return:
    """
    prefix = prefix + separator if prefix else prefix
    suffix = separator + suffix if suffix else suffix
    columns = ddf.columns if columns is None else columns
    df1 = ddf.select('*')
    for c in columns:
        df1 = df1.withColumnRenamed(c, prefix + c + suffix)
    return df1


def filter_columns(expr, df):
    """
    Filter columns list by regex expression
    :param expr: regex expression
    :param df:
    :return: columns list matching expr
    """
    import re
    return filter(lambda c: re.match(expr,c), df.columns)


def index_columns(ddf, index_columns, index_col_suffix='_idx', return_indexers=False):
    """
    Index multiple columns using pyspark.ml.feature.StringIndexer
    :param ddf: DataFrame
    :param index_columns: columns to index
    :param index_col_suffix: suffix added to new indexed columns that created
    :param return_indexers: return StringIndexers transformers in the result
    :return: DataFrame with indexed columns and indexers (optional)
    """
    from pyspark.ml.feature import StringIndexer
    indexers = map(lambda c: StringIndexer(inputCol=c, outputCol='%s%s' % (c,index_col_suffix)).fit(ddf), index_columns)
    indexed = reduce(lambda ddf,t: t.transform(ddf), indexers, ddf)

    if return_indexers:
        return indexed, indexers
    else:
        return indexed


def aggregate_and_pivot_into_vector(ddf, id_column, pivot_column, aggs, vector_column_name='features'):
    """
    1. apply aggs to DataFrame (group by [id_column, pivot_column]),
    2. pivot (one-hot encode) by pivot_column (values are indexed by StringIndexer)
    3. save results into vector_column_name as Vector (if multiple aggregations provided, assemble result into one vector using pyspark.ml.feature.VectorAssembler)

    Example:
    aggs = get_ddf_aggs(grpby_columns=['customer_id', 'category'], agg_columns=['productsize','purchasequantity'],
                        agg_funcs={'total':F.sum}, prefix='agg_', columns cast_to='double')
    print aggs
        #[Column<cast((sum(productsize),mode=Complete,isDistinct=false) as double) AS agg_customer_id_category_productsize_total#127>,
        #Column<cast((sum(purchasequantity),mode=Complete,isDistinct=false) as double) AS agg_customer_id_category_purchasequantity_total#128>]

    ddf_trans_pivot = aggregate_and_pivot_into_vector(ddf_trans, id_column='customer_id', pivot_column='category', aggs=aggs)
    ddf_trans_pivot.first()
        #Row(customer_id=98468631, features=SparseVector(1666, {0: 1.0, 1: 1.0, 5: 1.0, 8: 2.0, 13: 1.0, ))

    :param ddf: DataFrame
    :param id_column: row id column
    :param pivot_column: column to one-hot encode
    :param aggs:
    :param vector_column_name:
    :return:
    """
    from pyspark.mllib.linalg.distributed import CoordinateMatrix, IndexedRowMatrix
    from pyspark.ml.feature import VectorAssembler

    index_col_suffix = '_idx'
    grpby_columns = [id_column, pivot_column]

    aggregated = ddf.groupBy(grpby_columns).agg(*aggs)

    pivot_indexed_column = pivot_column+index_col_suffix
    agg_column_names = list(set(aggregated.columns)-set([id_column, pivot_column, pivot_indexed_column]))

    indexed, indexers = index_columns(ddf=aggregated, index_columns=[pivot_column], index_col_suffix=index_col_suffix, return_indexers=True)

    res = None
    agg_columns_vectors = map(lambda c: c+'_vector',agg_column_names)
    for agg_column, agg_column_vector in zip(agg_column_names, agg_columns_vectors):
        cm = CoordinateMatrix(
            indexed.map(lambda r: (long(r[id_column]), long(r[pivot_indexed_column]), r[agg_column]))
        )
        irm = cm.toIndexedRowMatrix()
        ddf_irm = irm.rows.toDF()
        ddf_irm = ddf_irm.withColumnRenamed('index', id_column).withColumnRenamed('vector', agg_column_vector)

        if res:
            res = res.join(ddf_irm, on=id_column, how='inner')
        else:
            res = ddf_irm

    if len(agg_columns_vectors) > 1:
        assembler = VectorAssembler(inputCols=agg_columns_vectors, outputCol=vector_column_name)
        res = assembler.transform(res)
    else:
        res = res.withColumnRenamed(agg_columns_vectors[0], vector_column_name)

    res = drop_columns(res, columns=agg_columns_vectors)
    return res


def merge_features(ddfs, join_column, merge_column, output_column='features', drop_merged_columns=True):
    """
    join (inner) several DataFrames by same id and merge its columns (merge_column) into one column using using pyspark.ml.feature.VectorAssembler

    Example:
        ddf_merge = merge_features(ddfs=[ddf_pivot1,ddf_pivot2], join_column='customer_id', merge_column='features')
    :param ddfs:
    :param join_column: id column to join by (each ddf must have this column)
    :param merge_column: column to merge (each ddf must have this column)
    :param output_column:
    :param drop_merged_columns:
    :return:
    """
    from pyspark.ml.feature import VectorAssembler

    ddf_res = ddfs.pop(0)
    merge_column_renamed = merge_column + str(0)
    merge_columns = [merge_column_renamed]
    ddf_res = ddf_res.withColumnRenamed(merge_column, merge_column_renamed)

    for i,ddf in enumerate(ddfs):
        merge_column_renamed = merge_column + str(i+1)
        merge_columns.append(merge_column_renamed)
        ddf_r = ddf.withColumnRenamed(merge_column, merge_column_renamed)
        ddf_res = ddf_res.join(ddf_r, on=join_column, how='inner')

    assembler = VectorAssembler(inputCols=merge_columns, outputCol=output_column)
    res = assembler.transform(ddf_res)

    if drop_merged_columns:
        res = drop_columns(res, columns=merge_columns)

    return res


# def pivot_aggregate(ddf, grpby_columns, pivot_column, aggs, pivot_filter_values=None, pivot_filter_support=None):
#     if pivot_filter_support and not pivot_filter_values:
#         frequent = ddf.freqItems([pivot_column], support=pivot_filter_support).first().asDict()[pivot_column+'_freqItems']
#         pivot_filter_values = map(str,frequent)
#
#     ddf_gr = ddf.groupBy(*grpby_columns)
#     ddf_pivot = ddf_gr.pivot(pivot_column, pivot_filter_values)
#     ddf_agg = ddf_pivot.agg(*aggs)
#     return ddf_agg