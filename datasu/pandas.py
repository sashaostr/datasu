__author__ = 'sashaostr'


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