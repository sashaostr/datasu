__author__ = 'sashaostr'

from functools import partial
import re

col_name_pattern = lambda prefix, infix: "%s.*%s.*" % (prefix, infix)
filter_array = lambda pattern, array: filter(lambda c: re.match(pattern, c), array)


def get_expr(members, operator, expr='%s',pref='', suf=''):
    if not isinstance(members,list):
        members = [members]
    members = map(lambda m:expr % m ,members)
    return ''.join([pref, operator.join(members), suf]) if len(members) > 0 else ''

plus_expr = partial(get_expr, operator='+')
plus_categorical_expr = partial(plus_expr, expr='C(%s)')

plus_agg_columns_by_infix = lambda infix, columns: get_expr(filter_array(col_name_pattern('agg', infix), columns), '+')
u

# agg_col_name_pattern = partial(col_name_pattern, 'agg')
# plus_agg_columns_by_infix1 = lambda infix: plus_expr(filter_columns(agg_col_name_pattern(infix)))


# examples
# brand_interactions = "%s:%s" % (plus_agg_columns_by_infix('brand',agg_cols),  plus_expr('brand'))
# brand_interactions = "%s:%s" % (plus_agg_columns_by_infix('category',agg_cols),  plus_expr('category'))