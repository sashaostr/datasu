__author__ = 'sashaostr'

import string


class PartialFormatter(string.Formatter):
    def __init__(self, missing='~~', bad_fmt='!!'):
        self.missing, self.bad_fmt=missing, bad_fmt

    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val=super(PartialFormatter, self).get_field(field_name, args, kwargs)
            # Python 3, 'super().get_field(field_name, args, kwargs)' works
        except (KeyError, AttributeError):
            #val=None,field_name
            val='{'+field_name+'}',field_name
        return val

    def format_field(self, value, spec):
        # handle an invalid format
        if value==None: return self.missing
        try:
            return super(PartialFormatter, self).format_field(value, spec)
        except ValueError:
            if self.bad_fmt is not None: return self.bad_fmt
            else: raise


def substitute_props(props_dict, *format_props):

    '''
    tenants_template_merged_propag_subst = [substitute_props(tenant_template_merged_propag, system_props) for tenant_template_merged_propag in tenants_template_merged_propag]
    '''
    from dict import merge_dicts

    all_props = merge_dicts(*format_props)
    fmt = PartialFormatter()
    subst_dick = {}

    for p in props_dict.iterkeys():
        if isinstance(props_dict[p], dict):
            child = substitute_props(props_dict[p], *format_props)
        else:
            child = props_dict[p]
            child = fmt.format(child, **all_props) if (isinstance(child, str) or isinstance(child, unicode)) else child
        subst_dick[p] = child

    return subst_dick


def get_value_str(value):
    import re
    rgx = re.compile(r'^(python|bash)\((.*)[\\)]$')
    if isinstance(value, str) or isinstance(value, unicode):
        g = rgx.findall(value)
        if not g:
            return "'%s'" % value
        elif g[0][0].lower() == 'python':
            return eval(g[0][1])
        elif g[0][0].lower() == 'bash':
            return "%s" % g[0][1]
    else:
        return value