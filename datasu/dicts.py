__author__ = 'sashaostr'


try:
    from collections import Mapping
except ImportError:
    Mapping = dict


def merge_dicts(*dicts):
    """
    Return a new dictionary that is the result of merging the arguments together.
    In case of conflicts, later arguments take precedence over earlier arguments.

    example: tenants_template_merged = [merge_dicts(template_conf, tenant_conf) for tenant_conf in all_tenants_conf.itervalues()]

    """
    updated = {}
    # grab all keys
    keys = set()
    for d in dicts:
        keys = keys.union(set(d))

    for key in keys:
        values = [d[key] for d in dicts if key in d]
        # which ones are mapping types? (aka dict)
        maps = [value for value in values if isinstance(value, Mapping)]
        if maps:
            # if we have any mapping types, call recursively to merge them
            updated[key] = merge_dicts(*maps)
        else:
            # otherwise, just grab the last value we have, since later arguments
            # take precedence over earlier arguments
            updated[key] = values[-1]
    return updated


def merge_dicts2(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def propagate_recursive_dict(dick, property_name, parent_name='', props={}, black_list=[]):
    '''
    tenants_template_merged_propag = [propagate_recursive_dict(tenant_template_merged, 'properties', black_list=['jobs']) for tenant_template_merged in tenants_template_merged]
    '''

    property_dick = merge_dicts(props, dict(dick[property_name]) if property_name in dick else {})
    propagated_dick = {property_name: merge_dicts(props, property_dick)} if parent_name not in black_list else {}

    for p in dick.iterkeys():
        if p != property_name:
            if isinstance(dick[p], dict):
                child = propagate_recursive_dict(dick[p], property_name, parent_name=p, props=property_dick, black_list=black_list)
            else:
                child = dick[p]
            propagated_dick[p] = child

    return propagated_dick



