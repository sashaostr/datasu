__author__ = 'sasha'


import joblib as jb
import os
from os import listdir
from os.path import isfile, join


def persist_variables(variables, vars_dict=globals(), path='persisted_vars'):
    if not isinstance(variables, list):
        variables = [variables]

    if not os.path.exists(path):
        os.makedirs(path)

    for v in variables:
        var_path = '%s/%s' % (path, v)
        print 'dumping %s to %s' % (v,var_path)
        jb.dump(vars_dict[v], filename=var_path, compress=True)


def load_variables(path='persisted_vars', variables=[]):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    print files
    for vn in files:
        if len(variables) == 0 or vn in variables:
            vv = jb.load(path+'/'+vn)
            exec("global {0}; {0}={1}".format(vn, vv))
            print 'loaded %s' % vn

