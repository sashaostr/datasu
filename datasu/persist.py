__author__ = 'sasha'


import joblib as jb
import os
from os import listdir
from os.path import isfile, join


def persist_variables(variables, path='persisted_vars'):
    if not isinstance(variables,list):
        variables = [variables]

    if not os.path.exists(path):
        os.makedirs(path)

    for v in variables:
        var_path = '%s/%s' % (path, v)
        print 'dumping %s to %s' % (v,var_path)
        jb.dump(globals()[v], filename=var_path, compress=True)


def load_variables(path='persisted_vars', variables=[]):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    print files
    for v in files:
        if len(variables) == 0 or v in variables:
            globals()[v] = jb.load(path+'/'+v)
            print 'loaded %s' % v

