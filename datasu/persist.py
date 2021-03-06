__author__ = 'sashaostr'


import joblib as jb
import inspect
import os
from os import listdir
from os.path import isfile, join, splitext


def persist_variables(variables, path='persisted_vars'):

    frm = inspect.stack()[1]
    # mod = inspect.getmodule(frm[0])
    vars_dict = frm[0].f_globals

    if not isinstance(variables, list):
        variables = [variables]

    if not os.path.exists(path):
        os.makedirs(path)

    for v in variables:
        var_path = '%s/%s.var' % (path, v)
        print 'dumping %s to %s' % (v,var_path)
        jb.dump(vars_dict[v], filename=var_path, compress=True)


def load_variables(path='persisted_vars', variables=[], load_to_context=True):

    frm = inspect.stack()[1]
    # mod = inspect.getmodule(frm[0])
    context = frm[0].f_globals

    files = [f for f in listdir(path) if isfile(join(path, f)) and os.path.splitext(f)[1] == '.var']
    print files
    loaded = {}
    for fn in files:
        vn = splitext(fn)[0]
        if len(variables) == 0 or vn in variables:
            vv = jb.load(path+'/'+fn)
            loaded[vn] = vv
            if load_to_context:
                context[vn] = vv
            # exec("global {0}; {0}={1}".format(vn, vv))
            # setattr(mod, vn, vv)
            # exec("mod.{0}={1}".format(vn, vv))
            print 'loaded %s' % vn
    return loaded

