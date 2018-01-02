
__author__ = 'sashaostr'

from itertools import chain

def flatmap(f, items):
    return chain.from_iterable(map(f, items))
