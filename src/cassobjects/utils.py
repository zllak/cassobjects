# -*- encoding: utf-8 -*-

"""Utils methods/objects for cassobjects"""

# Following is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

class ImmutableContainer(object):
    def _immutable(self, *arg, **kw):
        raise TypeError("%s object is immutable" % self.__class__.__name__)

    __delitem__ = __setitem__ = __setattr__ = _immutable

class immutabledict(ImmutableContainer, dict):

    clear = pop = popitem = setdefault = \
        update = ImmutableContainer._immutable

    def __new__(cls, *args):
        new = dict.__new__(cls)
        dict.__init__(new, *args)
        return new

    def __init__(self, *args):
        pass

    def __reduce__(self):
        return immutabledict, (dict(self), )

    def union(self, d):
        if not self:
            return immutabledict(d)
        else:
            d2 = immutabledict(self)
            dict.update(d2, d)
            return d2

    def __repr__(self):
        return "immutabledict(%s)" % dict.__repr__(self)
