# -*- encoding: utf-8 -*-

"""Defined models for cassobjects

Two classes:
    - Model: a simple class representing a "standard" SQL table
    - TimestampedModel: this class represents an object that will be altered in
      time. Only one object will be stored by rowkey. Each column will be a new
      version of the object. The last column always represents the last state
      of the object.

"""

from __future__ import absolute_import

from functools import partial
from types import StringTypes

from pycassa import ConnectionPool
from pycassa.types import CassandraType
from pycassa.columnfamily import ColumnFamily
from pycassa.index import create_index_expression, create_index_clause

__all__ = ['declare_model', 'MetaModel', 'MetaTimestampedModel', 'Column']

class ModelException(Exception):
    """An exception occured during Model parsing/construction"""
    pass

# Will hold ConnectionPool objects, where keys are the keyspace
DEFAULT_KEYSPACE = 'Keyspace'
DEFAULT_HOSTS = ['localhost:9160']
POOLS = {}

# Column object

class Column(object):
    """

    """
    def __init__(self, *args, **kwargs):
        self.index = kwargs.get('index', False)
        self.primary_key = kwargs.get('primary_key', False)
        if isinstance(args[0], StringTypes):
            self.alias = args[0]
            self.col_type = args[1]
        elif issubclass(args[0], CassandraType):
            self.alias = None
            self.col_type = args[0]
        else:
            raise ModelException("Unrecognized parameter: %s" % args[0])


# models classes

class MetaModel(type):
    """Represents a "standard" SQL table mapped on top of Cassandra.

    TODO: Explain how models works

    """
    def __init__(cls, name, bases, dct):
        """Verify model validity, add methods in `cls` to access indexes,
        etc...

        """
        if 'pool' in dct:
            # We are in the 'Model' constructor method, so, we store the pool
            cls.__pool__ = dct['pool']
        # Indexes
        indexes = dct.get('__indexes__', [])
        for attr, value in dct.items():
            if isinstance(value, Column):
                if value.index:
                    setattr(cls, 'get_by_%s' % attr, partial(cls.get_by, attr))
                    setattr(cls, 'get_one_by_%s' % attr, partial(cls.get_one_by, attr))
        if indexes:
            raise ModelException('Following indexes "%s" are not declared as '
                                 'fields' % ','.join(indexes))
        # Column family name
        if '__column_family__' not in dct:
            cls.__column_family__ = cls.__name__.lower()
        return type.__init__(cls, name, bases, dct)

    def get_by(self, attribute, value):
        """Only works for columns indexed in Cassandra.
        This means that the property must be in the __indexes__ attribute.

        :param attribute: The attribute to lookup.
          This argument is always provided by the partial method.

        :param value: The value to match.

        """
        col_fam = ColumnFamily(self.__pool__, self.__column_family__)
        clause = create_index_clause([create_index_expression(attribute, value)])
        idx_slices = col_fam.get_indexed_slices(clause)
        return attribute

    def get_one_by(self, attribute, value):
        """Same as :meth:`get_one`, except that it will raise if more than one
        value is returned, and will return directly an object instead of a
        list.

        :param attribute: The attribute to lookup.
          This argument is always provided by the partial method.

        :param value: The value to match.

        """
        values = self.get_by(attribute, value)
        return attribute

    # Maps pycassa.ColumnFamily methods
    def get(self, *args, **kwargs):
        col_fam = ColumnFamily(self.__pool__, self.__column_family__)
        return col_fam.get(*args, **kwargs)

    def multiget(self, *args, **kwargs):
        col_fam = ColumnFamily(self.__pool__, self.__column_family__)
        return col_fam.multiget(*args, **kwargs)

    def get_count(self, *args, **kwargs):
        col_fam = ColumnFamily(self.__pool__, self.__column_family__)
        return col_fam.get_count(*args, **kwargs)

    def multiget_count(self, *args, **kwargs):
        col_fam = ColumnFamily(self.__pool__, self.__column_family__)
        return col_fam.multiget_count(*args, **kwargs)

    def get_range(self, *args, **kwargs):
        col_fam = ColumnFamily(self.__pool__, self.__column_family__)
        return col_fam.get_range(*args, **kwargs)

    def insert(self, key, columns, timestamp=None, ttl=None, write_consistency_level='ALL'):
        # check si la row existe deja
        pass


class MetaTimestampedModel(type):
    """Represents a serialized object that will be altered in time.

    You can picture this model as the timeline of an object. Only one
    object is stored by row. Each column will be a new verseion of the
    object. Column keys are TimeUUID. Column values are the serialized
    object.

    """
    def __init__(cls, name, bases, dct):
        return type.__init__(cls, name, bases, dct)

    def get_one_by_rowkey(self, rowkey):
        pass


def declare_model(cls=object, name='Model', metaclass=MetaModel,
                  keyspace=DEFAULT_KEYSPACE, hosts=DEFAULT_HOSTS):
    """Constructs a base class for models"""
    POOLS.setdefault(keyspace, ConnectionPool(keyspace, hosts))
    return metaclass(name, (cls,), {'pool': POOLS[keyspace]})

# Relationships between models

#TODO
def relationship(target_kls, **kwargs):
    return None
