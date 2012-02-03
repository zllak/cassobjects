# -*- encoding: utf-8 -*-

"""Defined models for cassobjects

Two classes:
    - Model: a simple class representing a "standard" SQL table
    - TimestampedModel: this class represents an object that will be altered in
      time. Only one object will be stored by rowkey. Each column will be a new
      version of the object. The last column always represents the last state
      of the object.

The internal mechanism if largely inspired by the way SQLAlchemy works.
It defines classes (models) where properties maps columns.
Each model can be instanciated to represent a single row (object) from
Cassandra.

By now, it only supports read, create columns families, and create new entries
in columns families. Objects cannot be updated and saved.

"""

import inspect

from functools import partial

from pycassa import ConnectionPool
from pycassa.types import CassandraType
from pycassa.columnfamily import ColumnFamily
from pycassa.index import create_index_expression, create_index_clause

from cassobjects.utils import immutabledict

__all__ = ['declare_model', 'MetaModel', 'MetaTimestampedModel', 'Column']

class ModelException(Exception):
    """An exception occured during Model parsing/construction"""
    pass

# Will hold ConnectionPool objects, where keys are the keyspace
DEFAULT_KEYSPACE = 'Keyspace'
DEFAULT_HOSTS = ['localhost:9160']
POOLS = {}

#################
# Column object #
#################

class Column(object):
    """Wraps "metadata" of a field into this class.

    This class allow to define metadata for a model field, like if it's an
    index.
    Column type must be a valid Cassandra Type, can also be a CompositeType,
    the class or directly an instanciated object.
    For a CompositeType, arguments *MUST* be instanciated objects, not the
    class (ie: CompositeType(UTF8Type(), IntegerType())).
    Column types are the pycassa types. They accepts the same parameters.

    """
    def __init__(self, *args, **kwargs):
        self.index = kwargs.get('index', False)
        self.foreign_key = kwargs.get('foreign_key', None)
        self.alias = None
        self.col_type = None
        args = list(args)
        if args:
            if isinstance(args[0], basestring):
                self.alias = args.pop(0)
        if args:
            self.col_type = args[0]
        if self.col_type is None:
            raise ModelException("Column needs to have a type")
        if (inspect.isclass(self.col_type) and not issubclass(self.col_type, CassandraType)) \
            or (not inspect.isclass(self.col_type) and not issubclass(self.col_type.__class__, CassandraType)):
            raise ModelException("Column type must be an instance or a class "
                                 "inherited from a cassandra type: %s" % self.col_type)
        # instanciate the CassandraType if not already done in model
        if inspect.isclass(self.col_type):
            self.col_type = self.col_type()

    def do_init(self, local_class):
        """No initialization is needed"""
        pass

    def get(self, instance):
        """No need"""
        pass

##################
# models classes #
##################

class ModelAttribute(object):
    """This class wraps attributes (Column, ModelRelationship) of a Model with
    a descriptor-like object.
    As there is only one object instanciated in the Model inherited class, this
    class must store and retrieve values for all Model object instances.

    """
    def __init__(self, host_class, attribute, prop):
        self.host_class = host_class
        self.attribute = attribute
        self.prop = prop
        self.values = {}

    def __get__(self, instance, owner):
        """Access to the object is made.
        If `instance` is None, it means that we are called on the class object,
        so, simply returns self.
        If the returned value has already been saved, just returns it.
        Otherwise, we initialize the property, and get the value.

        """
        if instance is None:
            return self
        self.prop.do_init(self.host_class)
        if not self.values.get(instance):
            self.values[instance] = self.prop.get(instance)
        return self.values[instance]

    def __set__(self, instance, value):
        """Set a value for a Model instance object attribute"""
        if instance not in self.values:
            self.values[instance] = value
        else:
            #TODO: updating objects is currently not supported
            pass

class MetaModel(type):
    """Represents a "standard" SQL table mapped on top of Cassandra.

    As there is no such concepts as "foreign keys" in Cassandra, the foreign
    keys created in cassobjects are only based on rowkey values. Column
    families can be in different keyspaces, and still have "cassobjects foreign
    keys" working.

    """
    def __init__(cls, name, bases, dct):
        """Verify model validity, add methods in `cls` to access indexes,
        etc...

        """
        if 'registry' in cls.__dict__:
            return type.__init__(cls, name, bases, dct)
        # Indexes
        indexes = dct.get('__indexes__', [])
        columns = {}
        for attr, value in cls.__dict__.items():
            if isinstance(value, Column):
                if hasattr(value, 'index') and value.index:
                    setattr(cls, 'get_by_%s' % attr, partial(cls.get_by, attr))
                    setattr(cls, 'get_one_by_%s' % attr, partial(cls.get_one_by, attr))
                columns[attr] = value
                setattr(cls, attr, ModelAttribute(cls, attr, value))
            elif isinstance(value, ModelRelationship):
                setattr(cls, attr, ModelAttribute(cls, attr, value))
        if indexes:
            raise ModelException('Following indexes "%s" are not declared as '
                                 'fields' % ','.join(indexes))
        # Column family name
        if '__column_family__' not in dct:
            cls.__column_family__ = cls.__name__.lower()

        # add the model in the CFRegistry object
        cls.registry.add(cls, columns)

        return type.__init__(cls, name, bases, dct)

    def get_by(cls, attribute, value):
        """Only works for columns indexed in Cassandra.
        This means that the property must be in the __indexes__ attribute.

        :param attribute: The attribute to lookup.
          This argument is always provided by the partial method.

        :param value: The value to match.

        Returns a list of matched objects.

        """
        col_fam = ColumnFamily(cls.pool, cls.__column_family__)
        clause = create_index_clause([create_index_expression(attribute, value)])
        idx_slices = col_fam.get_indexed_slices(clause)
        result = []
        for rowkey, columns in idx_slices:
            result.append(cls(rowkey, **columns))
        return result

    def get_one_by(cls, attribute, value):
        """Same as :meth:`get_one`, except that it will raise if more than one
        value is returned, and will return directly an object instead of a
        list.

        :param attribute: The attribute to lookup.
          This argument is always provided by the partial method.

        :param value: The value to match.

        """
        res = cls.get_by(attribute, value)
        if len(res) > 1 or len(res) == 0:
            raise ModelException("get_one_by_%s() returned more than one "
                                 "element or zero" % attribute)
        return res[0]

    # Maps pycassa.ColumnFamily methods
    def get(self, *args, **kwargs):
        col_fam = ColumnFamily(self.pool, self.__column_family__)
        return col_fam.get(*args, **kwargs)

    def multiget(self, *args, **kwargs):
        col_fam = ColumnFamily(self.pool, self.__column_family__)
        return col_fam.multiget(*args, **kwargs)

    def get_count(self, *args, **kwargs):
        col_fam = ColumnFamily(self.pool, self.__column_family__)
        return col_fam.get_count(*args, **kwargs)

    def multiget_count(self, *args, **kwargs):
        col_fam = ColumnFamily(self.pool, self.__column_family__)
        return col_fam.multiget_count(*args, **kwargs)

    def get_range(self, *args, **kwargs):
        col_fam = ColumnFamily(self.pool, self.__column_family__)
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
    This kind of model can't support Columns, and foreign keys.

    """
    def __init__(cls, name, bases, dct):
        return type.__init__(cls, name, bases, dct)

    def get_one_by_rowkey(self, rowkey):
        pass

#################################
# Column Family Registry object #
#################################

class CFRegistry(object):
    """Store all created models in an immutable dict, and also store classes"""
    def __init__(self):
        self.cfs = immutabledict()
        self.classes = immutabledict()

    def __contains__(self, item):
        if not isinstance(item, basestring):
            item = item.__column_family__
        return item in self.cfs

    def __getitem__(self, item):
        return dict.__getitem__(self.cfs, item)

    def add(self, klass, definition):
        name = klass.__column_family__
        dict.__setitem__(self.cfs, name, definition)
        dict.__setitem__(self.classes, name, klass)

    def remove(self, name):
        dict.pop(self.cfs, name)
        dict.pop(self.classes, name)

    def clear(self):
        dict.clear(self.cfs)
        dict.clear(self.classes)

    def get_class(self, name):
        return dict.__getitem__(self.classes, name)

    #TODO do we need this here ?
    def create_column_families(self):
        pass

# metaclass methods

def _model_constructor(self, rowkey, **kwargs):
    """Constructor for instanciated model objects.
    Simply set the given attributes, and the given rowkey.
    Handles correctly Column aliases.

    """
    kls = self.__class__
    setattr(self, 'rowkey', rowkey)
    for arg in kwargs:
        if not hasattr(kls, arg):
            # maybe it's an alias
            for key, value in kls.__dict__.items():
                if isinstance(value, ModelAttribute) and \
                    isinstance(value.prop, Column) and \
                    value.prop.alias == arg:
                    setattr(self, key, kwargs[arg])
                    break
            else:
                raise ModelException("%s can't be resolved in %s" % (arg, kls))
        else:
            setattr(self, arg, kwargs[arg])
_model_constructor.__name__ = '__init__'

def declare_model(cls=object, name='Model', metaclass=MetaModel,
                  keyspace=DEFAULT_KEYSPACE, hosts=DEFAULT_HOSTS,
                  constructor=_model_constructor):
    """Constructs a base class for models.
    All models inheriting from this base will share the same CFRegistry object.

    """
    local_reg = CFRegistry()
    POOLS.setdefault(keyspace, ConnectionPool(keyspace, hosts))
    return metaclass(name, (cls,), {'pool': POOLS[keyspace],
                                    'registry': local_reg,
                                    '__init__': constructor})

# Relationships between models

class ModelRelationship(object):
    def __init__(self, target_kls, **kwargs):
        self.target = target_kls
        self.kwargs = kwargs
        self._initialized = False
        self.target_method = None

    def do_init(self, local_class):
        """Resolve the relationship, and creates backref if necessary.
        Look in CFRegistry if the remote side (column family) is present,
        then look up for a foreign key linking to this instance.

        :param local_class: class on which the relationship is attached

        """
        if self._initialized:
            return
        # find the remote side.
        registry = local_class.registry
        if self.target not in registry:
            raise ModelException('Model with column family name "%s" not found '
                                 'not found in registry' % self.target)
        target_model = registry.get_class(self.target)
        # find foreign key
        local_cf = local_class.__column_family__
        for col, value in registry[self.target].items():
            if value.foreign_key == local_cf:
                name = value.alias or col
                self.target_method = partial(getattr(target_model, 'get_by'), name)
        if self.target_method is None:
            raise ModelException('No foreign key found in "%s" for relationship '
                                 '"%s"' % (self.target, local_cf))
        self._initialized = True

    def get(self, instance):
        """
        """
        assert self._initialized
        return self.target_method(instance.rowkey)

#TODO
def relationship(target_kls, **kwargs):
    return ModelRelationship(target_kls, **kwargs)
