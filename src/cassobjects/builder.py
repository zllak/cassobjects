# -*- encoding: utf-8 -*-

"""This class is in charge of creating the column families in Cassandra.

"""

from pycassa.system_manager import *

from cassobjects.models import MetaModel, MetaTimestampedModel, Column
from cassobjects.types import TYPE_TABLE

# Exception
class BuilderException(Exception):
    """Something went wrong in the Builder"""
    pass

class Builder(object):
    """Create column families based on Models.

    This class is not responsible for the creation of the keyspace.
    It will only creates column families, secondary indexes, and
    "manually" created secondary indexes.

    If a column family is already created, it will not try to override them,
    unless if the `force` keyword is given.

    """
    @classmethod
    def create(cls, *classes, **kwargs):
        """Collect informations about classes
        """
        force = kwargs.get('force', False)

        for klass in classes:
            if isinstance(klass, MetaModel):
                cls._create_metamodel(klass, force)
            elif isinstance(klass, MetaTimestampedModel):
                cls._create_metatimestampedmodel(klass, force)
            else:
                raise BuilderException("%s is not recognized as a cassobjects "
                                       "class" % klass)

    @classmethod
    def _create_metamodel(cls, klass, force):
        """Creates family column for a MetaModel inherited class.

        Only one primary key can be specified by model.
        If a field is listed as an index, creates a Cassandra secondary index.
        Arbitrary connects to the first server found in the class
        ConnectionPool.

        """
        dct = klass.__dict__
        cf = klass.__column_family__
        pool = klass.__pool__
        sys = SystemManager(pool.server_list[0])
        try:
            cfs_keyspace = sys.get_keyspace_column_families(pool.keyspace)
            if cf in cfs_keyspace and not force:
                #FIXME: remove this
                #FIXME: remove column family before
                return
            primary = None
            cvclasses = {}
            indexes = {}
            for attr, value in dct.items():
                if isinstance(value, Column):
                    name = value.alias or attr
                    if value.primary_key:
                        if primary is not None:
                            raise BuilderException("%s: Only one key can be used "
                                                   "as primary" % klass)
                        primary = value
                    if value.index:
                        indexes[name] = TYPE_TABLE[value.col_type]
                    cvclasses[name] = TYPE_TABLE[value.col_type]
            if primary is None:
                raise BuilderException("%s: No primary key" % klass)
            # Create column family
            sys.create_column_family(pool.keyspace, cf, super=False,
                                     comparator_type=UTF8_TYPE,
                                     column_validation_classes=cvclasses,
                                     comment="Generated by cassobjects")
            # Create secondary indexes
            for name, value_type in indexes.items():
                sys.create_index(pool.keyspace, cf, name, value_type,
                                 index_name='%s_%s_index' % (cf, name))
        finally:
            sys.close()

    @classmethod
    def _create_metatimestampedmodel(cls, klass, force):
        """
        """
        pass

    @classmethod
    def _create_relation(cls, klass, force):
        """
        """
        pass