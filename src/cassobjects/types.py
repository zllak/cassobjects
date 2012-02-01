# -*- encoding: utf-8 -*-

"""Maps pycassa types, to avoid importing pycassa directly.
Also provides some variables to go from pycassa types to
variables needed by pycassa.system_manager.

"""

from pycassa.types import AsciiType, BooleanType, BytesType, CompositeType, \
                          CounterColumnType, DateType, DoubleType, FloatType, \
                          IntegerType, LexicalUUIDType, LongType, TimeUUIDType, \
                          UTF8Type
