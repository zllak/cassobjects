# -*- encoding: utf-8 -*-

"""Maps pycassa types, to avoid importing pycassa directly.
Also provides some variables to go from pycassa types to
variables needed by pycassa.system_manager.

"""

from pycassa.types import AsciiType, BooleanType, BytesType, CompositeType, \
                          CounterColumnType, DateType, DoubleType, FloatType, \
                          IntegerType, LexicalUUIDType, LongType, TimeUUIDType, \
                          UTF8Type
from pycassa.system_manager import BYTES_TYPE, LONG_TYPE, INT_TYPE, ASCII_TYPE, \
                                   UTF8_TYPE, TIME_UUID_TYPE, LEXICAL_UUID_TYPE, \
                                   COUNTER_COLUMN_TYPE, DOUBLE_TYPE, FLOAT_TYPE, \
                                   BOOLEAN_TYPE, DATE_TYPE

TYPE_TABLE = {
    AsciiType: ASCII_TYPE,
    BooleanType: BOOLEAN_TYPE,
    BytesType: BYTES_TYPE,
    CounterColumnType: COUNTER_COLUMN_TYPE,
    DateType: DATE_TYPE,
    DoubleType: DOUBLE_TYPE,
    FloatType: FLOAT_TYPE,
    IntegerType: INT_TYPE,
    LexicalUUIDType: LEXICAL_UUID_TYPE,
    LongType: LONG_TYPE,
    TimeUUIDType: TIME_UUID_TYPE,
    UTF8Type: UTF8_TYPE,
}
