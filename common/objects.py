from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event
import datetime, math

import numpy as np

Base = declarative_base()

multistring_separator = '|'  # used to combine multi column keys in fields


def typestring(value):
    return str(type(value))[8:].replace("'>", "")


def validate_float(value):
    allowed_types = ['numpy.int_', 'numpy.intc', 'numpy.intp', 'numpy.int64',
        'numpy.int32', 'numpy.int16', 'numpy.int8', 'numpy.uint8',
        'numpy.uint16', 'numpy.uint32', 'numpy.uint64', 'numpy.float_',
        'numpy.float16', 'numpy.float32', 'numpy.float64', 'numpy.complex_',
        'numpy.complex64', 'numpy.complex128', 'int', 'float']

    if typestring(value) in allowed_types:
        if math.isnan(value):
            value = None
    elif value is None:
        pass
    else:
        print(type(value))
        print('float property set to non-float value <{0}>; defaulting value to <None>'.format(value))
        value = None
    return value


def validate_int(value):
    allowed_types = ['numpy.int_', 'numpy.intc', 'numpy.intp', 'numpy.int64',
        'numpy.int32', 'numpy.int16', 'numpy.int8', 'numpy.uint8',
        'numpy.uint16', 'numpy.uint32', 'numpy.uint64', 'int']

    if typestring(value) in allowed_types:
        pass
    elif value is None:
        pass
    else:
        print('int property set to non-int value <{0}>; defaulting value to <None>'.format(value))
        value = None
    return value


def validate_string(value):
    if isinstance(value, str):
        pass
    elif value is None:
        pass
    else:
        print('str property set to non-str value <{0}>; defaulting value to <None>'.format(value))
        value = None
    return value


def validate_datetime(value):
    if isinstance(value, datetime.datetime):
        pass
    elif value is None:
        pass
    else:
        print('datetime.datetime property set to non-datetime.datetime value <{0}>; defaulting value to <None>'.format(value))
        value = None
    return value

validators = {
    Integer: validate_int,
    String: validate_string,
    DateTime: validate_datetime,
    Float: validate_float
}


# this event is called whenever an attribute
# on a class is instrumented
@event.listens_for(Base, 'attribute_instrument')
def configure_listener(class_, key, inst):
    if not hasattr(inst.property, 'columns'):
        return

    # this event is called whenever a "set"
    # occurs on that instrumented attribute
    @event.listens_for(inst, "set", retval=True)
    def set_(instance, value, oldvalue, initiator):
        validator = validators.get(inst.property.columns[0].type.__class__)
        if validator:
            return validator(value)
        else:
            return value


class PrimaryKey(Base):
    __tablename__ = 'mprimarykey'

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_catalog = Column(String(512))
    db_schema = Column(String(512))
    tablename = Column(String(512))
    db_columns = Column(String(512))
    keyname = Column(String(512))
    type = Column(String(512))
    score = Column(Float)
    comment = Column(String(2048))
    tags = Column(String(2048))
    date_added = Column(DateTime)

    def __init__(self, db_catalog='', db_schema='', tablename='', db_columns=[], keyname='', type='explicit'):
        self.db_catalog = db_catalog
        self.db_schema = db_schema
        self.tablename = tablename
        self.db_columns = multistring_separator.join(db_columns)  # can be single...
        self.keyname = keyname
        self.type = type
        self.date_added = datetime.datetime.now()

    def __str__(self):
        return "{0}.{1}.{2:100}{3}".format(self.db_catalog, self.db_schema, self.tablename, self.keyname)


class ForeignKey(Base):
    __tablename__ = 'mforeignkey'

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_catalog = Column(String(512))
    pkdb_schema = Column(String(512))
    fkdb_schema = Column(String(512))
    pktablename = Column(String(512))
    fktablename = Column(String(512))
    pk_columns = Column(String(512))
    fk_columns = Column(String(512))
    keyname = Column(String(512))
    type = Column(String(512))
    score = Column(Float)
    comment = Column(String(2048))
    tags = Column(String(2048))
    date_added = Column(DateTime)

    def __init__(self, db_catalog='', pkdb_schema='', fkdb_schema='', pktablename='', fktablename='', pk_columns=[], fk_columns=[], keyname='', type='explicit'):
        self.db_catalog = db_catalog
        self.pkdb_schema = pkdb_schema
        self.fkdb_schema = fkdb_schema
        self.pktablename = pktablename
        self.fktablename = fktablename
        self.pk_columns = multistring_separator.join(pk_columns)  # can be single...
        self.fk_columns = multistring_separator.join(fk_columns)  # can be single...
        self.keyname = keyname
        self.type = type
        self.date_added = datetime.datetime.now()

    def __str__(self):
        return "{0}.{1:10} {2:50} {3}".format(self.db_catalog, self.db_schema, self.pktablename, self.fktablename)


class Table(Base):
    __tablename__ = 'mtable'

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_catalog = Column(String(512))
    db_schema = Column(String(512))
    tablename = Column(String(512))
    num_rows = Column(Integer)
    num_columns = Column(Integer)
    num_explicit_inlinks = Column(Integer)
    num_explicit_outlinks = Column(Integer)
    num_implicit_outlinks = Column(Integer)
    num_implicit_inlinks = Column(Integer)
    num_emptycolumns = Column(Integer)
    tablefillscore = Column(Float)
    comment = Column(String(2048))
    tags = Column(String(2048))
    date_added = Column(DateTime)

    def __init__(self, db_catalog='', db_schema='', tablename=''):
        self.db_catalog = db_catalog
        self.db_schema = db_schema
        self.tablename = tablename

        self.num_rows = None
        self.num_columns = None

        self.date_added = datetime.datetime.now()

    def __str__(self):
        return "{0}.{1}.{2}".format(self.db_catalog, self.db_schema, self.tablename)


class Column(Base):
    __tablename__ = 'mcolumn'

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_catalog = Column(String(512))
    db_schema = Column(String(512))
    tablename = Column(String(512))
    columnname = Column(String(512))
    ordinal_position = Column(Integer)
    datatype = Column(String(512))
    num_nulls = Column(Integer)
    num_distinct_values = Column(Integer)

    # NUMERIC
    min = Column(Float)
    max = Column(Float)
    avg = Column(Float)
    stdev = Column(Float)
    variance = Column(Float)
    sum = Column(Float)
    median = Column(Float)
    mode = Column(Float)
    quantile0 = Column(String(512))
    quantile1 = Column(String(512))
    quantile2 = Column(String(512))
    quantile3 = Column(String(512))
    quantile4 = Column(String(512))
    num_negative = Column(Integer)
    num_positive = Column(Integer)
    num_zero = Column(Integer)

    # DATETIME
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    lifespan_in_days = Column(Integer)
    workdays = Column(Integer)
    weekends = Column(Integer)
    holidays = Column(Integer)
    quarter1 = Column(Integer)
    quarter2 = Column(Integer)
    quarter3 = Column(Integer)
    quarter4 = Column(Integer)
    m1 = Column(Integer)  # January
    m2 = Column(Integer)
    m3 = Column(Integer)
    m4 = Column(Integer)
    m5 = Column(Integer)
    m6 = Column(Integer)
    m7 = Column(Integer)
    m8 = Column(Integer)
    m9 = Column(Integer)
    m10 = Column(Integer)
    m11 = Column(Integer)
    m12 = Column(Integer)
    d1 = Column(Integer)  # Monday
    d2 = Column(Integer)
    d3 = Column(Integer)
    d4 = Column(Integer)
    d5 = Column(Integer)
    d6 = Column(Integer)
    d7 = Column(Integer)

    # TEXT
    word_frequency = Column(String(2048))

    comment = Column(String(2048))
    tags = Column(String(2048))
    date_added = Column(DateTime)

    def __init__(self, db_catalog='', db_schema='', tablename='', columnname='', datatype=None, ordinal_position=None):
        self.db_catalog = db_catalog
        self.db_schema = db_schema
        self.tablename = tablename
        self.columnname = columnname
        self.datatype = datatype
        self.ordinal_position = ordinal_position
        self.date_added = datetime.datetime.now()

        self.num_nulls = None
        self.num_distinct_values = None
        self.min = None
        self.max = None
        self.avg = None

    def __str__(self):
        return "{0}.{1}.{2:100}{3}".format(self.db_catalog, self.db_schema, self.tablename, self.columnname)
