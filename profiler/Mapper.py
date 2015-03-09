import sys, datetime
sys.path.append("../common")

from objects import Table, Column, PrimaryKey, ForeignKey

class TableMapper():
    def __init__(self):
        pass

    def single(self, table, statistics=None):
        obj = Table()
        obj.db_catalog = table.info['db_catalog']
        obj.schema = table.info['schemaname']
        obj.tablename = table.name
        obj.num_rows = statistics['num_rows']
        obj.num_columns = statistics['num_columns']
        obj.num_explicit_inlinks = table.info['num_explicit_inlinks']
        obj.num_explicit_outlinks = table.info['num_explicit_outlinks']
        return obj

    def multiple(self, lst):
        retval = retval = [self.single(tup[0], tup[1]) for tup in lst]
        # for tup in lst:
        #     retval.append(self.single(tup[0], tup[1]))
        return retval

class ColumnMapper():
    def __init__(self):
        pass

    def single(self, column, statistics=None):
        obj = Column()
        obj.db_catalog = column.info['db_catalog']
        obj.schema = column.info['schemaname']
        obj.tablename = column.table.name
        obj.columnname = column.name
        obj.ordinal_position = -1
        obj.datatype = str(column.type)
        obj.num_nulls = statistics['num_nulls']
        obj.num_distinct_values = statistics['num_distinct_values']
        obj.min = statistics['min']
        obj.max = statistics['max']
        obj.avg = statistics['avg']
        obj.var = statistics['var']
        obj.median = statistics['median']
        obj.stdev = statistics['stdev']
        obj.sum = statistics['sum']
        obj.mode = statistics['mode']
        obj.lifespan_in_days = statistics['lifespan_in_days']
        obj.num_positive = statistics['num_positive']
        obj.num_negative = statistics['num_negative']
        obj.num_zero = statistics['num_zero']
        obj.start_date = statistics['start_date']
        obj.end_date = statistics['end_date']
        return obj

    def multiple(self, lst):
        retval = [self.single(tup[0], tup[1]) for tup in lst]
        # for tup in lst:
        #     retval.append(self.single(tup[0], tup[1]))
        return retval

class PrimaryKeyMapper():
    def __init__(self):
        pass

    def single(self, pk, statistics=None):
        obj = PrimaryKey()
        obj.db_catalog = pk['db_catalog']
        obj.schema = pk['schemaname']
        obj.tablename = pk['tablename']
        obj.db_columns = '|'.join(pk['constrained_columns'])
        obj.keyname = pk['name']
        obj.type = 'EXPLICIT'
        obj.score = 1.0
        obj.comment = ''
        obj.tags = ''
        obj.date_added = datetime.datetime.now()
        return obj

    def multiple(self, lst):
        retval = [self.single(item) for item in lst]
        return retval

class ForeignKeyMapper():
    def __init__(self):
        pass

    def single(self, fk, statistics=None):
        obj = ForeignKey()
        obj.db_catalog = fk['db_catalog']
        obj.pkdb_schema = fk['srcschema']
        obj.fkdb_schema = fk['referred_schema']
        obj.pktablename = fk['srcschema']
        obj.fktablename = fk['referred_table']
        obj.pk_columns = '|'.join(fk['constrained_columns'])
        obj.fk_columns = '|'.join(fk['referred_columns'])
        obj.keyname = fk['name']
        obj.type = 'EXPLICIT'
        obj.score = 1.0
        obj.comment = ''
        obj.tags = ''
        obj.date_added = datetime.datetime.now()
        return obj

    def multiple(self, lst):
        retval = [self.single(item) for item in lst]
        return retval