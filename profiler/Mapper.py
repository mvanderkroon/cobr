import sys, datetime
sys.path.append("../common")

from objects import Table, Column, PrimaryKey, ForeignKey

class TableMapper():
    def __init__(self):
        pass

    def single(self, table, statistics):
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
        retval = []
        for tup in lst:
            retval.append(self.single(tup[0], tup[1]))
        return retval

class ColumnMapper():
    def __init__(self):
        pass

    def single(self, column, statistics):
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
        retval = []
        for tup in lst:
            retval.append(self.single(tup[0], tup[1]))
        return retval

class PrimaryKeyMapper():
    def __init__(self):
        pass

    def single(self, pk, statistics):
        obj = PrimaryKey()
        obj.db_catalog = self.db_catalog
        obj.schema = pk.table.metadata.schema
        obj.tablename = pk.table.name
        obj.db_columns = ''
        obj.keyname = ''
        obj.type = 'EXPLICIT'
        obj.score = 1.0
        obj.comment = ''
        obj.tags = ''
        obj.date_added = datetime.datetime.now()
        return obj

    def multiple(self, lst):
        retval = []
        for tup in lst:
            retval.append(self.single(tup[0], tup[1]))
        return retval

class ForeignKeyMapper():
    def __init__(self):
        pass

    def single(self, fk, statistics):
        obj = ForeignKey()
        obj.db_catalog = self.db_catalog
        obj.pkdb_schema = fk.table.metadata.schema
        obj.fkdb_schema = fk.table.metadata.schema
        obj.pktablename = fk.table.name
        obj.fktablename = fk.table.name
        obj.pk_columns = ''
        obj.fk_columns = ''
        obj.keyname = ''
        obj.type = 'EXPLICIT'
        obj.score = 1.0
        obj.comment = ''
        obj.tags = ''
        obj.date_added = datetime.datetime.now()
        return obj

    def multiple(self, lst):
        retval = []
        for tup in lst:
            retval.append(self.single(tup[0], tup[1]))
        return retval