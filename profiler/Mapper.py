import sys
sys.path.append("../common")

from objects import Table, Column

class TableMapper():
    def __init__(self):
        pass

    def single(self, table, statistics):
        print(table.metadata)
        exit()
        obj = Table()
        obj.db_catalog = ''
        obj.schema = ''
        obj.tablename = table.name
        obj.num_rows = statistics['num_rows']
        obj.num_columns = statistics['num_columns']
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
        obj.db_catalog = ''
        obj.schema = ''
        obj.tablename = column.table.name
        obj.columnname = column.name
        obj.ordinal_position = -1
        obj.datatype = column.type
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