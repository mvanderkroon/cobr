import sys, datetime, argparse
sys.path.append("../util")

from osxnotifications import Notifier
from MetaModel import MetaModel
from MPColumnProcessor import MPColumnProcessor
from MPTableProcessor import MPTableProcessor
from NumpyColumnProcessor import NumpyColumnProcessor
from Mapper import TableMapper, ColumnMapper

def main(args):
    sts = datetime.datetime.now()

    miner = MetaModel(args.src)

    columnmapper = ColumnMapper()
    tablemapper = TableMapper()

    columns = miner.columns()
    tables = miner.tables()
    fks = miner.foreignKeys()
    pks = miner.primaryKeys()

    print('## cols: ' + str(len(columns)))
    print('## tables: ' + str(len(tables)))
    print('## fks: ' + str(len(fks)))
    print('## pks: ' + str(len(pks)))

    print('')
    print('## processing columns...')
    cp = MPColumnProcessor(connection_string = args.src,
        columns = columns, \
        columnprocessor = NumpyColumnProcessor)
    col_result = cp.execute(processes=32, verbose=True)

    print('')
    print('## processing tables...')
    tp = MPTableProcessor(connection_string = args.src, tables = tables)
    table_result = tp.execute(processes=32, verbose=True)

    tablemapper.multiple(table_result)

    # TBD: load all columns/tables at this point, then incrementally merge() as below operations become done

    # ColumnProcessor(columns=columns, getDataFn=miner.getDataForColumn, processor=NumpyColumnProcessor).execute()
    # PostProcessor(tables=tables, columns=columns, explicit_primarykeys=pks, explicit_foreignkeys=fks, processor=SimplePostProcessor)
    # TableProcessor(tables=tables, processor=miner).execute()

    # writer.reset() # dropping the database tables and recreating them

    # writer.writeTables(tables)
    # writer.writeColumns(columns)
    # writer.writePrimaryKeys(pks)
    # writer.writeForeignKeys(fks)
    # writer.close()

    print('')
    print('## time elapsed: ' + str(datetime.datetime.now() - sts))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
    parser.add_argument("-t", "--target", help="connection_string for the target-database", metavar="string")
    args = parser.parse_args()

    main(args)
