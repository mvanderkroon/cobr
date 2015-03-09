import sys, datetime, argparse, math
sys.path.append("../util")

from osxnotifications import Notifier
from MetaModel import MetaModel
from MPColumnProcessor import MPColumnProcessor
from MPTableProcessor import MPTableProcessor
from NumpyColumnProcessor import NumpyColumnProcessor
from Mapper import TableMapper, ColumnMapper, PrimaryKeyMapper, ForeignKeyMapper

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def main(args):
    sts = datetime.datetime.now()

    miner = MetaModel(args.src)

    columnmapper = ColumnMapper()
    tablemapper = TableMapper()
    pkmapper = PrimaryKeyMapper()
    fkmapper = ForeignKeyMapper()

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
        columnprocessor = NumpyColumnProcessor,
        mapper = columnmapper)
    pcolumns = cp.execute(processes=32, verbose=True)

    cets = datetime.datetime.now()
    Notifier.notify(title='cobr.io ds-toolkit',
        subtitle='MPColumnProcessor done!',
        message='processed: ' + str(len(pcolumns)) + ' columns in ' + str(math.floor((cets - sts).total_seconds())) + ' seconds')

    print('')
    print('## processing tables...')
    tp = MPTableProcessor(connection_string = args.src, tables = tables, mapper = tablemapper)
    ptables = tp.execute(processes=32, verbose=True)

    Notifier.notify(title='cobr.io ds-toolkit',
        subtitle='MPTableProcessor done!',
        message='processed: ' + str(len(ptables)) + ' tables in ' + str(math.floor((datetime.datetime.now() - cets).total_seconds())) + ' seconds')


    if not args.dry_run:
        engine = create_engine(args.target)
        Session = sessionmaker(bind=engine)
        session = Session()

        writeToDb(session, ptables)
        writeToDb(session, pcolumns)
        # writeToDb(session, ppks)
        # writeToDb(session, pfks)

    print('')
    print('## time elapsed: ' + str(datetime.datetime.now() - sts))

    Notifier.notify(title='cobr.io ds-toolkit',
        subtitle='Profiling done!',
        message='duration: ' + str(math.floor((datetime.datetime.now() - sts).total_seconds())) + ' seconds')

def writeToDb(session, objects):
    try:
        for obj in objects:
            session.add(obj)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
    parser.add_argument("-t", "--target", help="connection_string for the target-database", metavar="string")
    parser.add_argument("-d", "--dry_run", help="flag to make a dry-run without storing the result to target database", action='store_true', default=False)
    args = parser.parse_args()

    main(args)
