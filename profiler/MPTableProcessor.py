import datetime, math, sys, argparse
sys.path.append("../util")

from osxnotifications import Notifier

from sqlalchemy import create_engine
from sqlalchemy.sql import select

from MetaModel import MetaModel
from multiprocessing import Pool

from contextlib import contextmanager

class MPTableProcessor():
    @contextmanager
    def dbconnection(self, engine):
        """Provide a transactional scope around a series of operations."""
        try:
            connection = engine.connect()
            yield connection
        except:
            connection.close()
            raise
        finally:
            connection.close()

    def __init__(self, connection_string, tables):
        self.connection_string = connection_string
        self.tables = tables

    def execute(self, processes=32, verbose=False):
        pool = Pool(processes=processes)
        result = []
        if verbose:
            print('')

        for i, _ in enumerate(pool.imap_unordered(self.profileOneTable, self.tables)):
            result.append(_)

            if verbose:
                sys.stdout.write("\033[1A")

                totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% \n".format(i, len(self.tables)-1, round(i/(len(self.tables)-1)*100,2))
                sys.stdout.write(totalprogress)
                sys.stdout.flush()

        return result;

    def profileOneTable(self, table=None):
        engine = create_engine(self.connection_string, pool_size=3, pool_recycle=3600)

        with self.dbconnection(engine) as subject:
            num_rows = subject.execute(table.count()).fetchone()[0]
            num_columns = len(table.columns)

            return (table, {'num_rows': num_rows, 'num_columns': num_columns})

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
    args = parser.parse_args()

    mm = MetaModel(args.src)

    sts = datetime.datetime.now()
    processor = MPTableProcessor(connection_string = args.src, tables = mm.tables())
    result = processor.execute(processes=32, verbose=True)
    duration = datetime.datetime.now() - sts

    print('number of processed tables: ' + str(len(result)))

    # Calling the notification function
    Notifier.notify(title='cobr.io ds-toolkit', subtitle='MPTableProcessor done!', message='processed: ' + str(len(result)) + ' tables in ' + str(math.floor(duration.total_seconds())) + ' seconds')