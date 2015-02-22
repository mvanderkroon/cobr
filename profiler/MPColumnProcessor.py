import datetime, math, sys, argparse
sys.path.append("../util")

from osxnotifications import Notifier

from sqlalchemy import create_engine
from sqlalchemy.sql import select

from MetaModel import MetaModel
from multiprocessing import Pool

from contextlib import contextmanager

from NumpyColumnProcessor import NumpyColumnProcessor

class MPColumnProcessor():
    @contextmanager
    def dbconnection(self):
        """Provide a transactional scope around a series of operations."""
        try:
            engine = create_engine(self.connection_string, pool_size=3, pool_recycle=3600)
            connection = engine.connect()
            yield connection
        except:
            connection.close()
            raise
        finally:
            connection.close()

    def __init__(self, connection_string, columns, columnprocessor):
        self.connection_string = connection_string
        self.columns = columns
        self.columnprocessor = columnprocessor

    def execute(self, processes=32, verbose=False):
        pool = Pool(processes=processes)
        result = []
        if verbose:
            print('')

        for i, _ in enumerate(pool.imap_unordered(self.profileOneColumn, self.columns)):
            result.append(_)

            if verbose:
                sys.stdout.write("\033[1A")

                totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% \n".format(i, len(self.columns)-1, round(i/(len(self.columns)-1)*100,2))
                sys.stdout.write(totalprogress)
                sys.stdout.flush()

        return result;

    def profileOneColumn(self, column=None):
        with self.dbconnection() as subject:
            s = select([column])
            result = subject.execute(s)

            values = [d[0] for d in result.fetchall()]

            cp = self.columnprocessor(values)
            return (column, cp.doOperations())

        # cleanup??
        engine.dispose()
        engine = None

    # def writeToDb(self, objects):
    #     Session = sessionmaker(bind=engine)
    #     session = Session()

    #     try:
    #         for obj in objects:
    #             session.add(obt)
    #         session.commit()
    #     except:
    #         session.rollback()
    #         raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
    args = parser.parse_args()

    mm = MetaModel(args.src)

    sts = datetime.datetime.now()
    processor = MPColumnProcessor(connection_string = args.src, \
        columns = mm.columns(), \
        columnprocessor = NumpyColumnProcessor)
    result = processor.execute(processes=32, verbose=True)

    duration = datetime.datetime.now() - sts

    print('number of processed columns: ' + str(len(result)))

    # Calling the notification function
    Notifier.notify(title='cobr.io ds-toolkit', subtitle='MPColumnProcessor done!', message='processed: ' + str(len(result)) + ' columns in ' + str(math.floor(duration.total_seconds())) + ' seconds')
