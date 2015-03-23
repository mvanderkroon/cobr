import datetime, math, sys, argparse
sys.path.append("../util")

# from osxnotifications import Notifier

from sqlalchemy import create_engine
from sqlalchemy.sql import select

from MetaModel import MetaModel
from multiprocessing import Pool

from NumpyColumnProcessor import NumpyColumnProcessor


class MPColumnProcessor():
    def __init__(self, connection_string, columns, columnprocessor, mapper=None):
        self.connection_string = connection_string
        self.columns = columns
        self.columnprocessor = columnprocessor
        self.mapper = mapper

    def execute(self, processes=2, verbose=False):
        pool = Pool(processes=processes)
        result = []
        if verbose:
            print('')

        for i, _ in enumerate(pool.imap_unordered(self.profileOneColumn, self.columns)):
            result.append(_)

            if verbose:
                sys.stdout.write("\033[1A")
                totalprogress = "\r\033[K## progress {0}/{1}: {2:.2f}% : {3} \n".format(i+1, len(self.columns), round(i/(len(self.columns)-1)*100,2), str(_.tablename + '.' + _.columnname))
                sys.stdout.write(totalprogress)
                sys.stdout.flush()

        pool.close()
        return result

    def profileOneColumn(self, column=None):
        try:
            engine = create_engine(self.connection_string)
            conn = engine.connect()

            s = select([column])
            result = conn.execute(s)

            values = [d[0] for d in result.fetchall()]

            cp = self.columnprocessor(column.type, values)

            return self.mapper.single(column, cp.doOperations())
        except Exception as ex:
            print(ex)
            print('')
        finally:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
    args = parser.parse_args()

    mm = MetaModel(args.src)

    sts = datetime.datetime.now()
    processor = MPColumnProcessor(connection_string=args.src,
        columns=mm.columns(),
        columnprocessor=NumpyColumnProcessor)
    result = processor.execute(processes=1, verbose=True)
    duration = datetime.datetime.now() - sts

    print('number of processed columns: ' + str(len(result)))

    # Calling the notification function
    # Notifier.notify(title='cobr.io ds-toolkit', subtitle='MPColumnProcessor done!', message='processed: ' + str(len(result)) + ' columns in ' + str(math.floor(duration.total_seconds())) + ' seconds')