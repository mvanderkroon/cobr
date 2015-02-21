from sqlalchemy.schema import Table
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

from contextlib import contextmanager

from optparse import OptionParser

class MetaModel():

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

    def __init__(self, connection_string, pool_size=3, pool_recycle=3600):
        engine = create_engine(connection_string, pool_size=pool_size, pool_recycle=pool_recycle)
        insp = reflection.Inspector.from_engine(engine)
        metadata = MetaData()

        # 'mine' the subject database for it's metamodel
        with self.dbconnection(engine) as connection:
            tablenames = insp.get_table_names()

            # getting all primary keys
            self.spks = []
            for tablename in tablenames:
                self.spks.extend(insp.get_primary_keys(tablename))

            # getting all foreign keys
            self.sfks = []
            for tablename in tablenames:
                self.sfks.extend(insp.get_foreign_keys(tablename))

            # getting all tables
            self.stables = []
            for tablename in tablenames:
                stable = Table(tablename, metadata, autoload=True, autoload_with=engine)
                # stable['num_rows'] = connection.execute(stable.count()).fetchone()[0]
                # stable['num_columns'] = len(stable.columns)
                self.stables.append(stable)

            # getting all columns
            self.scolumns = []
            for stable in self.stables:
                for column in stable.columns:
                    self.scolumns.append(column)

    def tables(self):
        return self.stables

    def columns(self):
        return self.scolumns

    def primaryKeys(self):
        return self.spks

    def foreignKeys(self):
        return self.sfks

parser = OptionParser()
parser.add_option("-c", "--connection_string", dest="connection_string", help="connection_string for the subject-database", metavar="string")
(options, args) = parser.parse_args()

if __name__ == "__main__":
    mm = MetaModel(options.connection_string)
    tables = mm.tables()
    columns = mm.columns()
    pks = mm.primaryKeys()
    fks = mm.foreignKeys()

    print('numtables: ' + str(len(tables)))
    print('numcolumns: ' + str(len(columns)))
    print('numpks: ' + str(len(pks)))
    print('numfks: ' + str(len(fks)))

