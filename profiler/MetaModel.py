from sqlalchemy.schema import Table
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import reflection

from contextlib import contextmanager

import argparse

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

    def __init__(self, connection_string, pool_size=20, pool_recycle=3600):
        self.__excluded_schemas__ = ['db_accessadmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_ddladmin', 'db_denydatareader', 'db_denydatawriter', 'db_owner', 'db_securityadmin', 'INFORMATION_SCHEMA', 'sys', 'information_schema', 'performance_schema']

        engine = create_engine(connection_string)
        insp = reflection.Inspector.from_engine(engine)

        self._pks = []
        self._fks = []
        self._tables = []
        self._columns = []
        self._schemanames = []
        self._db_catalog = ''

        tabledict = {}
        coldict = {}

        # 'mine' the subject database for it's metamodel
        with self.dbconnection(engine) as connection:
            self._db_catalog = connection_string[connection_string.rfind('/')+1:]
            if (engine.url.get_dialect().name == 'mysql'):
                self._schemanames = [engine.url.translate_connect_args()['database']]
            else:
                self._schemanames = insp.get_schema_names()

            for schemaname in self._schemanames:
                if schemaname in self.__excluded_schemas__:
                    continue

                metadata = MetaData(schema=schemaname)
                tablenames = insp.get_table_names(schema=schemaname)

                # getting all primary keys
                for tablename in tablenames:
                    self._pks.extend(insp.get_primary_keys(table_name=tablename, schema=schemaname))

                # getting all foreign keys
                for tablename in tablenames:
                    self._fks.extend(insp.get_foreign_keys(table_name=tablename, schema=schemaname))

                # getting all tables
                for tablename in tablenames:
                    table = Table(tablename, metadata, autoload=True, autoload_with=engine)
                    table.info['schemaname'] = schemaname
                    table.info['db_catalog'] = self.db_catalog()
                    table.info['num_explicit_inlinks'] = len(self.inlinksForTable(table));
                    table.info['num_explicit_outlinks'] = len(self.outlinksForTable(table));
                    self._tables.append(table)

                    for column in table.columns:
                        column.info['schemaname'] = schemaname
                        column.info['db_catalog'] = self.db_catalog()
                        self._columns.append(column)

    def tables(self):
        return self._tables

    def columns(self):
        return self._columns

    def primaryKeys(self):
        return self._pks

    def foreignKeys(self):
        return self._fks

    def schemas(self):
        return self._schemanames

    def db_catalog(self):
        return self._db_catalog

    def inlinksForTable(self, table):
        inlinks = []
        for efk in self.foreignKeys():
            if efk['referred_table'] == table.name and efk['referred_schema'] == table.schema:
                inlinks.append(efk)
        return inlinks

    def outlinksForTable(self, table):
        return table.foreign_keys

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
    args = parser.parse_args()

    mm = MetaModel(args.src)
    tables = mm.tables()
    columns = mm.columns()
    pks = mm.primaryKeys()
    fks = mm.foreignKeys()

    print('numtables: ' + str(len(tables)))
    print('numcolumns: ' + str(len(columns)))
    print('numpks: ' + str(len(pks)))
    print('numfks: ' + str(len(fks)))

