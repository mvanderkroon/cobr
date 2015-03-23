from sqlalchemy.schema import Table
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import reflection

import argparse, datetime


class MetaModel():
    def __get_db_catalogname(self, connection_string=None):
        return connection_string[connection_string.rfind('/')+1:]

    def __get_schemanames(self, inspector=None):
        if inspector is None:
            inspector = reflection.Inspector.from_engine(self.engine)  # we do this to avoid having to 'refresh'

        schemanames = []

        if (self.engine.url.get_dialect().name == 'mysql'):
            schemanames = [self.engine.url.translate_connect_args()['database']]
        else:
            schemanames = inspector.get_schema_names()

        return schemanames

    def pk_for_tablename(self, tablename=None, schemaname=None, inspector=None):
        if inspector is None:
            inspector = reflection.Inspector.from_engine(self.engine)  # we do this to avoid having to 'refresh'

        pk = inspector.get_pk_constraint(table_name=tablename, schema=schemaname)

        pk['tablename'] = tablename
        pk['schemaname'] = schemaname
        pk['db_catalog'] = self.db_catalog()

        return pk

    def fk_for_tablename(self, tablename=None, schemaname=None, inspector=None):
        if inspector is None:
            inspector = reflection.Inspector.from_engine(self.engine)  # we do this to avoid having to 'refresh'

        fks = inspector.get_foreign_keys(table_name=tablename, schema=schemaname)
        for fk in fks:
            fk['srctable'] = tablename
            fk['srcschema'] = schemaname
            fk['db_catalog'] = self.db_catalog()

        return fks

    def table_for_tablename(self, tablename=None, schemaname=None, inspector=None):
        if inspector is None:
            inspector = reflection.Inspector.from_engine(self.engine)  # we do this to avoid having to 'refresh'

        table = Table(tablename, MetaData(schema=schemaname), autoload=True, autoload_with=self.engine)
        table.info['schemaname'] = schemaname
        table.info['db_catalog'] = self.db_catalog()
        table.info['num_explicit_inlinks'] = len(self.inlinksForTable(table))
        table.info['num_explicit_outlinks'] = len(self.outlinksForTable(table))

        return table

    def __init__(self, connection_string, pool_size=20, pool_recycle=3600):
        __excluded_schemas = ['db_accessadmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_ddladmin', 'db_denydatareader', 'db_denydatawriter', 'db_owner', 'db_securityadmin', 'INFORMATION_SCHEMA', 'sys', 'information_schema', 'performance_schema']

        self.engine = create_engine(connection_string)
        insp = reflection.Inspector.from_engine(self.engine)

        self._pks = []
        self._fks = []
        self._tables = []
        self._columns = []
        self._schemanames = self.__get_schemanames(insp)
        self._db_catalog = self.__get_db_catalogname(connection_string)

        # 'mine' the subject database for it's metamodel
        for schemaname in self._schemanames:
            if schemaname in __excluded_schemas:
                continue

            tablenames = insp.get_table_names(schema=schemaname)

            # getting all primary keys
            for tablename in tablenames:
                self._pks.append(self.pk_for_tablename(tablename=tablename,
                                                    schemaname=schemaname,
                                                    inspector=insp))

            # getting all foreign keys
            for tablename in tablenames:
                self._fks.extend(self.fk_for_tablename(tablename=tablename,
                                                    schemaname=schemaname,
                                                    inspector=insp))

            # getting all tables
            for tablename in tablenames:
                table = self.table_for_tablename(tablename=tablename,
                                                schemaname=schemaname,
                                                inspector=insp)
                self._tables.append(table)

                self._columns.extend(table.columns)

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
        return [c for c in self.foreignKeys() if c['referred_table'] == table.name and c['referred_schema'] == table.schema]

    def outlinksForTable(self, table):
        return table.foreign_keys

if __name__ == "__main__":
    sts = datetime.datetime.now()

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

    print('db_catalog: ' + str(mm.db_catalog()))
    print('schemas: ' + str(mm.schemas()))

    print('time elapsed: ' + str(datetime.datetime.now() - sts))
