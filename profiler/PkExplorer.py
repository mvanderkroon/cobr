from __future__ import print_function

import sys, os, itertools, operator, sortedcontainers, pymssql, argparse
sys.path.append("../common")
sys.path.append("../api")

from MetaModel import MetaModel
from objects import ForeignKey, PrimaryKey, Table, Column, Base

from collections import Counter

from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker

class PkExplorer():

    def __init__(self, colseparator='|', connection_string=''):
        self.colseparator = colseparator
        self.engine = create_engine(connection_string)

    def getDataForSelectedColumns(self, columns):
        conn = self.engine.connect()

        s = select([columns])
        result = conn.execute(s)

        return result.fetchall()

    def doOneTable(self, table):
        excluded_fields = ['text', 'decimal', 'float', 'binary']
        retval = []

        try:
            data = {}
            counts = {}
            columnnames = []
            alldata = []

            columns = [ c for c in table.columns if c.type not in excluded_fields ]
            columnnames = [ c.name for c in columns ]

            alldata = self.getDataForSelectedColumns(table)

            for column in table.columns:
                idx = columnnames.index(column.name)
                data[column.name] = [ row[idx] for row in alldata ]

            if len(data) > 0:
                for columnname in columnnames:
                    values = [str(d) for d in data[columnname]]
                    c = Counter(values)
                    counts[columnname] = dict(c)

                singlekeys = itertools.combinations(columnnames, 1)
                pks = []
                useless = []
                candidateCombi = {}
                numrows = len(alldata)

                for combi in singlekeys:
                    key = combi[0]
                    dictionary = counts[key]
                    if len(dictionary.keys()) == 1:
                        useless.append(key)
                    if len(dictionary.keys()) == numrows:
                        candidateCombi[combi] = len(dictionary.keys())
                        pks.append(key)

                for key in useless:
                    columnnames.remove(key)

                duokeys = itertools.combinations(columnnames, 2)
                triplekeys = []
                if len(columnnames) <= 10:
                    triplekeys = itertools.combinations(columnnames, 3)

                for combi in itertools.chain(duokeys, triplekeys):
                    n = 1
                    overlap = [val for val in combi if val in useless]
                    overlap = overlap or [val for val in combi if val in pks]
                    if len(overlap) == 0:
                        for key in combi:
                            n = n * len(counts[key].keys())
                        if n >= numrows:
                            candidateCombi[combi] = n

                sortedCombis = sorted(candidateCombi.items(), key=operator.itemgetter(1), reverse=True)
                rankedCombis = []
                for length in range(1, len(columnnames)):
                    for combi in sortedCombis:
                        if len(combi[0]) == length:
                            rankedCombis.append(combi)

                del counts

                combiCounts = {}
                validCombis = []
                for combi in rankedCombis:
                    validCombis.append(combi[0])
                    combiCounts[combi[0]] = sortedcontainers.SortedList()

                for row in alldata:
                    for combi in validCombis:
                        values = []
                        for key in combi:
                            idx = columnnames.index(key)
                            value = row[idx]
                            values.append(str(value))
                        if len(combi) > 0:
                            if not values in combiCounts[combi]:
                                combiCounts[combi].add(values)
                            else:
                                validCombis.remove(combi)

                for validCombi in validCombis:
                    pk = PrimaryKey()
                    pk.db_catalog = table.info['db_catalog']
                    pk.db_schema = table.info['schemaname']
                    pk.tablename = table.name
                    pk.db_columns = self.colseparator.join(list(validCombi))
                    pk.keyname = 'detected PK'
                    pk.type = 'IMPLICIT'
                    pk.score = 1.0
                    retval.append(pk)
        except Exception as e:
            print('could not process: ' + table.name)
            print(e)

        return retval

    def doMultipleTables(self, tables):
        retval = []
        for table in tables:
            retval.extend(self.doOneTable(table))
        return retval

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

def main(args):
    miner = MetaModel(args.src)

    tables = miner.tables()
    columns = miner.columns()

    pkexplorer = PkExplorer(connection_string=args.src)
    pks = pkexplorer.doMultipleTables(tables)

    if not args.dry_run and args.target:
        engine = create_engine(args.target)
        Session = sessionmaker(bind=engine)
        session = Session()

        writeToDb(session, pks)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", help="connection_string for the subject-database", metavar="string")
    parser.add_argument("-t", "--target", help="connection_string for the target-database", metavar="string")
    parser.add_argument("-d", "--dry_run", help="flag to make a dry-run without storing the result to target database", action='store_true', default=False)
    args = parser.parse_args()

    main(args)