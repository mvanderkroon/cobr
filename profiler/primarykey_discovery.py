from __future__ import print_function

import sys
sys.path.append("../common")
sys.path.append("../api")
import metaclient
from objects import ForeignKey, PrimaryKey, Table, Column, Base

import os
import itertools
import operator
import sortedcontainers
from collections import Counter
import pymssql
import configparser
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-i", "--host", dest="db_host", help="", metavar="string")
parser.add_option("-u", "--user", dest="db_user", help="", metavar="string")
parser.add_option("-p", "--password", dest="db_password", help="", metavar="string")
parser.add_option("-c", "--catalog", dest="db_catalog", help="", metavar="string")
(options, args) = parser.parse_args()

class Discovery():
    def __init__(self, tables=[], columns=[], colseparator='|'):
        self.tables = tables
        self.columns = columns
        self.colseparator = colseparator

    def getDataFn(self, table=None, verbose=False):
        if table is None:
            return

        retval = []
        with pymssql.connect(options.db_host, options.db_user, options.db_password, options.db_catalog) as conn:
            with conn.cursor(as_dict=True) as cursor:
                query = """
                    SELECT 
                        *
                    FROM 
                        [{0}].[{1}]
                    """.format(table.db_schema, table.tablename)
                
                if verbose:
                    print(query)
                    print('')

                cursor.execute(query)
                retval = [ d for d in cursor.fetchall() ]
        return retval

    def discoverpks(self):
        excluded_fields = ['text']
        retval = []

        for table in self.tables:
            print(table)
            
            try:
                data = {}
                counts = {}
                tablecolumns = []
                # tablecoldict = {}
                columnnames = []
                alldata = []

                tablecolumns = [ i for i in self.columns if i.db_catalog == table.db_catalog and i.db_schema == table.db_schema and i.tablename == table.tablename ]
                columnnames = [ i.columnname for i in tablecolumns if i.datatype not in excluded_fields ]
                alldata = self.getDataFn(table)

                for column in tablecolumns:
                    columnname = column.columnname

                    # tablecoldict[columnname] = column
                    data[columnname] = [ i[columnname] for i in alldata ]

                if len(data) > 0:
                    for column in tablecolumns:
                        values = data[column.columnname]
                        c = Counter(values)
                        counts[column.columnname] = dict(c)
                    
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

                    for rowdict in alldata:
                        for combi in validCombis:
                            values = []
                            for key in combi:
                                value = rowdict[key]
                                values.append(str(value))
                            if len(combi) > 0:
                                if not values in combiCounts[combi]:
                                    combiCounts[combi].add(values)
                                else:
                                    validCombis.remove(combi)

                    for validCombi in validCombis:
                        pk = PrimaryKey(db_catalog=table.db_catalog, db_schema=table.db_schema, tablename=table.tablename, db_columns=list(validCombi), keyname='implicit primary key', type='implicit')
                        retval.append(pk)
            except Exception as e:
                print('could not process: ' + str(table))
                print(e)

        return retval

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    if len(config.sections()) == 0:
        print('config.ini file not yet present, please copy from template (templace_config.ini) and fill in required properties')
        quit()

    reader = metaclient.reader(config['metadb']['connection_string'])

    # filter syntax, see sqlalchemy documentation
    tables = reader.getTables(filter=Table.db_catalog==options.db_catalog)
    columns = reader.getColumns(filter=Column.db_catalog==options.db_catalog)

    disc = Discovery(tables=tables, columns=columns)
    pks = disc.discoverpks()

    writer = metaclient.writer(config['metadb']['connection_string'])
    writer.writeForeignKeys(pks)
    writer.close()

if __name__ == '__main__':
    main()