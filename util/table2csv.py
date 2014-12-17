import pymssql
import os
import csv

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-c", "--db_catalog", dest="db_catalog", help="", metavar="string")
parser.add_option("-s", "--db_schema", dest="db_schema", help="", metavar="string")
parser.add_option("-o", "--objectname", dest="objectname", help="", metavar="string")
(options, args) = parser.parse_args()

server = ""
user = ""
password = ""

def main():
    db_catalog = options.db_catalog
    db_schema = options.db_schema
    objectname = options.objectname

    conn = pymssql.connect(server, user, password, db_catalog)
    cursor = conn.cursor()

    with open(os.path.join('data', '{0}.{1}.{2}.csv'.format(db_catalog, db_schema, objectname)), 'w', newline='', encoding='utf-8') as fp:
        a = csv.writer(fp,delimiter = ",", quoting=csv.QUOTE_ALL)
        
        try:
            # get table contents
            cursor.execute(""" SELECT * FROM {0}.{1}.{2} """.format(db_catalog, db_schema, objectname))
            row = cursor.fetchone()
            while row:
                a.writerow(row)
                row = cursor.fetchone()
        except Error as e:
            print(e)
        finally:
            conn.close()

if __name__ == '__main__':
    main()