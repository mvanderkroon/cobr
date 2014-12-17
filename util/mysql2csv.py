import pymysql
import os
import csv

database = ''
conn = pymysql.connect(host='localhost', port=3306, user='', passwd='', db=database)
cursor = conn.cursor()


cursor.execute(""" SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{0}' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME ASC """.format(database))
tables = cursor.fetchall()

for table in tables:
    print('{0}.{1}.{2}'.format(database, table[0], table[1]))

    with open(os.path.join('csv', '{0}.{1}.{2}.csv'.format(database, table[0], table[1])), 'w', newline='', encoding='utf-8') as fp:
        a = csv.writer(fp,delimiter = ",", quoting=csv.QUOTE_ALL)
        
        # get columns for table
        cursor.execute(""" SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}' ORDER BY ORDINAL_POSITION ASC """.format(table[0], table[1]))
        columns = cursor.fetchall()

        # header
        cols = []
        for column in columns:
            cols.append(column[0])
        a.writerow(cols)

        # columndefinitie voor totaalquery
        coldef = ''
        for column in columns:
            coldef +=  "`" + column[0] + "`, "
        coldef = coldef[0:-2]

        # get table contents
        cursor.execute(""" SELECT {0} FROM {1}.{2} """.format(coldef, table[0], table[1]))
        row = cursor.fetchone()
        while row:
            a.writerow(row)
            row = cursor.fetchone()

conn.close()