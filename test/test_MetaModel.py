import sys
sys.path.append("../profiler")

import unittest
import sqlite3

from MetaModel import MetaModel

conn = sqlite3.connect(':memory:')
c = conn.cursor()

class TestMetaModel(unittest.TestCase):
    def setUp(self):
        # Create table
        c.execute('''CREATE TABLE stocks
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, date text,\
                      trans text, symbol text, qty real, price real)''')

        for i in range(0, 100):
            c.execute("INSERT INTO stocks VALUES (NULL, '2006-01-05','BUY',\
                'RHAT',100,35.14)")

        # Save (commit) the changes
        conn.commit()

    def cleanUp(self):
        c.execute('''DROP TABLE stocks''')
        conn.close()

    def test(self):
        mm = MetaModel('sqlite://')
        print(mm.columns())


def main():
    unittest.main()

if __name__ == '__main__':
    main()
