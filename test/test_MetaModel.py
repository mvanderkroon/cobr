import sys
sys.path.append("../profiler")

import unittest

import sqlite3

class TestMetaModel(unittest.TestCase):
    def setUp(self):
        conn = sqlite3.connect(':memory:')
        c = conn.cursor()

        # Create table
        c.execute('''CREATE TABLE stocks
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, date text, trans text, symbol text, qty real, price real)''')

        # Insert a row of data
        c.execute("INSERT INTO stocks VALUES (NULL, '2006-01-05','BUY','RHAT',100,35.14)")
        c.execute("INSERT INTO stocks VALUES (NULL, '2006-01-05','BUY','RHAT',100,35.14)")
        c.execute("INSERT INTO stocks VALUES (NULL, '2006-01-05','BUY','RHAT',100,35.14)")
        c.execute("INSERT INTO stocks VALUES (NULL, '2006-01-05','BUY','RHAT',100,35.14)")
        c.execute("INSERT INTO stocks VALUES (NULL, '2006-01-05','BUY','RHAT',100,35.14)")

        # Save (commit) the changes
        conn.commit()

        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        conn.close()

    def cleanUp(self):
        conn = sqlite3.connect(':memory:')
        c = conn.cursor()
        c.execute('''DROP TABLE stocks''')

    def test(self):
        self.setUp()
        self.assertEqual(4, 4)
        self.cleanUp()

if __name__ == '__main__':
    unittest.main()