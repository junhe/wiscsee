import unittest

from sqlitebench.sqlitedb import *
from utilities.utils import *


class TestSqliteDB(unittest.TestCase):
    def test_init(self):
        bench = SqliteDB('/tmp/tmp.db')

    def test_initializing(self):
        bench = SqliteDB('/tmp/tmp.db')
        bench.open()
        bench.initialize()
        bench.close()

    def test_open_close(self):
        bench = SqliteDB('/tmp/tmp.db')
        bench.open()
        bench.close()

    def test_inserting(self):
        bench = SqliteDB('/tmp/tmp.db')
        bench.open()
        bench.initialize()
        bench.insert(key='888', value='999')
        table = bench.select_all()
        self.assertEquals(table, [('888', '999')])

        bench.close()

    def test_updating(self):
        bench = SqliteDB('/tmp/tmp.db')
        bench.open()
        bench.initialize()
        bench.insert(key='888', value='999')
        bench.update(key='888', value='777')
        table = bench.select_all()
        self.assertEquals(table, [('888', '777')])

        bench.close()


class TestSqlitebench(unittest.TestCase):
    def test_random(self):
        shcmd('python sqlitebench/bench.py -f /tmp/tmpdb -n 100 -p random -e 10')

    def test_sequential(self):
        shcmd('python sqlitebench/bench.py -f /tmp/tmpdb -n 100 -p sequential -e 10')

    def test_preload_and_random(self):
        shcmd('python sqlitebench/bench.py -f /tmp/tmpdb -n 100 -p preload_and_random -e 10')


if __name__ == '__main__':
    unittest.main()

