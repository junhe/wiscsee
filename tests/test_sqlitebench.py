import unittest
import os

from sqlitebench.sqlitedb import *
from utilities.utils import *
from sqlitebench.bench import Bench


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

    def test_get(self):
        bench = SqliteDB('/tmp/tmp.db')
        bench.open()
        bench.initialize()
        bench.insert(key='888', value='999')
        value = bench.get_value(key='888')
        self.assertEquals(value, '999')

        bench.close()

    def test_open_existing(self):
        os.remove('/tmp/tmp.db')
        bench = SqliteDB('/tmp/tmp.db')
        bench.open()
        bench.insert(key='888', value='999')
        bench.commit()
        value = bench.get_value(key='888')
        self.assertEquals(value, '999')
        bench.close()

        bench = SqliteDB('/tmp/tmp.db', use_existing_db=True)
        bench.open()
        value = bench.get_value(key='888')
        self.assertEquals(value, '999')
        bench.close()


class TestSqlitebench(unittest.TestCase):
    def test_random(self):
        shcmd('python sqlitebench/bench.py -f /tmp/tmpdb -n 100 -p random -e 10 -m 20')

    def test_sequential(self):
        shcmd('python sqlitebench/bench.py -f /tmp/tmpdb -n 100 -p sequential -e 10 -m 20')

    def test_preload_and_random(self):
        shcmd('python sqlitebench/bench.py -f /tmp/tmpdb -n 100 -p preload_and_random -e 10 -m 20')


class TestBenchClass(unittest.TestCase):
    def test_init(self):
        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 100,
              pattern = 'random',
              commit_period = 10,
              max_key = 8
                )

    def test_random(self):
        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 100,
              pattern = 'random',
              commit_period = 2,
              max_key = 8
                )
        b.run()

    def test_sequential(self):
        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 20,
              pattern = 'sequential',
              commit_period = 2,
              max_key = 8
                )
        b.run()

    def test_preload_and_random(self):
        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 10,
              pattern = 'preload_and_random',
              commit_period = 2,
              max_key = 8
                )
        b.run()

    def test_random_read(self):
        print 'write to it'
        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 100,
              pattern = 'random',
              commit_period = 100,
              max_key = 8
                )
        b.run()

        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 100,
              pattern = 'random_read',
              commit_period = 100,
              max_key = 8
                )
        b.run()

    def test_sequential_read(self):
        print 'write to it'
        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 100,
              pattern = 'random',
              commit_period = 100,
              max_key = 8
                )
        b.run()

        b = Bench(db_path = '/tmp/tmp.db',
              n_insertions = 100,
              pattern = 'sequential_read',
              commit_period = 100,
              max_key = 8
                )
        b.run()


if __name__ == '__main__':
    unittest.main()

