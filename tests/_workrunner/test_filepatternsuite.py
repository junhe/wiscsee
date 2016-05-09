import unittest
import os
import itertools

from workrunner import filepatternsuite
from workrunner.filepatternsuite import File
from accpatterns import patterns
from accpatterns.patterns import READ, WRITE, DISCARD

class TestAccessFile(unittest.TestCase):
    def test(self):
        filepath = "/tmp/testfile"
        req_iter = patterns.Random(op=WRITE, zone_offset=0, zone_size=2,
                chunk_size=2, traffic_size=2)

        if os.path.exists(filepath):
            os.remove(filepath)

        f = File(filepath)
        f.open()
        f.access(req_iter)
        f.close()

        fsize = os.path.getsize(filepath)
        self.assertEqual(fsize, 2)

    def test_discard(self):
        filepath = "/tmp/testfile"
        req_iter_w = patterns.Random(op=WRITE, zone_offset=0, zone_size=2,
                chunk_size=2, traffic_size=2)
        req_iter_d = patterns.Random(op=DISCARD, zone_offset=0, zone_size=2,
                chunk_size=2, traffic_size=2)

        if os.path.exists(filepath):
            os.remove(filepath)

        f = File(filepath)
        f.open()
        f.access(req_iter_w)
        f.access(req_iter_d)
        f.close()

        fsize = os.path.getsize(filepath)
        self.assertEqual(fsize, 2)

        with open(filepath, 'r') as f:
            a = f.read(1)
            self.assertEqual(a, '\x00')

if __name__ == '__main__':
    unittest.main()
