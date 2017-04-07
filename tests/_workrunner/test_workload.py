import unittest
import os

from commons import *
import ssdbox
from workrunner.workload import PatternSuite

# @unittest.skip('This does not work with pypy.')
class TestPatternSuite(unittest.TestCase):
    def test_random(self):
        conf = ssdbox.dftldes.Config()

        # for test
        conf['fs_mount_point'] = '/tmp/'

        conf['workload_class'] = 'PatternSuite'
        conf['workload_conf_key'] = 'PatternSuite'
        conf['PatternSuite'] = {'patternname': 'SRandomWrite',
                'parameters': {
                    'zone_size': 1*MB,
                    'chunk_size': 512*KB,
                    'traffic_size': 1*MB,
                    }
                }

        wl = PatternSuite(conf, workload_conf_key='PatternSuite')
        wl.run()

        self.assertEqual(os.path.exists('/tmp/datafile'), True)



if __name__ == '__main__':
    unittest.main()

