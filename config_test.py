import unittest

import config

class TestConfig(unittest.TestCase):
    def test_basic(self):
        conf = config.Config({"para1": "value1"})

        self.assertDictEqual({"para1": "value1"}, conf)

    def test_default(self):
        conf = config.Config()

        self.assertIn('workload_class', conf)
        self.assertIn('expname', conf)

if __name__ == '__main__':
    unittest.main()

