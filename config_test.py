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

    def test_sec_translation(self):
        conf = config.Config()

        pagesize = conf['flash_page_size']
        secsize = conf['sector_size']
        page, cnt = conf.sec_ext_to_page_ext(pagesize*3/secsize,
                pagesize*2/secsize)

        self.assertEqual(page, 3)
        self.assertEqual(cnt, 2)

    def test_offset_size_translation(self):
        conf = config.Config()

        secsize = conf['sector_size']
        sec, count = conf.off_size_to_sec_count(
                offset = secsize * 10,
                size = secsize * 31)

        self.assertEqual(sec, 10)
        self.assertEqual(count, 31)

class TestConfigNewFlash(unittest.TestCase):
    def test_npages(self):
        conf = config.ConfigNewFlash()

        npages1 = conf.total_num_pages()
        npages2 = conf['flash_config']['n_pages_per_block'] \
            * conf.n_blocks_per_dev
        print 'npages1', npages1
        print 'npages2', npages2
        self.assertEqual(npages1, npages2)


if __name__ == '__main__':
    unittest.main()

