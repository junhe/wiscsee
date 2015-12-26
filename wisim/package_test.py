import unittest
import package

import simpy
from commons import *

import flashConfig

class PageReadWriteTester(object):
    def __init__(self):
        self.values = [3, 4, 8]
        self.ret = None

    def accessor_proc(self, env, page):
        """
        write and read page in serial
        """
        rets = []
        for i in self.values:
            yield env.process(page.write(i))
            ret = yield env.process(page.read())
            # erase page for testing
            page.set_state(PAGE_ERASED)
            rets.append(ret)

        self.ret = rets

    def equal(self):
        return self.values == self.ret


class PageTest(unittest.TestCase):
    def test_write_and_read(self):
        env = simpy.Environment()

        page = package.Page(env = env, conf = flashConfig.flash_config)

        rwtester = PageReadWriteTester()
        env.process(rwtester.accessor_proc(env, page))
        env.run()
        self.assertTrue( rwtester.equal() )

class OOBATest(unittest.TestCase):
    def test_set_get(self):
        env = simpy.Environment()

        page = package.Page(env = env, conf = flashConfig.flash_config)

        page.set_state(ERASED)

        self.assertTrue( page.get_state() == ERASED )


class BlockTestHelper(object):
    def __init__(self):
        self.ret = []
        self.values = [0, 1]

    def loopback(self, env, block):
        for value in self.values:
            yield env.process( block.write_page(page_offset = 0, value = value) )
            ret = yield env.process( block.read_page(page_offset = 0) )
            yield env.process( block.erase_block() )

            self.ret.append(ret)

    def assert_true(self):
        return self.ret == self.values

class BlockTest(unittest.TestCase):
    def test_block(self):
        env = simpy.Environment()

        block = package.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )


if __name__ == '__main__':
    unittest.main()


