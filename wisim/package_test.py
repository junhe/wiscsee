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

class BlockTestHelper2(object):
    def __init__(self):
        self.ret = []

    def loopback(self, env, block):
        offsets = range(block.npages)
        self.values = [off * 10 for off in offsets]
        for i, off in enumerate(offsets):
            yield env.process( block.write_page(page_offset = off,
                value = self.values[i]) )
            ret = yield env.process( block.read_page(page_offset = off) )

            self.ret.append(ret)

    def assert_true(self):
        return self.ret == self.values

class BlockTestHelper3(object):
    def __init__(self):
        self.ret = []

    def loopback(self, env, block):
        self.values = [off * 10 for off in range(block.npages)]
        for off, value in enumerate(self.values):
            yield env.process( block.append(value = value) )
            ret = yield env.process( block.read_page(page_offset = off) )

            self.ret.append(ret)

    def assert_true(self):
        return self.ret == self.values

class BlockTestHelper4(object):
    def __init__(self):
        self.ret = []

    def loopback(self, env, block):
        self.values = [off * 10 for off in range(block.npages)]
        for off, value in enumerate(self.values):
            yield env.process( block.append(value = value) )
            ret = yield env.process( block.read_page(page_offset = off) )
            self.ret.append(ret)

        yield env.process(block.erase_block())

        self.values = [off * 10 for off in range(block.npages)]
        for off, value in enumerate(self.values):
            yield env.process( block.append(value = value) )
            ret = yield env.process( block.read_page(page_offset = off) )
            self.ret.append(ret)

    def assert_true(self):
        return self.ret == (self.values * 2)


class BlockTest(unittest.TestCase):
    def test_offsets(self):
        env = simpy.Environment()

        block = package.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper2()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )

    def test_appends(self):
        env = simpy.Environment()

        block = package.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper3()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )

    def test_erase(self):
        env = simpy.Environment()

        block = package.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper4()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )

if __name__ == '__main__':
    unittest.main()


