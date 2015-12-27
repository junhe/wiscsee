import unittest
import flashPackage

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

        page = flashPackage.Page(env = env, conf = flashConfig.flash_config)

        rwtester = PageReadWriteTester()
        env.process(rwtester.accessor_proc(env, page))
        env.run()
        self.assertTrue( rwtester.equal() )


class OOBATest(unittest.TestCase):
    def test_set_get(self):
        env = simpy.Environment()

        page = flashPackage.Page(env = env, conf = flashConfig.flash_config)

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


class BlockTestHelper5(object):
    def __init__(self):
        self.ret = []
        self.endtime = None
        self.block = None

    def loopback(self, env, block):
        """
        append and read
        """
        self.block = block

        yield env.process( block.append(value = 111) )
        ret = yield env.process( block.read_page(page_offset = 0) )

        self.endtime = env.now

    def assert_true(self):
        block = self.block

        correct_time = block.conf['page_read_time'] + \
            block.conf['page_prog_time']
        print('correct_time', correct_time)

        return correct_time == self.endtime


class BlockTest(unittest.TestCase):
    def test_offsets(self):
        env = simpy.Environment()

        block = flashPackage.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper2()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )

    def test_appends(self):
        env = simpy.Environment()

        block = flashPackage.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper3()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )

    def test_erase(self):
        env = simpy.Environment()

        block = flashPackage.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper4()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )

    def test_time(self):
        env = simpy.Environment()

        block = flashPackage.Block(env = env, conf = flashConfig.flash_config)
        helper = BlockTestHelper5()

        env.process( helper.loopback(env, block) )
        env.run()

        self.assertTrue( helper.assert_true() )


class HelperPlaneLoopBack(object):
    def loopback(self, env, plane):
        nblocks = plane.conf['n_blocks_per_plane']
        npages = plane.conf['n_pages_per_block']
        blocks = [0, 3 % nblocks]
        pages = [i for i in range(npages)]

        self.write_values = []
        for block_off in blocks:
            for page_off in pages:
                value = page_off * 100
                yield env.process(
                        plane.write_page(block_off, page_off, value))
                self.write_values.append(value)

        self.read_values = []
        for block_off in blocks:
            for page_off in pages:
                value = page_off * 100
                ret = yield env.process(
                        plane.read_page(block_off, page_off))
                self.read_values.append(ret)

    def assert_true(self):
        return self.write_values == self.read_values


class PlaneTest(unittest.TestCase):
    def test_loopback(self):
        env = simpy.Environment()

        plane = flashPackage.Plane(env = env, conf = flashConfig.flash_config)
        helper = HelperPlaneLoopBack()

        env.process( helper.loopback(env, plane) )
        env.run()

        self.assertTrue( helper.assert_true() )


class HelperChipLoopBack(object):
    def loopback(self, env, chip):
        nplanes = chip.conf['n_planes_per_chip']
        nblocks = chip.conf['n_blocks_per_plane']
        npages = chip.conf['n_pages_per_block']

        planes = [0, 1 % nplanes]
        blocks = [0, 3 % nblocks]
        pages = [i for i in range(npages)]

        self.write_values = []
        for plane_off in planes:
            for block_off in blocks:
                for page_off in pages:
                    value = page_off * 100
                    yield env.process(
                        chip.write_page(plane_off, block_off, page_off, value))
                    self.write_values.append(value)

        self.read_values = []
        for plane_off in planes:
            for block_off in blocks:
                for page_off in pages:
                    value = page_off * 100
                    ret = yield env.process(
                            chip.read_page(plane_off, block_off, page_off))
                    self.read_values.append(ret)

    def assert_true(self):
        return self.write_values == self.read_values


class ChipTest(unittest.TestCase):
    def test_loopback(self):
        env = simpy.Environment()

        chip = flashPackage.Chip(env = env, conf = flashConfig.flash_config)
        helper = HelperChipLoopBack()

        env.process( helper.loopback(env, chip) )
        env.run()

        self.assertTrue( helper.assert_true() )


class HelperPackageLoopBack(object):
    def loopback(self, env, package):
        nchips = package.conf['n_chips_per_package']
        nplanes = package.conf['n_planes_per_chip']
        nblocks = package.conf['n_blocks_per_plane']
        npages = package.conf['n_pages_per_block']

        chips = [0, 1]
        planes = [0, 1 % nplanes]
        blocks = [0, 3 % nblocks]

        # chips = [x for x in range(nchips)]
        # planes = [x for x in range(nplanes)]
        # blocks = [x for x in range(nblocks)]
        pages = [i for i in range(npages)]

        self.write_values = []
        for chip_off in chips:
            for plane_off in planes:
                for block_off in blocks:
                    for page_off in pages:
                        value = page_off * 100
                        yield env.process(
                            package.write_page(chip_off, plane_off,
                                block_off, page_off, value))
                        self.write_values.append(value)

        self.read_values = []
        for chip_off in chips:
            for plane_off in planes:
                for block_off in blocks:
                    for page_off in pages:
                        value = page_off * 100
                        ret = yield env.process(
                                package.read_page(chip_off, plane_off,
                                    block_off, page_off))
                        self.read_values.append(ret)

    def assert_true(self):
        return self.write_values == self.read_values


class PackageTest(unittest.TestCase):
    def test_loopback(self):
        env = simpy.Environment()

        package = \
                flashPackage.Package(env = env, conf = flashConfig.flash_config)
        helper = HelperPackageLoopBack()

        env.process( helper.loopback(env, package) )
        env.run()

        self.assertTrue( helper.assert_true() )


if __name__ == '__main__':
    unittest.main()


