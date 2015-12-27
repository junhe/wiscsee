import unittest

import simpy

from commons import *
import flashConfig
import flashPackage2

class HelperPackageLoopBack(object):
    def loopback(self, env, package):
        pages = [0, 1 % package.n_pages_per_package]
        self.written_values = []
        for page_off in pages:
            value = page_off * 10
            yield env.process( package.write_page(page_off, value) )
            self.written_values.append(value)

        self.read_values = []
        for page_off in pages:
            ret = yield env.process( package.read_page(page_off) )
            self.read_values.append(ret)

    def assert_true(self):
        return self.written_values == self.read_values

class HelperPackageLoopBackTime(object):
    def loopback(self, env, package):
        pages = [0, 1 % package.n_pages_per_package]
        self.written_values = []
        for page_off in pages:
            value = page_off * 10
            yield env.process( package.write_page(page_off, value) )
            self.written_values.append(value)

        assert env.now == package.conf['page_prog_time'] * 2

        self.read_values = []
        for page_off in pages:
            ret = yield env.process( package.read_page(page_off) )
            self.read_values.append(ret)

        assert env.now == package.conf['page_prog_time'] * 2 + \
                package.conf['page_read_time'] * 2

    def assert_true(self):
        return self.written_values == self.read_values

class PackageTest(unittest.TestCase):
    def test_loopback(self):
        env = simpy.Environment()

        package = \
                flashPackage2.Package(env = env, conf = flashConfig.flash_config)
        helper = HelperPackageLoopBack()

        env.process( helper.loopback(env, package) )
        env.run()

        self.assertTrue( helper.assert_true() )

    def test_loopback_time(self):
        env = simpy.Environment()

        package = \
                flashPackage2.Package(env = env, conf = flashConfig.flash_config)
        helper = HelperPackageLoopBackTime()

        env.process( helper.loopback(env, package) )
        env.run()

        self.assertTrue( helper.assert_true() )



if __name__ == '__main__':
    unittest.main()

