import unittest
import cProfile

import simpy

from commons import *
import flashDev
import config

class HelperDevLoopBackTime(object):
    def loopback(self, env, flashdev):
        channels = [0, 1 % flashdev.n_channels_per_dev]
        pages = [0, 1 % flashdev.conf['n_pages_per_package']]

        for channel_off in channels:
            for page_off in pages:
                yield env.process(
                        flashdev.write_page(channel_off, page_off, None) )

        assert env.now == flashdev.page_prog_time * len(channels) * len(pages)
        write_finish = env.now

        self.read_values = []
        for channel_off in channels:
            for page_off in pages:
                ret = yield env.process( flashdev.read_page(channel_off,
                    page_off) )
                self.read_values.append(ret)

        assert env.now == write_finish + flashdev.page_read_time * \
                len(channels) * len(pages)

    def assert_true(self):
        return True

class Helper_CompeteChannel(object):
    def loopback(self, env, flashdev):
        assert flashdev.conf['n_channels_per_dev'] > 1

        # write to channel 0
        env.process(
            flashdev.write_page(channel_off = 0, page_off = 0, value = None))

        # write to channel 1
        env.process(
            flashdev.write_page(channel_off = 1, page_off = 0, value = None))

        assert env.now == 0

        # write to channel 0
        yield env.process(
            flashdev.write_page(channel_off = 0, page_off = 0, value = None))

        assert env.now == flashdev.page_prog_time * 2

        # read from channel 0
        yield env.process( flashdev.read_page(channel_off = 0, page_off = 0) )

        assert env.now == flashdev.page_prog_time * 2 + flashdev.page_read_time

    def assert_true(self):
        return True

class DevTest(unittest.TestCase):
    def test_loopback_time(self):
        env = simpy.Environment()
        helper = HelperDevLoopBackTime()

        env.process( helper.loopback(env,
                     flashDev.DevChannelParallelOnly(env = env,
                         conf = config.Config()['flash_config'])
            ) )
        env.run()

        self.assertTrue( helper.assert_true() )

    def test_compete(self):
        env = simpy.Environment()
        helper = Helper_CompeteChannel()

        env.process( helper.loopback(env,
                     flashDev.DevChannelParallelOnly(env = env,
                         conf = config.Config()['flash_config'])
            ) )
        env.run()

        self.assertTrue( helper.assert_true() )

if __name__ == '__main__':
    unittest.main()


