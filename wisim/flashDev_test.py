import unittest
import cProfile

import simpy

from commons import *
import flashDev
import flashConfig

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

class DevTest(unittest.TestCase):
    def test_loopback_time(self):
        env = simpy.Environment()
        helper = HelperDevLoopBackTime()

        env.process( helper.loopback(env,
                     flashDev.FlashDevice(env = env,
                         conf = flashConfig.flash_config)
            ) )
        env.run()

        self.assertTrue( helper.assert_true() )

if __name__ == '__main__':
    unittest.main()


