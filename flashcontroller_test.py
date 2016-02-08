import config
import unittest
import simpy

import flashcontroller

class TestTemplate(unittest.TestCase):
    def setup_config(self):
        self.conf = config.Config()

    def setup_environment(self):
        raise NotImplementedError

    def setup_workload(self):
        raise NotImplementedError

    def setup_ftl(self):
        raise NotImplementedError

    def my_run(self):
        raise NotImplementedError

    def _test_main(self):
        "Remove prefix _"
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestChannel(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def access(self, env,  channel):
        yield env.process( channel.write_page(None) )
        self.assertEqual(env.now, channel.program_time)
        yield env.process( channel.read_page(None) )
        self.assertEqual(env.now, channel.program_time + channel.read_time)
        yield env.process( channel.erase_block(None) )
        self.assertEqual(env.now, channel.program_time + channel.read_time +
                channel.erase_time)

    def my_run(self):
        env = simpy.Environment()
        channel = flashcontroller.controller.Channel(env, self.conf)
        env.process(self.access(env, channel))
        env.run()

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestController(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()
        self.conf.n_channels_per_dev = 3

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def access(self, env,  controller):
        addr = flashcontroller.controller.FlashAddress()
        addr.channel = 1

        channel = controller.channels[addr.channel]
        yield env.process( controller.write_page(addr) )
        self.assertEqual(env.now, channel.program_time)
        yield env.process( controller.read_page(addr) )
        self.assertEqual(env.now, channel.program_time + channel.read_time)
        yield env.process( controller.erase_block(addr) )
        self.assertEqual(env.now, channel.program_time + channel.read_time +
                channel.erase_time)

    def my_run(self):
        env = simpy.Environment()
        controller = flashcontroller.controller.Controller(env, self.conf)
        env.process(self.access(env, controller))
        env.run()

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


def main():
    unittest.main()

if __name__ == '__main__':
    main()



