import config
import unittest
import simpy

import flashcontroller


class TestTemplate(unittest.TestCase):
    def setup_config(self):
        self.conf = config.Config()

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        pass

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


class TestControllerTranslation(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()
        self.conf.n_channels_per_dev = 3

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        env = simpy.Environment()
        controller = flashcontroller.controller.Controller(env, self.conf)
        addr = controller.physical_to_machine_page(0)
        for n in addr.location:
            self.assertEqual(n, 0)

        addr = controller.physical_to_machine_page(1)
        if self.conf['flash_config']['n_pages_per_block'] > 1:
            self.assertEqual(addr.page, 1)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


class TestControllerRequest(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()
        self.conf.n_channels_per_dev = 3

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def create_request(self, channel, op):
        req = flashcontroller.controller.FlashRequest()
        req.addr = flashcontroller.controller.FlashAddress()
        req.addr.channel = channel
        if op == 'read':
            req.operation = flashcontroller.controller.FlashRequest.OP_READ
        elif op == 'write':
            req.operation = flashcontroller.controller.FlashRequest.OP_WRITE
        elif op == 'erase':
            req.operation = flashcontroller.controller.FlashRequest.OP_ERASE
        else:
            raise RuntimeError()

        return req

    def access(self, env,  controller):

        channel = controller.channels[1]
        yield env.process( controller.execute_request(
            self.create_request(1, 'read') ) )
        self.assertEqual( env.now, channel.read_time )

        yield env.process( controller.execute_request(
            self.create_request(1, 'read') ) )
        self.assertEqual( env.now, channel.read_time * 2 )

        yield env.process( controller.execute_request(
            self.create_request(1, 'write') ) )
        self.assertEqual( env.now, channel.read_time * 2 + channel.program_time)

        yield env.process( controller.execute_request(
            self.create_request(1, 'erase') ) )
        self.assertEqual( env.now, channel.read_time * 2
                + channel.program_time + channel.erase_time)

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


class TestFlashAddress(unittest.TestCase):
    def setup_config(self):
        self.conf = config.Config()

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        addr = flashcontroller.controller.FlashAddress()
        addr.page = 3
        addr.channel = 5
        self.assertEqual(addr.page, 3)
        self.assertEqual(addr.channel, 5)

    def test_main(self):
        "Remove prefix _"
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


def main():
    unittest.main()

if __name__ == '__main__':
    main()



