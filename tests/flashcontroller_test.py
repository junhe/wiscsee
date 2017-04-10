import config
import unittest
import simpy

import wiscsim
from utilities.utils import *
from commons import *


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
        channel = wiscsim.controller.Channel(env, self.conf)
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
        addr = wiscsim.controller.FlashAddress()
        addr.channel = 1

        channel = controller.channels[addr.channel]
        yield env.process( controller.write_page(addr) )
        self.assertEqual(env.now, channel.program_time)
        yield env.process( controller.read_page(addr) )
        self.assertEqual(env.now, channel.program_time + channel.read_time)
        yield env.process( controller.erase_block(addr) )
        self.assertEqual(env.now, channel.program_time + channel.read_time +
                channel.erase_time)

        e1 = env.process( controller.read_page(addr) )
        e2 = env.process( controller.read_page(addr) )
        yield e1 & e2
        self.assertEqual(env.now, channel.program_time + channel.read_time +
                channel.erase_time + 2 * channel.read_time)

    def my_run(self):
        env = simpy.Environment()
        controller = wiscsim.controller.Controller(env, self.conf)
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
        controller = wiscsim.controller.Controller(env, self.conf)
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
        req = wiscsim.controller.FlashRequest()
        req.addr = wiscsim.controller.FlashAddress()
        req.addr.channel = channel
        if op == 'read':
            req.operation = OP_READ
        elif op == 'write':
            req.operation = OP_WRITE
        elif op == 'erase':
            req.operation = OP_ERASE
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
        controller = wiscsim.controller.Controller(env, self.conf)
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
        addr = wiscsim.controller.FlashAddress()
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


class TestControllerRequest2(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

        # 2 pages per block, 2 blocks per channel, 2 channels in total
        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 2

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def create_request(self, channel, op):
        req = wiscsim.controller.FlashRequest()
        req.addr = wiscsim.controller.FlashAddress()
        req.addr.channel = channel
        if op == 'read':
            req.operation = OP_READ
        elif op == 'write':
            req.operation = OP_WRITE
        elif op == 'erase':
            req.operation = OP_ERASE
        else:
            raise RuntimeError()

        return req

    def access(self, env,  controller):
        channel = controller.channels[1]
        rt = channel.read_time
        wt = channel.program_time
        et = channel.erase_time

        yield env.process( controller.rw_ppn_extent(0, 2, 'read') )
        self.assertEqual( env.now, rt *2 ) # two pages go to the same channel

        yield env.process( controller.rw_ppn_extent(4, 2, 'write') )
        self.assertEqual( env.now, rt *2 + wt*2 ) # two pages go to the same channel

        yield env.process( controller.rw_ppn_extent(0, 8, 'write') )
        # 4 pages go to one channel
        self.assertEqual( env.now, rt *2 + wt*2 + wt*4 )

        # one req goes to channel 1, another one goes to channel 2
        yield env.process( controller.erase_pbn_extent(1, 2) )
        self.assertEqual( env.now, rt *2 + wt*2 + wt*4 + et )

    def my_run(self):
        env = simpy.Environment()
        controller = wiscsim.controller.Controller(env, self.conf)
        env.process(self.access(env, controller))
        env.run()

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestControllerTime(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

        # 2 pages per block, 2 blocks per channel, 2 channels in total
        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 2

        self.conf['flash_config']['t_WC'] = 1
        self.conf['flash_config']['t_R'] = 1
        self.conf['flash_config']['t_RC'] = 1
        self.conf['flash_config']['t_PROG'] = 1
        self.conf['flash_config']['t_BERS'] = 1
        self.conf['flash_config']['page_size'] = 1


    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        env = simpy.Environment()
        controller = wiscsim.controller.Controller(env, self.conf)

        self.assertEqual(controller.channels[0].read_time, 9)
        self.assertEqual(controller.channels[0].program_time, 9)
        self.assertEqual(controller.channels[0].erase_time, 6)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestControllerTag(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNewFlash()

        # 2 pages per block, 2 blocks per channel, 2 channels in total
        self.conf['flash_config']['n_pages_per_block'] = 2
        self.conf['flash_config']['n_blocks_per_plane'] = 2
        self.conf['flash_config']['n_planes_per_chip'] = 1
        self.conf['flash_config']['n_chips_per_package'] = 1
        self.conf['flash_config']['n_packages_per_channel'] = 1
        self.conf['flash_config']['n_channels_per_dev'] = 2

    def setup_environment(self):
        pass

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def create_request(self, channel, op):
        req = wiscsim.controller.FlashRequest()
        req.addr = wiscsim.controller.FlashAddress()
        req.addr.channel = channel
        if op == 'read':
            req.operation = OP_READ
        elif op == 'write':
            req.operation = OP_WRITE
        elif op == 'erase':
            req.operation = OP_ERASE
        else:
            raise RuntimeError()

        return req

    def access(self, env,  controller):
        channel = controller.channels[0]
        rt = channel.read_time
        wt = channel.program_time
        et = channel.erase_time

        yield env.process( controller.rw_ppn_extent(0, 2, 'read',
            tag = 'mytag1') )
        self.assertEqual( env.now, rt *2 ) # two pages go to the same channel
        self.assertEqual(
            controller.recorder.\
            general_accumulator['channel_busy_time']['channel_0-read-mytag1'], rt * 2)

        yield env.process( controller.rw_ppn_extent(4, 2, 'write',
            tag = 'mytag2') )
        self.assertEqual( env.now, rt *2 + wt*2 ) # two pages go to the same channel

        yield env.process( controller.rw_ppn_extent(0, 8, 'write',
            tag = 'mytag2') )
        # 4 pages go to one channel
        self.assertEqual( env.now, rt *2 + wt*2 + wt*4 )

        # one req goes to channel 1, another one goes to channel 2
        yield env.process( controller.erase_pbn_extent(1, 2, tag = 'mytag3') )
        self.assertEqual( env.now, rt *2 + wt*2 + wt*4 + et )

    def my_run(self):
        env = simpy.Environment()
        set_exp_metadata(self.conf, save_data = False,
                expname = 'default',
                subexpname = 'default-sub')
        runtime_update(self.conf)
        rec = wiscsim.recorder.Recorder(output_target = self.conf['output_target'],
            output_directory = self.conf['result_dir'],
            verbose_level = self.conf['verbose_level'],
            print_when_finished = False
            )
        rec.enable()

        controller = wiscsim.controller.Controller3(env, self.conf, rec)
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



