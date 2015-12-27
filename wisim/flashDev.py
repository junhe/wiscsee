import simpy

import flashConfig

"""
This flashDev is what the FTL directly interacts with.
It has multiple channels (serial buses), each of which connects to one or more
flash packages.
"""

class FlashDevice:
    """
    This is a simplified SSD hardware device. We only consider the parallelism
    at channel level here.
    """
    def __init__(self, env, conf):
        self.env = env
        self.conf = conf

        self.n_channels_per_dev = self.conf['n_channels_per_dev']

        # time
        self.page_read_time = self.conf['page_read_time']
        self.page_prog_time = self.conf['page_prog_time']
        self.block_erase_time = self.conf['block_erase_time']

        # used to coordinate access to each channel
        self.channel_resources = [ simpy.Resource(env, capacity = 1) \
            for i in range(self.n_channels_per_dev) ]

    def get_channel_res(self, channel_off):
        return self.channel_resources[channel_off]

    def read_page(self, channel_off, page_off):
        res = self.get_channel_res(channel_off)

        request = res.request()
        yield request
        yield self.env.timeout( self.page_read_time )

        res.release(request)

        self.env.exit(None) # TODO: need to return a value

    def write_page(self, channel_off, page_off, value):
        res = self.get_channel_res(channel_off)

        request = res.request()
        yield request
        yield self.env.timeout( self.page_prog_time )

        res.release(request)

    def erase_block(self, channel_off, block_off):
        res = self.get_channel_res(channel_off)

        request = res.request()
        yield request
        yield self.env.timeout( self.block_erase_time )

        res.release(request)



