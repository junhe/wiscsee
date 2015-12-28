import simpy

"""
This flashDev is what the FTL directly interacts with.
It has multiple channels (serial buses), each of which connects to one or more
flash packages.
"""

class FlashDeviceAbs(object):
    def __init__(self, env, conf):
        raise NotImplementedError

    def read_page(self, channel_off, page_off):
        raise NotImplementedError

    def write_page(self, channel_off, page_off, value):
        raise NotImplementedError

    def erase_block(self, channel_off, block_off):
        raise NotImplementedError

    def read_page_simple(self, page_num):
        raise NotImplementedError

    def write_page_simple(self, page_num, value):
        raise NotImplementedError

    def erase_block_simple(self, block_num):
        raise NotImplementedError


class DevChannelParallelOnly(FlashDeviceAbs):
    """
    This is a simplified SSD hardware device. We only consider the parallelism
    at channel level here.

    Requirements:
    1. The FTL should be able to write to two or more channels at the same
    time. This means that writing channel#2 does not need to wait until
    writing to channel#1 to finish. You cannot do:
        yield env.process( dev.page_write(channel#1, pagenum, None) )
        yield env.process( dev.page_write(channel#2, pagenum, None) )
    because this enforces that writing to channel2 starts after writing to
    channel1. How about
        env.process( dev.page_write(channel#1, pagenum, None) )
        env.process( dev.page_write(channel#2, pagenum, None) )
    this will start two processes at the same time.
        env.process( dev.page_write(channel#1, pagenum, None) )
        env.process( dev.page_write(channel#2, pagenum, None) )
        env.process( dev.page_write(channel#1, pagenum, None) )
    the third write above will wait until the first process finishes.

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
        """
        page_off = 0 is the first page in channel_off
        """
        res = self.get_channel_res(channel_off)

        request = res.request()
        yield request
        yield self.env.timeout( self.page_read_time )

        res.release(request)

        self.env.exit(None) # TODO: need to return a value

    def write_page(self, channel_off, page_off, value):
        """
        page_off = 0 is the first page in channel_off
        """
        res = self.get_channel_res(channel_off)

        request = res.request()
        yield request
        yield self.env.timeout( self.page_prog_time )

        res.release(request)

    def erase_block(self, channel_off, block_off):
        """
        block_off = 0 is the first block in channel_off
        """
        res = self.get_channel_res(channel_off)

        request = res.request()
        yield request
        yield self.env.timeout( self.block_erase_time )

        res.release(request)

    def read_page_simple(self, page_num):
        """
        This function provides a simple interface so the user does not
        need to specify channel_off. This function has a mapping from
        page_num to (channel_off, page_off) that may need to be changed.
        """
        pass

    def write_page_simple(self, page_num, value):
        pass

    def erase_block_simple(self, block_num):
        pass


