import simpy

import ftlbuilder
import devsim

class DmftlDES(ftlbuilder.FtlBuilder):
    """
    It implements a dumb FTL, to demonstrate DES
    """
    def __init__(self, confobj, recorderobj, flashobj, simpy_env):
        super(DmftlDES, self).__init__(confobj, recorderobj, flashobj)

        self.env = simpy_env

        self.n_channels_per_dev = \
                self.conf['flash_config']['n_channels_per_dev']
        self.n_pages_per_block = \
                self.conf['flash_config']['n_pages_per_block']

        # stripe setting
        self.stripe_unit = 4 # pages
        self.stripe_size = self.n_channels_per_dev * self.stripe_unit

        self.flash = devsim.flashDev.DevChannelParallelOnly(
            self.env, self.conf)

    def sec_read(self, sector, count):
        page, pg_cnt = self.conf.sec_ext_to_page_ext(sector, count)

        ret = yield self.env.process(
                self.read_pages( range(page, page + pg_cnt) ))

        self.env.exit(ret)

    def sec_write(self, sector, count, data):
        """
        For writing, you need to read block, modify, write.
        """
        yield self.env.timeout(3)

        # page, pg_cnt = self.conf.sec_ext_to_page_ext(sector, count)

        # for pg in range(page, page + pg_cnt):
            # channel, block = self.page_to_channel_block(pg)
            # pass

    def sec_discard(self, sector, count):
        yield self.env.timeout(3)

    def page_to_channel_page(self, page):
        """
        It returns a channel number and a page number.
        This is where we implement a page mapping strategy.
        This is for both reading and writing because this is
        direct mapping FTL.

        channel 0 1 2 .. n-1
        paegs   k k k .. k

        k is the stripe unit size. n*k is the stripe size.
        """
        page_off = page % self.stripe_unit
        channel = (page / self.stripe_unit) % self.n_channels_per_dev

        return channel, page_off

    def page_to_channel_block(self, page):
        """
        This function finds the channel no. and block no. of a page.
        """
        channel = (page / self.stripe_unit) % self.n_channels_per_dev
        page_off = page % self.stripe_unit
        block = page_off / self.n_pages_per_block

        return channel, block

    def read_pages(self, page_list):
        read_procs = []
        for pg in page_list:
            channel, page_off = self.page_to_channel_page(pg)
            e = self.env.process(
                    self.flash.read_page(channel, page_off))
            read_procs.append( e )

        # wait for them to finish
        data = []
        yield simpy.events.AllOf(self.env, read_procs)

        self.env.exit(data)

    def read_block(self, channel, blocknum):
        start, end = self.conf.block_to_page_range(blocknum)
        for pg in range(start, end):
            self.flash.read_page(channel, pg)

    def post_processing(self):
        pass

    def get_ftl_name(self):
        return "Dmftl-DES"

