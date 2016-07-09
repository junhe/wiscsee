from collections import deque
import sys
from ssdbox.devblockpool import *

TDATA = 'TDATA'
TTRANS = 'TTRANS'
TGCDATA = 'TGCDATA'
TGCTRANS = 'TGCTRANS'

class OutOfSpaceError(RuntimeError):
    pass

class BlockPool(object):
    def __init__(self, confobj):
        self.conf = confobj
        self.n_channels = self.conf['flash_config']['n_channels_per_dev']
        self.cur_channel = 0
        self.stripe_size = self.conf['stripe_size']

        self.pool = MultiChannelBlockPool(
                n_channels=self.n_channels,
                n_blocks_per_channel=self.conf.n_blocks_per_channel,
                n_pages_per_block=self.conf.n_pages_per_block,
                tags=[TDATA, TTRANS, TGCDATA, TGCTRANS])

    @property
    def freeblocks(self):
        blocks = self.pool.get_blocks_of_tag(tag=TFREE)
        return blocks

    @property
    def data_usedblocks(self):
        blocks = self.pool.get_blocks_of_tag(tag=TDATA)
        return blocks

    @property
    def trans_usedblocks(self):
        blocks = self.pool.get_blocks_of_tag(tag=TTRANS)
        return blocks

    @property
    def used_blocks(self):
        blocks1 = self.pool.get_blocks_of_tag(tag=TDATA)
        blocks2 = self.pool.get_blocks_of_tag(tag=TTRANS)

        return blocks1 + blocks2

    def count_blocks(self, tag, channels=None):
        return self.pool.count_blocks(tag, channels)

    def pop_a_free_block_to_trans(self):
        try:
            blocknum = self.pool.pick_and_move(src=TFREE, dst=TTRANS)
        except TagOutOfSpaceError:
            raise OutOfSpaceError
        return blocknum

    def pop_a_free_block_to_data(self):
        try:
            blocknum = self.pool.pick_and_move(src=TFREE, dst=TDATA)
        except TagOutOfSpaceError:
            raise OutOfSpaceError
        return blocknum

    def move_used_data_block_to_free(self, blocknum):
        self.pool.change_tag(blocknum, src=TDATA, dst=TFREE)

    def move_used_trans_block_to_free(self, blocknum):
        self.pool.change_tag(blocknum, src=TTRANS, dst=TFREE)

    def move_used_trans_block_to_data(self, blocknum):
        self.pool.change_tag(blocknum, src=TTRANS, dst=TDATA)

    def next_n_data_pages_to_program_striped(self, n):
        try:
            ppns = self.pool.next_ppns(n=n, tag=TDATA, block_index=0,
                    stripe_size=self.conf['stripe_size'])
        except TagOutOfSpaceError:
            raise OutOfSpaceError
        return ppns

    def next_data_page_to_program(self):
        ppns = self.pool.next_ppns(n=1, tag=TDATA, block_index=0,
                stripe_size=1)
        return ppns[0]

    def next_translation_page_to_program(self):
        ppns = self.pool.next_ppns(n=1, tag=TTRANS, block_index=0,
                stripe_size=1)
        return ppns[0]

    def next_gc_data_page_to_program(self):
        ppns = self.pool.next_ppns(n=1, tag=TGCDATA, block_index=0,
                stripe_size=1)
        return ppns[0]

    def next_gc_translation_page_to_program(self):
        ppns = self.pool.next_ppns(n=1, tag=TGCTRANS, block_index=0,
                stripe_size=1)
        return ppns[0]

    def current_blocks(self):
        return self.pool.current_blocks()

    def used_ratio(self):
        nfree = self.pool.count_blocks(tag=TFREE)
        return (self.conf.n_blocks_per_dev - nfree) / float(self.conf.n_blocks_per_dev)

    def total_used_blocks(self):
        nfree = self.pool.count_blocks(tag=TFREE)
        return self.conf.n_blocks_per_dev - nfree

    def num_freeblocks(self):
        nfree = self.pool.count_blocks(tag=TFREE)
        return nfree


