from collections import deque
import sys
from wiscsim.devblockpool import *
from ftlsim_commons import random_channel_id

TDATA = 'TDATA'
TTRANS = 'TTRANS'

class OutOfSpaceError(RuntimeError):
    pass

class BlockPool(object):
    def __init__(self, confobj):
        self.conf = confobj
        self.n_channels = self.conf['flash_config']['n_channels_per_dev']
        self.stripe_size = self.conf['stripe_size']

        self.pool = MultiChannelBlockPool(
                n_channels=self.n_channels,
                n_blocks_per_channel=self.conf.n_blocks_per_channel,
                n_pages_per_block=self.conf.n_pages_per_block,
                tags=[TDATA, TTRANS],
                leveling_factor = self.conf['wear_leveling_factor'],
                leveling_diff = self.conf['wear_leveling_diff']
                )

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

    def get_wear_status(self):
        return self.pool.get_wear_status()

    def need_wear_leveling(self):
        return self.pool.need_wear_leveling()

    def get_erasure_count(self):
        return self.pool.get_erasure_count()

    def get_erasure_count_dist(self):
        return self.pool.get_erasure_count_dist()

    def count_blocks(self, tag, channels=None):
        return self.pool.count_blocks(tag, channels)

    def pop_a_free_block_to_trans(self, choice=LEAST_ERASED):
        try:
            blocknum = self.pool.pick_and_move(src=TFREE, dst=TTRANS,
                    choice=LEAST_ERASED)
        except TagOutOfSpaceError:
            raise OutOfSpaceError
        return blocknum

    def pop_a_free_block_to_data(self, choice=LEAST_ERASED):
        try:
            blocknum = self.pool.pick_and_move(src=TFREE, dst=TDATA,
                    choice=LEAST_ERASED)
        except TagOutOfSpaceError:
            raise OutOfSpaceError
        return blocknum

    def move_used_data_block_to_free(self, blocknum):
        self.pool.change_tag(blocknum, src=TDATA, dst=TFREE)

    def move_used_trans_block_to_free(self, blocknum):
        self.pool.change_tag(blocknum, src=TTRANS, dst=TFREE)

    def move_used_trans_block_to_data(self, blocknum):
        self.pool.change_tag(blocknum, src=TTRANS, dst=TDATA)

    def next_n_data_pages_to_program_striped(self, n, seg_id=0, choice=LEAST_ERASED):
        try:
            ppns = self.pool.next_ppns(n=n, tag=TDATA, block_index=seg_id,
                    stripe_size=self.conf['stripe_size'])
        except TagOutOfSpaceError:
            raise OutOfSpaceError
        return ppns

    def next_data_page_to_program(self, seg_id=0):
        ppns = self.pool.next_ppns(n=1, tag=TDATA, block_index=seg_id,
                stripe_size=1)
        return ppns[0]

    def next_translation_page_to_program(self):
        ppns = self.pool.next_ppns(n=1, tag=TTRANS, block_index=0,
                stripe_size=1)
        return ppns[0]

    def next_gc_data_page_to_program(self, choice=LEAST_ERASED):
        ppns = self.pool.next_ppns(n=1, tag=TDATA, block_index=0,
                stripe_size=1, choice=choice)
        return ppns[0]

    def next_gc_translation_page_to_program(self, choice=LEAST_ERASED):
        ppns = self.pool.next_ppns(n=1, tag=TTRANS, block_index=0,
                stripe_size=1, choice=choice)
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

    def remove_full_cur_blocks(self):
        self.pool.remove_full_cur_blocks()


