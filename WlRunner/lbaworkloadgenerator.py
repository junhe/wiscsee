import abc
import random

class LBAWorkloadGenerator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
        return

class Sequential(LBAWorkloadGenerator):
    def __init__(self, flash_page_size,
        flash_npage_per_block,
        flash_num_blocks,
        lba_to_flash_size_ratio = 0.5):
        self.flash_page_size = flash_page_size
        self.flash_npage_per_block = flash_npage_per_block
        self.flash_num_blocks = flash_num_blocks
        self.lba_to_flash_size_ratio = lba_to_flash_size_ratio


    def __iter__(self):
        n_flash_pages = self.conf.total_num
        n_lba_pages = int(n_flash_pages * self.lba_to_flash_size_ratio)

        n_accesses = n_lba_pages
        for i in range(n_accesses):
            page = i % n_lba_pages # restrict to lba space
            offset = page * self.flash_page_size
            size = self.flash_page_size
            event = 'write {} {}'.format(offset, size)
            yield event

class Random(LBAWorkloadGenerator):
    def __init__(self, flash_page_size,
        flash_npage_per_block,
        flash_num_blocks,
        lba_to_flash_size_ratio = 0.5):
        self.flash_page_size = flash_page_size
        self.flash_npage_per_block = flash_npage_per_block
        self.flash_num_blocks = flash_num_blocks
        self.lba_to_flash_size_ratio = lba_to_flash_size_ratio

    def __iter__(self):
        n_flash_pages = self.flash_npage_per_block * self.flash_num_blocks
        n_lba_pages = int(n_flash_pages * self.lba_to_flash_size_ratio)

        n_accesses = int(0.1 * n_lba_pages)
        for i in range(n_accesses):
            page = i % n_lba_pages # restrict to lba space
            offset = page * self.flash_page_size
            size = self.flash_page_size
            event = 'write {} {}'.format(offset, size)
            yield event

