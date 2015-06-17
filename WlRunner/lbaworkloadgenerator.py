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
                       flash_num_blocks):
        self.flash_page_size = flash_page_size
        self.flash_npage_per_block = flash_npage_per_block
        self.flash_num_blocks = flash_num_blocks

    def __iter__(self):
        totalpages = self.flash_npage_per_block * self.flash_num_blocks
        for page in range(1, totalpages/100):
            page = int(page % (totalpages * 0.3))
            offset = page * self.flash_page_size
            size = self.flash_page_size
            event = 'write {} {}'.format(offset, size)
            yield event

class Random(LBAWorkloadGenerator):
    def __init__(self, flash_page_size,
                       flash_npage_per_block,
                       flash_num_blocks):
        self.flash_page_size = flash_page_size
        self.flash_npage_per_block = flash_npage_per_block
        self.flash_num_blocks = flash_num_blocks

    def __iter__(self):
        totalpages = self.flash_npage_per_block * self.flash_num_blocks
        for page in range(1, totalpages/100):
            page = int(random.random() * totalpages)
            offset = page * self.flash_page_size
            size = self.flash_page_size
            event = 'write {} {}'.format(offset, size)
            yield event

