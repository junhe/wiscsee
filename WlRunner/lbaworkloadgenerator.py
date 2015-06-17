import abc
import random

import config

class LBAWorkloadGenerator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
        return

class Sequential(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

    def __iter__(self):
        n_flash_pages = self.conf.total_num_pages()
        n_lba_pages = int(n_flash_pages * \
            self.conf["LBA"]["lba_to_flash_size_ratio"])

        n_accesses = n_lba_pages
        for i in range(n_accesses):
            page = i % n_lba_pages # restrict to lba space
            offset = page * self.conf['flash_page_size']
            size = self.conf["flash_page_size"]
            event = 'write {} {}'.format(offset, size)
            yield event

class Random(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

    def __iter__(self):
        n_flash_pages = self.conf.total_num_pages()
        n_lba_pages = int(n_flash_pages * \
            self.conf["LBA"]["lba_to_flash_size_ratio"])

        n_accesses = n_lba_pages
        random.seed(1)
        for i in range(n_accesses):
            page = int(random.random() * n_lba_pages) # restrict to lba space
            offset = page * self.conf['flash_page_size']
            size = self.conf["flash_page_size"]
            event = 'write {} {}'.format(offset, size)
            yield event

