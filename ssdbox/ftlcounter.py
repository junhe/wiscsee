import bitarray
from collections import deque, Counter
import csv
import datetime
import random
import os
import Queue
import sys

import bidict

import config
import flash
import ftlbuilder
import lrulist
import recorder
from utilities import utils
from .blkpool import BlockPool
from .bitmap import FlashBitmap2

class Config(config.ConfigNCQFTL):
    def __init__(self, confdic = None):
        super(Config, self).__init__(confdic)

        local_itmes = {
            # number of bytes per entry in mapping_on_flash
            "translation_page_entry_bytes": 4, # 32 bits
            "cache_entry_bytes": 8, # 4 bytes for lpn, 4 bytes for ppn
            "GC_threshold_ratio": 0.95,
            "GC_low_threshold_ratio": 0.9,
            "over_provisioning": 1.28,
            "mapping_cache_bytes": None # cmt: cached mapping table
            }
        self.update(local_itmes)

    @property
    def n_mapping_entries_per_page(self):
        return self.page_size / self['translation_page_entry_bytes']

    @property
    def mapping_cache_bytes(self):
        return self['mapping_cache_bytes']

    @mapping_cache_bytes.setter
    def mapping_cache_bytes(self, value):
        self['mapping_cache_bytes'] = value

    @property
    def n_cache_entries(self):
        return self.mapping_cache_bytes / self['cache_entry_bytes']

    @n_cache_entries.setter
    def n_cache_entries(self, value):
        self.mapping_cache_bytes = value * self['cache_entry_bytes']

    @property
    def cache_mapped_data_bytes(self):
        return self.n_cache_entries * self.page_size

    @cache_mapped_data_bytes.setter
    def cache_mapped_data_bytes(self, data_bytes):
        self.n_cache_entries = data_bytes / self.page_size
        if self.n_cache_entries % self.n_mapping_entries_per_page != 0:
            print "WARNING: size of mapping cache is not aligned with "\
                "translation page size."
    @property
    def translation_page_entry_bytes(self):
        return self['translation_page_entry_bytes']

    @property
    def over_provisioning(self):
        return self['over_provisioning']

    @property
    def GC_threshold_ratio(self):
        return self['GC_threshold_ratio']

    @property
    def GC_low_threshold_ratio(self):
        return self['GC_low_threshold_ratio']

    def sec_ext_to_page_ext(self, sector, count):
        """
        The sector extent has to be aligned with page
        return page_start, page_count
        """
        page = sector / self.n_secs_per_page
        page_end = (sector + count) / self.n_secs_per_page
        page_count = page_end - page
        if (sector + count) % self.n_secs_per_page != 0:
            page_count += 1
        return page, page_count


class Ftl(ftlbuilder.FtlBuilder):
    """
    The implementation literally follows DFtl paper.
    This class is a coordinator of other coordinators and data structures
    """
    def __init__(self, confobj, recorderobj, flashobj):
        super(Ftl, self).__init__(confobj, recorderobj, flashobj)

        if not isinstance(confobj, Config):
            raise TypeError("confobj is not Config. it is {}".
               format(type(confobj).__name__))

        self.n_sec_per_page = self.conf.page_size \
                / self.conf['sector_size']

        self.read_count = Counter()
        self.write_count = Counter()
        self.discard_count = Counter()

    def sec_read(self, sector, count):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        for lpn in range(lpn_start, lpn_start + lpn_count):
            self.read_count[lpn] += 1

    def sec_write(self, sector, count, data = None):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        for lpn in range(lpn_start, lpn_start + lpn_count):
            self.write_count[lpn] += 1

    def sec_discard(self, sector, count):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        for lpn in range(lpn_start, lpn_start + lpn_count):
            self.discard_count[lpn] += 1

    def pre_workload(self):
        pass

    def post_processing(self):
        """
        This function is called after the simulation.
        """
        # print self.read_count
        # print self.write_count
        # print self.discard_count
        self.dump_counts()

    def dump_counts(self):
        lpns = list(set(self.read_count.keys()
            + self.write_count.keys()
            + self.discard_count.keys()))
        lpns = sorted(lpns)

        counters = [self.read_count, self.write_count, self.discard_count]

        table = []
        for lpn in lpns:
            counts = [counter[lpn] for counter in counters]
            row = [lpn] + counts
            row = [str(x) for x in row]
            row = ' '.join(row)
            table.append(row)

        count_path = os.path.join(self.conf['result_dir'], 'lpn.count')
        with open(count_path, 'w') as f:
            f.write('lpn read write discard\n')
            for row in table:
                f.write(row)
                f.write('\n')

    def get_type(self):
        return "ftlcounter"


