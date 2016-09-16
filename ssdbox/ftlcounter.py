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

import prepare4pyreuse
from pyreuse.sysutils import blocktrace, blockclassifiers, dumpe2fsparser
from pyreuse.fsutils import ext4dumpextents



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
        self.do_stats()

    def do_stats(self):
        lpns = self.get_lpns()

        self.dump_counts(lpns)
        self.dump_lpn_sem(lpns)

    def dump_counts(self, lpns):
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

        self.clean_up()

    def dump_lpn_sem(self, lpns):
        classifier = LpnClassification(
                lpns = lpns,
                device_path = self.conf['device_path'],
                result_dir = self.conf['result_dir'],
                flash_page_size = 2048)
        table = classifier.classify()

        lpn_sem_path = os.path.join(self.conf['result_dir'], 'lpn_sem.out')
        with open(lpn_sem_path, 'w') as f:
            f.write(utils.table_to_str(table, width=0))


    def get_lpns(self):
        lpns = list(set(self.read_count.keys()
            + self.write_count.keys()
            + self.discard_count.keys()))
        lpns = sorted(lpns)

        return lpns

    def clean_up(self):
        with utils.cd(self.conf['result_dir']):
            utils.shcmd('rm blkparse*', ignore_error=True)

    def get_type(self):
        return "ftlcounter"


class LpnClassification(object):
    def __init__(self, lpns, device_path, result_dir, flash_page_size):
        self.device_path = device_path
        self.result_dir = result_dir
        self.flash_page_size = flash_page_size
        self.lpns = lpns

        self.dumpe2fs_out_path = os.path.join(self.result_dir, 'dumpe2fs.out')
        self.extents_path = os.path.join(self.result_dir, 'extents.json')
        self.fs_block_size = 4096

    def classify(self):
        extents = self._get_extents()
        filepath_classifier = blockclassifiers.Ext4FileClassifier(extents,
                self.fs_block_size)

        range_table = self._get_range_table()
        classifier = blockclassifiers.Ext4BlockClassifier(range_table,
                self.fs_block_size)

        table = []
        for lpn in self.lpns:
            offset  = lpn * self.flash_page_size
            sem = classifier.classify(offset)
            if sem == 'UNKNOWN':
                sem = filepath_classifier.classify(offset)
            row = {'lpn': lpn,
                   'sem':sem}
            table.append(row)

        return table

    def _get_extents(self):
        d = utils.load_json(self.extents_path)
        extents = d['extents']

        return extents

    def _get_range_table(self):
        with open(self.dumpe2fs_out_path, 'r') as f:
            text = f.read()

        header_text, bg_text = text.split("\n\n\n")

        range_table = dumpe2fsparser.parse_bg_text(bg_text)

        j_start, j_end = self._get_journal_block_ext(header_text)
        if j_start != -1:
            range_table.append( {'journal': (j_start, j_end)} )

        return range_table

    def _get_journal_block_ext(self, header_text):
        header_dict = dumpe2fsparser.parse_header_text(header_text)

        if header_dict.has_key('journal-inode') is not True:
            return -1, -1

        journal_inum = header_dict['journal-inode']
        journal_len = header_dict['journal-length']

        ext_text = ext4dumpextents.dump_extents_of_a_file(self.device_path,
                '<{}>'.format(journal_inum))
        table = ext4dumpextents.parse_dump_extents_output(ext_text)
        return table[0]['Physical_start'], table[0]['Physical_end']


