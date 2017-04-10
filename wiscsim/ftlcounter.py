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
from commons import *
from wiscsim import hostevent

from pyreuse.sysutils import blocktrace, blockclassifiers, dumpe2fsparser
from pyreuse.fsutils import ext4dumpextents
from pyreuse.sysutils.filefragparser import filefrag, get_file_range_table


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

        self.total_write_bytes = 0
        self.total_read_bytes = 0
        self.total_discard_bytes = 0

    def sec_read(self, sector, count):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        self.total_read_bytes += lpn_count * self.conf.page_size

        for lpn in range(lpn_start, lpn_start + lpn_count):
            self.read_count[lpn] += 1

    def sec_write(self, sector, count, data = None):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        self.total_write_bytes += lpn_count * self.conf.page_size

        for lpn in range(lpn_start, lpn_start + lpn_count):
            self.write_count[lpn] += 1

    def sec_discard(self, sector, count):
        lpn_start, lpn_count = self.conf.sec_ext_to_page_ext(sector, count)

        self.total_discard_bytes += lpn_count * self.conf.page_size

        for lpn in range(lpn_start, lpn_start + lpn_count):
            self.discard_count[lpn] += 1

    def pre_workload(self):
        pass

    def post_processing(self):
        """
        This function is called after the simulation.
        """
        self.record_traffic()

        if self.conf['only_get_traffic'] == True:
            return

        self.do_stats()

        if self.conf['gen_ncq_depth_table'] is True:
            self.gen_ncq_depth_table_from_event()

    def do_stats(self):
        lpns = self.get_lpns()

        self.dump_counts(lpns)

        if self.conf['do_dump_lpn_sem'] is True:
            self.dump_lpn_sem(lpns)

    def record_traffic(self):
        self.recorder.add_to_general_accumulater(
                'traffic_size', 'write', self.total_write_bytes)

        self.recorder.add_to_general_accumulater(
                'traffic_size', 'read', self.total_read_bytes)

        self.recorder.add_to_general_accumulater(
                'traffic_size', 'discard', self.total_discard_bytes)

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

        # self.clean_up()

    def dump_lpn_sem(self, lpns):
        if self.conf['filesystem'] == 'ext4':
            self.dump_lpn_sem_ext4(lpns)

        elif self.conf['filesystem'] == 'f2fs':
            self.dump_lpn_sem_f2fs(lpns)

        elif self.conf['filesystem'] == 'xfs':
            self.dump_lpn_sem_xfs(lpns)

        else:
            raise RuntimeError('{} is not supported'.format(
                self.conf['filesystem']))


    def dump_lpn_sem_ext4(self, lpns):
        classifier = LpnClassification(
                lpns = lpns,
                device_path = self.conf['device_path'],
                result_dir = self.conf['result_dir'],
                flash_page_size = 2048)
        table = classifier.classify()

        lpn_sem_path = os.path.join(self.conf['result_dir'], 'lpn_sem.out')
        with open(lpn_sem_path, 'w') as f:
            f.write(utils.table_to_str(table, width=0))

    def dump_lpn_sem_f2fs(self, lpns):
        classifier = F2FSLpnClassification(
                lpns = lpns,
                device_path = self.conf['device_path'],
                result_dir = self.conf['result_dir'],
                flash_page_size = 2048)
        table = classifier.classify()

        lpn_sem_path = os.path.join(self.conf['result_dir'], 'lpn_sem.out')
        with open(lpn_sem_path, 'w') as f:
            f.write(utils.table_to_str(table, width=0))

    def dump_lpn_sem_xfs(self, lpns):
        file_ranges = get_range_table(self.conf['fs_mount_point'])

        classifier = XFSLpnClassification(
                lpns = lpns,
                device_path = self.conf['device_path'],
                result_dir = self.conf['result_dir'],
                flash_page_size = 2048,
                external_ranges = file_ranges
                )
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

    def gen_ncq_depth_table_from_event(self):
        if self.conf["workload_src"] == config.LBAGENERATOR:
            event_file_path = self.conf["lba_workload_configs"]["ftlsim_event_path"]
        else:
            event_file_path = self.conf.get_ftlsim_events_output_path()

        workload_line_iter = hostevent.FileLineIterator(event_file_path)
        event_workload_iter = hostevent.EventIterator(self.conf, workload_line_iter)

        parser = EventNCQParser(event_workload_iter)
        table = parser.parse()

        ncq_depth_table_path = os.path.join(self.conf['result_dir'],
                'ncq_depth_timeline.txt')
        with open(ncq_depth_table_path, 'w') as f:
            f.write(utils.table_to_str(table, width=0))


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
        # extents = self._get_extents()
        # filepath_classifier = blockclassifiers.Ext4FileClassifier(extents,
                # self.fs_block_size)

        range_table = self._get_range_table()
        classifier = blockclassifiers.Ext4BlockClassifier(range_table,
                self.fs_block_size)

        table = []
        for lpn in self.lpns:
            offset  = lpn * self.flash_page_size
            sem = classifier.classify(offset)
            # if sem == 'UNKNOWN':
                # sem = filepath_classifier.classify(offset)
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


class F2FSLpnClassification(object):
    def __init__(self, lpns, device_path, result_dir, flash_page_size):
        self.device_path = device_path
        self.result_dir = result_dir
        self.flash_page_size = flash_page_size
        self.lpns = lpns

        self.fs_block_size = 4096

    def classify(self):
        classifier = self._get_classifier()

        table = []
        for lpn in self.lpns:
            offset  = lpn * self.flash_page_size
            sem = classifier.classify(offset)
            row = {'lpn': lpn,
                   'sem':sem}
            table.append(row)

        return table

    def _get_classifier(self):
        """
        =====[ partition info(sdc1). #0 ]=====
                |<-- aligned to segment
        [SB: 1] [CP: 2] [SIT: 2] [NAT: 4] [SSA: 1] [MAIN: 502(OverProv:70 Resv:48)]
        """
        range_table = [
                {'Superblock': (0, 2*MB)},
                {'Checkpoint': (2*MB, 4*MB)},
                {'SegInfoTab': (4*MB, 6*MB)},
                {'NodeAddrTab': (6*MB, 10*MB)},
                {'SegSummArea': (10*MB, 11*MB)},
                {'MainArea': (20*MB, 1024*MB)},
                ]

        classifier = blockclassifiers.OffsetClassifier(range_table)

        return classifier

class XFSLpnClassification(object):
    def __init__(self, lpns, device_path, result_dir, flash_page_size, external_ranges=None):
        self.device_path = device_path
        self.result_dir = result_dir
        self.flash_page_size = flash_page_size
        self.lpns = lpns
        self.external_ranges = external_ranges

        self.fs_block_size = 4096

    def classify(self):
        classifier = self._get_classifier()

        table = []
        for lpn in self.lpns:
            offset  = lpn * self.flash_page_size
            sem = classifier.classify(offset)
            row = {'lpn': lpn,
                   'sem':sem}
            table.append(row)

        return table

    def _get_classifier(self):
        """
        block 32768 (1/0) type sb
        block 32769 (1/1) type agf
        block 32770 (1/2) type agi
        block 32771 (1/3) type agfl
        block 32772 (1/4) type btbno
        block 32773 (1/5) type btcnt
        block 32774 (1/6) type btino
        block 32775 (1/7) type freelist
        block 32776 (1/8) type freelist
        block 32777 (1/9) type freelist
        block 32778 (1/10) type freelist
        """
        range_table = [
                {'Journal': (131079*4*KB, 131079*4*KB + 10*MB)}
                ]

        BLOCKSIZE = 4*KB
        for i in range(8):
            ag_start = i * 128*MB
            range_table.extend(
                [
                    {'Superblock'          : (ag_start + 0 * BLOCKSIZE, ag_start + 1 * BLOCKSIZE)},
                    {'FreeBlockInfo'       : (ag_start + 1 * BLOCKSIZE, ag_start + 2 * BLOCKSIZE)},
                    {'InodeInfo'           : (ag_start + 2 * BLOCKSIZE, ag_start + 3 * BLOCKSIZE)},
                    {'FreeListInfo'        : (ag_start + 3 * BLOCKSIZE, ag_start + 4 * BLOCKSIZE)},
                    {'FreeSpTree1Root'     : (ag_start + 4 * BLOCKSIZE, ag_start + 5 * BLOCKSIZE)},
                    {'FreeSpTree2Root'     : (ag_start + 5 * BLOCKSIZE, ag_start + 6 * BLOCKSIZE)},
                    {'InodeTreeRoot'       : (ag_start + 6 * BLOCKSIZE, ag_start + 7 * BLOCKSIZE)},
                    {'FreeList'            : (ag_start + 7 * BLOCKSIZE, ag_start + 11 * BLOCKSIZE)},
                ])

        if not self.external_ranges is None:
            range_table.extend(self.external_ranges)

        classifier = blockclassifiers.OffsetClassifier(range_table)

        return classifier


def get_range_table(dirpath):
    byte_ranges = get_file_range_table(dirpath)

    ret_table = []
    for row in byte_ranges:
        sem = os.path.basename(row['path'])
        start = row['start_byte']
        end = start + row['size']
        new_row = {sem: (start, end)}
        ret_table.append(new_row)

    return ret_table



class EventNCQParser(object):
    def __init__(self, event_iter):
        self.event_iter = event_iter

    def parse(self):
        table = []
        depth = 0
        for event in self.event_iter:
            action = event.action.strip()

            pre_depth = depth
            if action == 'D':
                depth += 1
            elif action == 'C':
                depth -= 1
            else:
                raise RuntimeError('action has to be D or C')
            post_depth = depth

            row = {'action': action,
                   'operation': event.operation,
                   'timestamp': event.timestamp,
                   'offset': event.offset,
                   'size': event.size,
                   'pid': event.pid,
                   'pre_depth': pre_depth,
                   'post_depth': post_depth}
            table.append(row)

        return table


