import bitarray
from collections import deque, Counter
import csv
import datetime
import heapq
import itertools
import random
import os
import Queue
import sys
import simpy

import bidict

import config
import flash
import ftlbuilder
from lrulist import LruDict, SegmentedLruCache, LruCache
import recorder
from utilities import utils
from commons import *
from ftlsim_commons import *
from .blkpool import BlockPool, MOST_ERASED, LEAST_ERASED
from .bitmap import FlashBitmap2



UNINITIATED, MISS = ('UNINIT', 'MISS')
DATA_BLOCK, TRANS_BLOCK = ('data_block', 'trans_block')
random.seed(0)
LOGICAL_READ, LOGICAL_WRITE, LOGICAL_DISCARD = ('LOGICAL_READ', \
        'LOGICAL_WRITE', 'LOGICAL_DISCARD')

PURPOSE_GC = 'PURPOSE_GC'
PURPOSE_WEAR_LEVEL = 'PURPOSE_WEAR_LEVEL'

#
# - translation pages
#   - cache miss read (trans.cache.load)
#   - eviction write  (trans.cache.evict)
#   - cleaning read   (trans.clean)
#   - cleaning write  (trans.clean)
# - data pages
#   - user read       (data.user)
#   - user write      (data.user)
#   - cleaning read   (data.cleaning)
#   - cleaning writes (data.cleaning)
# Tag format
# pagetype.
# Example tags:

# trans cache read is due to cache misses, the read fetches translation page
# to cache.
# write is due to eviction. Note that entry eviction may incure both page read
# and write.
TRANS_CACHE = "trans.cache"

# trans clean include:
#  erasing translation block
#  move translation page during gc (including read and write)
TRANS_CLEAN = "trans.clean"  #read/write are for moving pages

#  clean_data_block()
#   update_mapping_in_batch()
#    update_translation_page_on_flash() this is the same as cache eviction
TRANS_UPDATE_FOR_DATA_GC = "trans.update.for.data.gc"

DATA_USER = "data.user"

# erase data block in clean_data_block()
# move data page during gc (including read and write)
DATA_CLEANING = "data.cleaning"



class Ftl(object):
    def __init__(self, confobj, recorderobj, flashcontrollerobj, env):
        if not isinstance(confobj, Config):
            raise TypeError("confobj is not Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj
        self.recorder = recorderobj
        self.flash = flashcontrollerobj
        self.env = env

        self.block_pool = BlockPool(confobj)
        self.oob = OutOfBandAreas(confobj)

        self._directory = GlobalTranslationDirectory(self.conf,
                self.oob, self.block_pool)

        self._trans_page_locks =  LockPool(self.env)

        self._mappings = MappingCache(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = self.flash,
            oobobj = self.oob,
            recorderobj = self.recorder,
            envobj = self.env,
            directory = self._directory,
            mapping_on_flash = MappingOnFlash(self.conf),
            trans_page_locks = self._trans_page_locks
            )

        self.n_sec_per_page = self.conf.page_size \
                / self.conf['sector_size']

        self._cleaner = Cleaner(
            conf = self.conf,
            flash = self.flash,
            oob = self.oob,
            block_pool = self.block_pool,
            mappings = self._mappings,
            directory = self._directory,
            rec = self.recorder,
            env = self.env,
            trans_page_locks = self._trans_page_locks
            )

        self._check_segment_config()

        self.written_bytes = 0
        self.discarded_bytes = 0
        self.read_bytes = 0
        self.pre_written_bytes = 0
        self.pre_discarded_bytes = 0
        self.pre_read_bytes = 0
        self.display_interval = 4 * MB

    def _check_segment_config(self):
        if self.conf['segment_bytes'] % (self.conf.n_pages_per_block \
                * self.conf.page_size) != 0:
            print 'WARNING: segment should be multiple of block size'

    def _ppns_to_write(self, ext, new_mappings):
        ppns = []
        for lpn in ext.lpn_iter():
            ppns.append(new_mappings[lpn])

        return ppns

    def get_ppns_to_write(self, ext):
        exts_by_seg = split_ext_by_segment(self.conf.n_pages_per_segment, ext)
        mapping = {}
        for seg_id, seg_ext in exts_by_seg.items():
            ppns = self.block_pool.next_n_data_pages_to_program_striped(
                    n=seg_ext.lpn_count, seg_id=seg_id,
                    choice=LEAST_ERASED)
            mapping.update( dict(zip(seg_ext.lpn_iter(), ppns)) )
        return mapping

    def flush_trans_cache(self):
        yield self.env.process(self._mappings.flush())

    def drop_trans_cache(self):
        self._mappings.drop()

    def purge_trans_cache(self):
        yield self.env.process(self._mappings.flush())
        self._mappings.drop()

    def write_ext(self, extent):
        req_size = extent.lpn_count * self.conf.page_size
        self.recorder.add_to_general_accumulater('traffic', 'write', req_size)
        self.written_bytes += req_size
        if self.written_bytes > self.pre_written_bytes + self.display_interval:
            print 'Written (MB)', self.pre_written_bytes / MB, 'writing', round(float(req_size) / MB, 2)
            sys.stdout.flush()
            self.pre_written_bytes = self.written_bytes

        op_id = self.recorder.get_unique_num()
        start_time = self.env.now # <----- start

        exts_in_mvpngroup = split_ext_to_mvpngroups(self.conf, extent)
        new_mappings = self.get_ppns_to_write(extent)

        procs = []
        for ext_single_m_vpn in exts_in_mvpngroup:
            ppns_of_ext = self._ppns_to_write(ext_single_m_vpn, new_mappings)
            p = self.env.process(
                self._write_single_mvpngroup(ext_single_m_vpn, ppns_of_ext,
                    tag = op_id))
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)

        write_timeline(self.conf, self.recorder,
            op_id = op_id, op = 'write_ext', arg = extent.lpn_start,
            start_time = start_time, end_time = self.env.now)

    def _write_single_mvpngroup(self, ext_single_m_vpn, ppns_to_write,
            tag=None):
        m_vpn = self.conf.lpn_to_m_vpn(ext_single_m_vpn.lpn_start)

        p_relocate = self.env.process(
            self._update_metadata_for_relocating_lpns(
                ext_single_m_vpn.lpn_iter(),
                new_ppns=ppns_to_write, tag=tag))

        p_w_user = self.env.process(self._program_user_data(ppns_to_write, tag))

        yield simpy.events.AllOf(self.env, [p_relocate, p_w_user])

    def _program_user_data(self, ppns_to_write, tag=None):
        start_time = self.env.now
        op_id = self.recorder.get_unique_num()

        channel_tag = self.recorder.get_tag('write_user', tag)
        yield self.env.process(
            self.flash.rw_ppns(ppns_to_write, 'write',
                tag = channel_tag))

        write_timeline(self.conf, self.recorder,
            op_id = op_id, op = 'write_user_data', arg = len(ppns_to_write),
            start_time = start_time, end_time = self.env.now)

    def _update_metadata_for_relocating_lpns(self, lpns, new_ppns, tag=None):
        """
        This may be parallelized.
        """
        old_ppns = yield self.env.process(
                self._mappings.lpns_to_ppns(lpns, tag))

        for lpn, old_ppn, new_ppn in zip(lpns, old_ppns, new_ppns):
            yield self.env.process(
                self._update_metadata_for_relocating_lpn(
                    lpn, old_ppn, new_ppn, tag))

    def _update_metadata_for_relocating_lpn(self, lpn, old_ppn, new_ppn,
            tag=None):
        """
        contents of lpn used to be in old_ppn, but now it is in new_ppn.
        This function adjust all metadata to reflect the change.

        ----- template for metadata change --------
        # mappings in cache
        raise NotImplementedError()

        # mappings on flash
        raise NotImplementedError()

        # translation directory
        raise NotImplementedError()

        # oob state
        raise NotImplementedError()

        # oob ppn->lpn/vpn
        raise NotImplementedError()

        # blockpool
        raise NotImplementedError()
        """
        # mappings in cache
        yield self.env.process(
                self._mappings.update(lpn=lpn, ppn=new_ppn, tag=tag))

        # mappings on flash
        #   handled by _mappings

        # translation directory
        #   handled by _mappings

        # oob state
        # oob ppn->lpn/vpn
        self.oob.relocate_data_page(lpn=lpn, old_ppn=old_ppn, new_ppn=new_ppn,
                update_time=True)

        # blockpool
        #   should be handled when we got new_ppn

    def read_ext(self, extent):
        req_size = extent.lpn_count * self.conf.page_size
        self.recorder.add_to_general_accumulater('traffic', 'read', req_size)
        self.read_bytes += req_size
        if self.read_bytes > self.pre_read_bytes + self.display_interval:
            print 'Read (MB)', self.pre_read_bytes / MB, 'reading', round(float(req_size) / MB, 2)
            sys.stdout.flush()
            self.pre_read_bytes = self.read_bytes

        ext_list = split_ext_to_mvpngroups(self.conf, extent)
        # print [str(x) for x in ext_list]

        op_id = self.recorder.get_unique_num()
        start_time = self.env.now

        procs = []
        for ext_single_m_vpn in ext_list:
            p = self.env.process(
                    self._read_single_mvpngroup(ext_single_m_vpn, tag=op_id))
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)

        write_timeline(self.conf, self.recorder,
            op_id = op_id, op = 'read_ext', arg = extent.lpn_start,
            start_time = start_time, end_time = self.env.now)

    def _read_single_mvpngroup(self, ext_single_m_vpn, tag=None):
        m_vpn = self.conf.lpn_to_m_vpn(ext_single_m_vpn.lpn_start)


        ppns_to_read = yield self.env.process(
                self._mappings.lpns_to_ppns(ext_single_m_vpn.lpn_iter(),
                    tag=tag))
        ppns_to_read = remove_invalid_ppns(ppns_to_read)


        op_id = self.recorder.get_unique_num()
        start_time = self.env.now

        yield self.env.process(
            self.flash.rw_ppns(ppns_to_read, 'read',
                tag=self.recorder.get_tag('read_user', tag)))

        write_timeline(self.conf, self.recorder,
            op_id=op_id, op='read_user_data', arg=len(ppns_to_read),
            start_time = start_time, end_time = self.env.now)

    def discard_ext(self, extent):
        req_size = extent.lpn_count * self.conf.page_size
        self.recorder.add_to_general_accumulater('traffic', 'discard', req_size)
        self.discarded_bytes += req_size
        if self.discarded_bytes > self.pre_discarded_bytes + self.display_interval:
            print 'Discarded (MB)', self.pre_discarded_bytes / MB, 'discarding', round(float(req_size) / MB, 2)
            sys.stdout.flush()
            self.pre_discarded_bytes = self.discarded_bytes

        ext_list = split_ext_to_mvpngroups(self.conf, extent)

        procs = []
        for ext_single_m_vpn in ext_list:
            p = self.env.process(self._discard_single_mvpngroup(ext_single_m_vpn))
            procs.append(p)

        yield simpy.events.AllOf(self.env, procs)

    def _discard_single_mvpngroup(self, ext_single_m_vpn):
        m_vpn = self.conf.lpn_to_m_vpn(ext_single_m_vpn.lpn_start)

        ppns_to_invalidate = yield self.env.process(
                self._mappings.lpns_to_ppns(ext_single_m_vpn.lpn_iter()))

        ppns_to_invalidate = remove_invalid_ppns(ppns_to_invalidate)

        mapping_dict = dict(itertools.izip_longest(
            ppns_to_invalidate, (), fillvalue = UNINITIATED))
        self._mappings.update_batch(mapping_dict)

        self.oob.invalidate_ppns(ppns_to_invalidate)

    def is_cleaning_needed(self):
        return self._cleaner.is_cleaning_needed()

    def is_wear_leveling_needed(self):
        factor, diff = self.block_pool.get_wear_status()
        self.recorder.append_to_value_list('wear_diff', diff)
        print 'ddddddddddddddddddddiiiiiiiiiiifffffffffff', diff

        return self.block_pool.need_wear_leveling()

    def clean(self, forced=True):
        yield self.env.process(self._cleaner.clean())

    def level_wear(self):
        yield self.env.process(self._cleaner.level_wear())

    def snapshot_valid_ratios(self):
        victim_blocks = VictimBlocks(self.conf, self.block_pool, self.oob)
        ratios = victim_blocks.get_valid_ratio_counter_of_used_blocks()
        self.recorder.append_to_value_list('ftl_func_valid_ratios',
                ratios)

    def snapshot_user_traffic(self):
        self.recorder.append_to_value_list('ftl_func_user_traffic',
                {'timestamp': self.env.now/float(SEC),
                 'write_traffic_size': self.written_bytes,
                 'read_traffic_size': self.read_bytes,
                 'discard_traffic_size': self.discarded_bytes,
                 },
                )

    def snapshot_erasure_count_dist(self):
        dist = self.block_pool.get_erasure_count_dist()
        print self.env.now
        print dist
        self.recorder.append_to_value_list('ftl_func_erasure_count_dist',
                dist)


def remove_invalid_ppns(ppns):
    return [ppn for ppn in ppns if not ppn in (UNINITIATED, MISS)]

def split_ext_to_mvpngroups(conf, extent):
    """
    return a list of extents, each belongs to one m_vpn
    """
    group_extent_list = []
    for i, lpn in enumerate(extent.lpn_iter()):
        cur_m_vpn = conf.lpn_to_m_vpn(lpn)
        if i == 0:
            # intialization
            cur_group_extent = Extent(lpn_start = extent.lpn_start,
                lpn_count = 1)
            group_extent_list.append(cur_group_extent)
        else:
            if cur_m_vpn == last_m_vpn:
                cur_group_extent.lpn_count += 1
            else:
                cur_group_extent = Extent(lpn_start = lpn,
                    lpn_count = 1)
                group_extent_list.append(cur_group_extent)

        last_m_vpn = cur_m_vpn

    return group_extent_list


class MappingDict(dict):
    """
    Used to map lpn->ppn
    """
    pass


class FlashTransmitMixin(object):
    def _write_back(self, m_vpn, tag=None):
        assert m_vpn in self._trans_page_locks.locked_addrs

        mapping_in_cache = self._lpn_table.get_m_vpn_mappings(m_vpn)

        # We have to mark it clean before writing it back because
        # if we do it after writing flash, the cache may already changed
        self._lpn_table.mark_clean_multiple(mapping_in_cache.keys())

        if len(mapping_in_cache) < self.conf.n_mapping_entries_per_page:
            # Not all mappings are in cache
            self.recorder.count_me("translation", 'read_trans_page-for-write-back')
            mapping_in_flash = yield self.env.process(
                    self._read_translation_page(m_vpn, tag))
            latest_mapping = mapping_in_flash
            latest_mapping.update(mapping_in_cache)
        else:
            # all mappings are in cache, no need to read the translation page
            latest_mapping = mapping_in_cache

        yield self.env.process(
            self.__update_mapping_on_flash(m_vpn, latest_mapping, tag))

    def _read_translation_page(self, m_vpn, tag=None):
        lpns = self.conf.m_vpn_to_lpns(m_vpn)
        mapping_dict = self.mapping_on_flash.lpns_to_ppns(lpns)


        # as if we readlly read from flash
        m_ppn = self.directory.m_vpn_to_m_ppn(m_vpn)

        op_id = self.recorder.get_unique_num()
        start_time = self.env.now

        yield self.env.process(
                self.flash.rw_ppn_extent(m_ppn, 1, 'read',
                tag = self.recorder.get_tag('read_trans', tag)))

        write_timeline(self.conf, self.recorder,
            op_id = op_id, op = 'read_trans_page', arg = m_vpn,
            start_time = start_time, end_time = self.env.now)

        self.env.exit(mapping_dict)

    def __update_mapping_on_flash(self, m_vpn, mapping_dict, tag=None):
        """
        mapping_dict should only has lpns belonging to m_vpn
        """
        # mapping_dict has to have all and only the entries of m_vpn
        lpn_sample = mapping_dict.keys()[0]
        tmp_m_vpn = self.conf.lpn_to_m_vpn(lpn_sample)
        assert tmp_m_vpn == m_vpn
        assert len(mapping_dict) == self.conf.n_mapping_entries_per_page

        self.mapping_on_flash.batch_update(mapping_dict)

        yield self.env.process(self.__program_translation_page(m_vpn, tag))

    def __program_translation_page(self, m_vpn, tag=None):
        new_m_ppn = self.block_pool.next_translation_page_to_program()
        old_m_ppn = self.directory.m_vpn_to_m_ppn(m_vpn)

        op_id = self.recorder.get_unique_num()
        start_time = self.env.now

        yield self.env.process(
                self.flash.rw_ppn_extent(new_m_ppn, 1, 'write',
                tag = self.recorder.get_tag('prog_trans', tag)))

        write_timeline(self.conf, self.recorder,
            op_id = op_id, op = 'prog_trans_page', arg = m_vpn,
            start_time = start_time, end_time = self.env.now)

        self.oob.relocate_trans_page(m_vpn=m_vpn, old_ppn=old_m_ppn,
            new_ppn = new_m_ppn, update_time=True)
        self.directory.update_mapping(m_vpn = m_vpn, m_ppn = new_m_ppn)

        assert self.oob.states.is_page_valid(old_m_ppn) == False
        assert self.oob.states.is_page_valid(new_m_ppn) == True
        assert self.oob.ppn_to_lpn_mvpn[new_m_ppn] == m_vpn
        assert self.directory.m_vpn_to_m_ppn(m_vpn) == new_m_ppn


class InsertMixin(object):
    def _insert_new_mapping(self, lpn, ppn, tag=None):
        assert not self._lpn_table.has_lpn(lpn), 'lpn: {}'.format(lpn)
        # no free space for this insertion, free and lock 1
        locked_rows = yield self.env.process(
                self.__add_locked_room_for_insert(tag=tag))
        locked_row_id = locked_rows[0]

        # even if you inserting and loading of the same lpn can be
        # serialize, but loading of lpns in the same mvpn which
        # also brings lpn to memory is not serialized.
        self._lpn_table.add_lpn(rowid = locked_row_id,
                lpn = lpn, ppn = ppn, dirty = True)

    def _add_to_free(self, lpn, ppn):
        locked_row_id = self._lpn_table.lock_free_row()
        self._lpn_table.add_lpn(rowid = locked_row_id,
                lpn = lpn, ppn = ppn, dirty = True)

    def __add_locked_room_for_insert(self, tag=None):
        locked_row_ids = []
        row_id = yield self.env.process(self.__evict_entry_for_insert(tag))
        locked_row_ids.append(row_id)

        self.env.exit(locked_row_ids)

    def __evict_entry_for_insert(self, tag=None):
        victim_row = self._victim_row(avoid_m_vpns=[])
        victim_row.state = USED_AND_HOLD

        yield self._concurrent_trans_quota.get(1)

        # lock m_vpn
        m_vpn = self.conf.lpn_to_m_vpn(lpn = victim_row.lpn)
        tp_req = self._trans_page_locks.get_request(m_vpn)
        yield tp_req
        self._trans_page_locks.locked_addrs.add(m_vpn)

        if victim_row.dirty == True:
            self.recorder.count_me('translation', 'write-back-dirty-for-insert')
            yield self.env.process(self._write_back(m_vpn, tag))

        assert self._lpn_table.has_lpn(victim_row.lpn), \
                "lpn_table does not has lpn {}.".format(victim_row.lpn)
        # Hmm... victim_row could be updated when you are writing them back
        # this is OK because we have written the old mapping to flash
        # and the new one in cache is marked dirty, we will
        # later write the new dirty one back to flash
        # assert victim_row.dirty == False, repr(victim_row)
        assert victim_row.state == USED_AND_HOLD
        victim_row.state = USED

        self.recorder.count_me('translation', 'delete-lpn-in-table-for-insert')
        locked_row_id = self._lpn_table.delete_lpn_and_lock(victim_row.lpn)

        self._trans_page_locks.release_request(m_vpn, tp_req)
        self._trans_page_locks.locked_addrs.remove(m_vpn)

        yield self._concurrent_trans_quota.put(1)

        self.env.exit(locked_row_id)


class LoadMixin(object):
    def _load_missing(self, m_vpn, wanted_lpn, tag=None):
        """
        Return True if we really load flash page
        """
        yield self._concurrent_trans_quota.get(2)
        tp_req = self._trans_page_locks.get_request(m_vpn)
        yield tp_req
        self._trans_page_locks.locked_addrs.add(m_vpn)

        # check again before really loading
        if wanted_lpn is not None and not self._lpn_table.has_lpn(wanted_lpn):
            n_needed = self._lpn_table.needed_space_for_m_vpn(m_vpn)
            locked_rows = self._lpn_table.lock_free_rows(n_needed)
            n_more = n_needed - len(locked_rows)

            if n_more > 0:
                more_locked_rows = yield self.env.process(
                    self.__add_locked_room_for_load(n_more, loading_m_vpn=m_vpn,
                        tag=tag))
                locked_rows += more_locked_rows

            yield self.env.process(
                self.__load_to_locked_space(m_vpn, locked_rows, tag=tag))

            loaded = True
        else:
            loaded = False

        ppn = self._lpn_table.lpn_to_ppn(wanted_lpn)

        self._trans_page_locks.release_request(m_vpn, tp_req)
        self._trans_page_locks.locked_addrs.remove(m_vpn)

        yield self._concurrent_trans_quota.put(2)

        self.env.exit((loaded, ppn))

    def __add_locked_room_for_load(self, n_needed, loading_m_vpn, tag=None):
        locked_row_ids = []
        for i in range(n_needed):
            row_id = yield self.env.process(
                    self.__evict_entry_for_load(loading_m_vpn, tag))
            locked_row_ids.append(row_id)

        self.env.exit(locked_row_ids)

    def __evict_entry_for_load(self, loading_m_vpn, tag=None):
        victim_row = self._victim_row(
                [loading_m_vpn] + list(self._trans_page_locks.locked_addrs))
        victim_row.state = USED_AND_HOLD

        m_vpn = self.conf.lpn_to_m_vpn(lpn = victim_row.lpn)

        tp_req = self._trans_page_locks.get_request(m_vpn)
        yield tp_req
        self._trans_page_locks.locked_addrs.add(m_vpn)

        if victim_row.dirty == True:
            self.recorder.count_me('translation', 'write-back-dirty-for-load')
            yield self.env.process(self._write_back(m_vpn, tag))

        # after writing back, this lpn could already been deleted
        # by another _evict_entry()?
        assert self._lpn_table.has_lpn(victim_row.lpn), \
                "lpn_table does not has lpn {}.".format(victim_row.lpn)
        # assert victim_row.dirty == False, repr(victim_row)
        assert victim_row.state == USED_AND_HOLD
        victim_row.state = USED

        # This is the only place that we delete a lpn
        self.recorder.count_me('translation', 'delete-lpn-in-table-for-load')
        locked_row_id = self._lpn_table.delete_lpn_and_lock(victim_row.lpn)

        self._trans_page_locks.release_request(m_vpn, tp_req)
        self._trans_page_locks.locked_addrs.remove(m_vpn)

        self.env.exit(locked_row_id)

    def __load_to_locked_space(self, m_vpn, locked_rows, tag=None):
        """
        It should not call _write_back() directly or indirectly as it
        will deadlock.
        """
        self.recorder.count_me('translation', 'read-trans-for-load')
        mapping_dict = yield self.env.process(
                self._read_translation_page(m_vpn, tag))
        uncached_mapping = self.__get_uncached_mappings(mapping_dict)

        n_needed = len(uncached_mapping)
        needed_rows = locked_rows[:n_needed]
        unused_rows = locked_rows[n_needed:]

        self._lpn_table.add_lpns(needed_rows, uncached_mapping, False,
                as_least_recent = True)
        self._lpn_table.unlock_free_rows(unused_rows)

    def __get_uncached_mappings(self, mapping_dict):
        uncached_mapping = {}
        for lpn, ppn in mapping_dict.items():
            if not self._lpn_table.has_lpn(lpn):
                uncached_mapping[lpn] = ppn
        return uncached_mapping

class FlushMixin(object):
    """
    Write back all dirty entries in translation cache

    Flush has to be run alone without other processes. Run barrier before
    calling flush
    """
    def _flush(self, tag=None):
        for lpn, row in self._lpn_table.least_to_most_lpn_items():
            if row.dirty is True:
                # write back this m_vpn
                m_vpn = self.conf.lpn_to_m_vpn(lpn)

                tp_req = self._trans_page_locks.get_request(m_vpn)
                yield tp_req
                self._trans_page_locks.locked_addrs.add(m_vpn)

                self.recorder.count_me('translation', 'write-back-dirty-for-flush')
                yield self.env.process(self._write_back(m_vpn, tag))

                self._trans_page_locks.release_request(m_vpn, tp_req)
                self._trans_page_locks.locked_addrs.remove(m_vpn)


class MappingCache(FlashTransmitMixin, InsertMixin, LoadMixin, FlushMixin):
    """
    TODO: should separate operations that do/do not change recency
    """
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj,
            envobj, directory, mapping_on_flash, trans_page_locks):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj
        self.env = envobj
        self.directory = directory
        self.mapping_on_flash = mapping_on_flash

        self._lpn_table = LpnTableMvpn(confobj)

        self._trans_page_locks = trans_page_locks

        n_cache_tps = self.conf.n_cache_entries / self.conf.n_mapping_entries_per_page
        capsize = max(n_cache_tps - 1, 2)
        print 'max number of tps in cache', n_cache_tps
        self._concurrent_trans_quota = simpy.Container(self.env, init=capsize,
                capacity=capsize)
        self._m_vpn_interface_lock = LockPool(self.env)

    def update_batch(self, mapping_dict, tag=None):
        for lpn, ppn in mapping_dict.items():
            yield self.env.process(self.update(lpn, ppn, tag))

    def update(self, lpn, ppn, tag=None):
        """
        All translation and update of the same m_vpn are serialized.
        """
        m_vpn = self.conf.lpn_to_m_vpn(lpn)
        req = self._m_vpn_interface_lock.get_request(m_vpn)
        yield req

        if self._lpn_table.has_lpn(lpn):
            self.recorder.count_me('translation', 'overwrite-in-cache')
            self._lpn_table.overwrite_lpn(lpn, ppn, dirty=True)
        else:
            if self._lpn_table.n_free_rows() > 0:
                self.recorder.count_me('translation', 'insert-to-free')
                self._add_to_free(lpn, ppn)
            else:
                yield self.env.process(self._insert_new_mapping(lpn, ppn, tag))

        self._m_vpn_interface_lock.release_request(m_vpn, req)

    def lpns_to_ppns(self, lpns, tag=None):
        """
        If lpns are of the same m_vpn, this process will only
        have one cache miss.
        """
        ppns = []
        for lpn in lpns:
            ppn = yield self.env.process(self.lpn_to_ppn(lpn, tag))
            ppns.append(ppn)
        self.env.exit(ppns)

    def lpn_to_ppn(self, lpn, tag=None):
        """
        Note that the return can be UNINITIATED
        All translation and update of the same m_vpn are serialized.
        """
        m_vpn = self.conf.lpn_to_m_vpn(lpn)

        req = self._m_vpn_interface_lock.get_request(m_vpn)
        yield req

        ppn = self._lpn_table.lpn_to_ppn(lpn)
        if ppn == MISS:
            loaded, ppn = yield self.env.process(
                self._load_missing(m_vpn, wanted_lpn=lpn, tag=tag))
            assert ppn != MISS
        else:
            loaded = False

        if loaded == True:
            self.recorder.count_me("Mapping_Cache", "miss")
        else:
            self.recorder.count_me("Mapping_Cache", "hit")

        self._m_vpn_interface_lock.release_request(m_vpn, req)
        self.env.exit(ppn)

    def flush(self):
        yield self.env.process(self._flush())

    def drop(self):
        "flush before dropping, otherwise mapping will be lost"
        for lpn, row in self._lpn_table.least_to_most_lpn_items():
            self.recorder.count_me('translation', 'delete-lpn-in-table-for-drop')
            self._lpn_table.delete_lpn_and_lock(lpn)
            row.state = FREE

    def _victim_row(self, avoid_m_vpns):
        for lpn, row in self._lpn_table.least_to_most_lpn_items():
            if row.state == USED:
                m_vpn = self.conf.lpn_to_m_vpn(lpn)
                if not m_vpn in avoid_m_vpns:
                    return row
        raise RuntimeError("Cannot find a victim. Current stats: {}"\
                ", avoid_m_vpns: {}.\n"
                .format(str(self._lpn_table.stats()), avoid_m_vpns))


FREE, FREE_AND_LOCKED, USED, USED_AND_LOCKED, USED_AND_HOLD = \
        'FREE', 'FREE_AND_LOCKED', 'USED', 'USED_AND_LOCKED', 'USED_AND_HOLD'

class LpnTable(object):
    def __init__(self, n_rows):
        self._n_rows = n_rows

        self._rows = self._fresh_rows()

        # lpns to Row instances, it is a dict
        # {lpn1: row1, lpn2: row2, ...}
        # self._lpn_to_row = SegmentedLruCache(n_rows, 0.5)
        # self._lpn_to_row = LruDict()
        self._lpn_to_row = LruCache()

    def _fresh_rows(self):
         return [
            Row(lpn = None, ppn = None, dirty = False, state = FREE, rowid = i)
            for i in range(self._n_rows) ]

    def rows(self):
        return self._rows

    def _count_states(self):
        """
        slower but reduces duplication
        """
        counter = Counter()
        for row in self._rows:
            counter[row.state] += 1
        return counter

    def n_free_rows(self):
        return self._count_states()[FREE]

    def n_locked_free_rows(self):
        return self._count_states()[FREE_AND_LOCKED]

    def n_used_rows(self):
        return self._count_states()[USED]

    def n_locked_used_rows(self):
        return self._count_states()[USED_AND_LOCKED]

    def lock_free_row(self):
        """FREE TO FREE_AND_LOCKED"""
        for row in self._rows:
            if row.state == FREE:
                row.state = FREE_AND_LOCKED
                return row.rowid
        return None

    def lock_used_rows(self, row_ids):
        for row_id in row_ids:
            self.lock_used_row(row_id)

    def lock_used_row(self, row_id):
        row = self._rows[row_id]
        row.state = USED_AND_LOCKED

    def unlock_used_rows(self, row_ids):
        for row_id in row_ids:
            self.unlock_used_row(row_id)

    def unlock_used_row(self, row_id):
        row = self._rows[row_id]
        row.state = USED

    def lock_free_rows(self, n):
        row_ids = []
        got = 0
        for row in self._rows:
            if row.state == FREE:
                row.state = FREE_AND_LOCKED
                row_ids.append(row.rowid)
                got += 1

                if got == n:
                    break
        return row_ids

    def unlock_free_row(self, rowid):
        """FREE_AND_LOCKED -> FREE"""
        row = self._rows[rowid]
        row.state = FREE

    def unlock_free_rows(self, row_ids):
        for row_id in row_ids:
            self.unlock_free_row(row_id)

    def lock_lpn(self, lpn):
        row = self._lpn_to_row.peek(lpn)
        row.state = USED_AND_LOCKED

    def unlock_lpn(self, lpn):
        row = self._lpn_to_row.peek(lpn)
        assert row.state == USED_AND_LOCKED
        row.state = USED

    def hold_used_row(self, rowid):
        row = self._rows[rowid]
        row.state = USED_AND_HOLD

    def hold_used_rows(self, row_ids):
        for rowid in row_ids:
            self.hold_used_row(rowid)

    def unhold_used_row(self, rowid):
        row = self._rows[rowid]
        row.state = USED

    def unhold_used_rows(self, row_ids):
        for rowid in row_ids:
            self.unhold_used_row(rowid)

    def add_lpns(self, row_ids, mapping_dict, dirty, as_least_recent = False):
        assert len(row_ids) == len(mapping_dict), \
                "{} == {}".format(len(row_ids), len(mapping_dict))
        for row_id, (lpn, ppn) in zip(row_ids, mapping_dict.items()):
            self.add_lpn(row_id, lpn, ppn, dirty, as_least_recent)

    def add_lpn(self, rowid, lpn, ppn, dirty, as_least_recent = False):
        assert self.has_lpn(lpn) == False, "lpn is {}.".format(lpn)

        row = self._rows[rowid]

        row.lpn = lpn
        row.ppn = ppn
        row.dirty = dirty
        row.state = USED

        if as_least_recent:
            self._lpn_to_row.add_as_least_used(lpn, row)
        else:
            self._lpn_to_row[lpn] = row

    def lpn_to_ppn(self, lpn):
        try:
            row = self._lpn_to_row[lpn]
        except KeyError:
            return MISS
        else:
            return row.ppn

    def mark_clean_multiple(self, lpns):
        for lpn in lpns:
            self.mark_clean(lpn)

    def mark_clean(self, lpn):
        row = self._lpn_to_row.peek(lpn)
        assert row.state in (USED, USED_AND_HOLD)
        row.dirty = False

    def overwrite_lpn(self, lpn, ppn, dirty):
        row = self._lpn_to_row[lpn]
        row.lpn = lpn
        row.ppn = ppn
        row.dirty = dirty

    def is_dirty(self, lpn):
        row = self._lpn_to_row.peek(lpn)
        return row.dirty

    def row_state(self, rowid):
        return self._rows[rowid].state

    def delete_lpn_and_lock(self, lpn):
        assert self.has_lpn(lpn)
        row = self._lpn_to_row.peek(lpn)
        assert row.state == USED
        del self._lpn_to_row[lpn]
        row.clear_data()
        row.state = FREE_AND_LOCKED

        return row.rowid

    def has_lpn(self, lpn):
        try:
            row = self._lpn_to_row.peek(lpn)
        except KeyError:
            return False
        else:
            return True

    def stats(self):
        return self._count_states()


class LpnTableMvpn(LpnTable):
    """
    With addition supports related to m_vpn
    """
    def __init__(self, conf):
        super(LpnTableMvpn, self).__init__(conf.n_cache_entries)
        self.conf = conf

    def least_to_most_lpn_items(self):
        return self._lpn_to_row.least_to_most_items()

    def needed_space_for_m_vpn(self, m_vpn):
        cached_mappings = self.get_m_vpn_mappings(m_vpn)
        return self.conf.n_mapping_entries_per_page - len(cached_mappings)

    def get_m_vpn_mappings(self, m_vpn):
        """ return all the mappings of m_vpn that are in cache
        """
        rows = self._rows_of_m_vpn(m_vpn)
        mapping_dict = {}
        for row in rows:
            mapping_dict[row.lpn] = row.ppn

        return mapping_dict

    def row_ids_of_m_vpn(self, m_vpn):
        rows = self._rows_of_m_vpn(m_vpn)

        row_ids = []
        for row in rows:
            row_ids.append(row.rowid)

        return row_ids

    def _rows_of_m_vpn(self, m_vpn):
        lpns = self.conf.m_vpn_to_lpns(m_vpn)
        rows = []
        for lpn in lpns:
            try:
                row = self._lpn_to_row.peek(lpn)
            except KeyError:
                pass
            else:
                rows.append(row)

        return rows

    def get_un_cached_lpn_of_m_vpn(self, m_vpn):
        lpns = self.conf.m_vpn_to_lpns(m_vpn)
        cached_dict = self.get_m_vpn_mappings(m_vpn)
        uncached_lpns = set(lpns) - set(cached_dict.keys())
        return uncached_lpns


class _Row(object):
    def __init__(self, lpn, ppn, dirty, state, rowid):
        self.lpn = lpn
        self.ppn = ppn
        self.dirty = dirty
        self.state = state
        self.rowid = rowid

    def clear_data(self):
        self.lpn = None
        self.ppn = None
        self.dirty = None


class Row(object):
    def __init__(self, lpn, ppn, dirty, state, rowid):
        self._lpn = lpn
        self._ppn = ppn
        self._dirty = dirty
        self._state = state
        self._rowid = rowid

    def _assert_modification_allowed(self):
         assert self._state in (FREE_AND_LOCKED, USED, USED_AND_HOLD), \
                "current state {}".format(self._state)

    @property
    def lpn(self):
        return self._lpn

    @lpn.setter
    def lpn(self, lpn):
        self._assert_modification_allowed()
        self._lpn = lpn

    @property
    def ppn(self):
        return self._ppn

    @ppn.setter
    def ppn(self, ppn):
        self._assert_modification_allowed()
        self._ppn = ppn

    @property
    def dirty(self):
        return self._dirty

    @dirty.setter
    def dirty(self, dirty):
        self._assert_modification_allowed()
        self._dirty = dirty

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state_value):
        """
        State graph:
            FREE <----> FREE & LOCKED <----> USED <----> USED & LOCKED
                                              ^
                                              |
                                              v
                                        USED & HOLD
        """
        # check state transition
        if state_value == FREE:
            assert self._state == FREE_AND_LOCKED, \
                    "current state {}".format(self._state)
        elif state_value == FREE_AND_LOCKED:
            assert self._state in (FREE, USED), \
                    "current state {}".format(self._state)
        elif state_value == USED:
            assert self._state in (FREE_AND_LOCKED, USED_AND_LOCKED, USED_AND_HOLD), \
                    "current state {}".format(self._state)
        elif state_value == USED_AND_LOCKED:
            assert self._state == USED, \
                    "current state {}".format(self._state)
        elif state_value == USED_AND_HOLD:
            assert self._state == USED, \
                    "current state {}".format(self._state)
        else:
            raise RuntimeError("{} is not a valid state".format(state_value))
        self._state = state_value

    @property
    def rowid(self):
        return self._rowid

    def clear_data(self):
        self.lpn = None
        self.ppn = None
        self.dirty = None

    def __repr__(self):
        return "lpn:{}, ppn:{}, dirty:{}, rowid:{}".format(self.lpn,
            self.ppn, self.dirty, self._rowid)


class CacheEntryData(object):
    """
    This is a helper class that store entry data for a LPN
    """
    def __init__(self, lpn, ppn, dirty):
        self.lpn = lpn
        self.ppn = ppn
        self.dirty = dirty

    def __repr__(self):
        return "lpn:{}, ppn:{}, dirty:{}".format(self.lpn,
            self.ppn, self.dirty)


class MappingOnFlash(object):
    """
    This mapping table is for data pages, not for translation pages.
    GMT should have entries as many as the number of pages in flash
    """
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not conf.Config. it is {}".
               format(type(confobj).__name__))

        self.conf = confobj

        self.n_entries_per_page = self.conf.n_mapping_entries_per_page

        # do the easy thing first, if necessary, we can later use list or
        # other data structure
        self.entries = {}

    def lpn_to_ppn(self, lpn):
        """
        GMT should always be able to answer query. It is perfectly OK to return
        None because at the beginning there is no mapping. No valid data block
        on device.
        """
        return self.entries.get(lpn, UNINITIATED)

    def update(self, lpn, ppn):
        self.entries[lpn] = ppn

    def batch_update(self, mapping_dict):
        for lpn, ppn in mapping_dict.items():
            self.update(lpn, ppn)

    def lpns_to_ppns(self, lpns):
        d = MappingDict()
        for lpn in lpns:
            d[lpn] = self.lpn_to_ppn(lpn)

        return d

    def __repr__(self):
        return "global mapping table: {}".format(repr(self.entries))


class GlobalTranslationDirectory(object):
    """
    This is an in-memory data structure. It is only for book keeping. It used
    to remeber thing so that we don't lose it.
    """
    def __init__(self, confobj, oob, block_pool):
        self.conf = confobj

        self.flash_npage_per_block = self.conf.n_pages_per_block
        self.flash_num_blocks = self.conf.n_blocks_per_dev
        self.flash_page_size = self.conf.page_size
        self.total_pages = self.conf.total_num_pages()
        self.oob = oob
        self.block_pool = block_pool

        self.n_entries_per_page = self.conf.n_mapping_entries_per_page

        # M_VPN -> M_PPN
        # Virtual translation page number --> Physical translation page number
        # Dftl should initialize
        self.mapping = {}

        self._initialize()

    def _initialize(self):
        """
        This function initialize global translation directory. We assume the
        GTD is very small and stored in flash before mounting. We also assume
        that the global mapping table has been prepared by the vendor, so there
        is no other overhead except for reading the GTD from flash. Since the
        overhead is very small, we ignore it.
        """
        total_pages = self.conf.total_translation_pages()

        # use some free blocks to be translation blocks
        tmp_blk_mapping = {}
        for m_vpn in range(total_pages):
            m_ppn = self.block_pool.next_translation_page_to_program()
            # Note that we don't actually read or write flash
            self.add_mapping(m_vpn=m_vpn, m_ppn=m_ppn)
            # update oob of the translation page
            self.oob.relocate_trans_page(m_vpn=m_vpn, old_ppn=UNINITIATED,
                new_ppn=m_ppn, update_time=True)

    def m_vpn_to_m_ppn(self, m_vpn):
        """
        m_vpn virtual translation page number. It should always be successfull.
        """
        return self.mapping[m_vpn]

    def add_mapping(self, m_vpn, m_ppn):
        if self.mapping.has_key(m_vpn):
            raise RuntimeError("self.mapping already has m_vpn:{}"\
                .format(m_vpn))
        self.mapping[m_vpn] = m_ppn

    def update_mapping(self, m_vpn, m_ppn):
        self.mapping[m_vpn] = m_ppn

    def remove_mapping(self, m_vpn):
        del self.mapping[m_vpn]

    def lpn_to_m_ppn(self, lpn):
        m_vpn = self.conf.lpn_to_m_vpn(lpn)
        m_ppn = self.m_vpn_to_m_ppn(m_vpn)
        return m_ppn

    def __repr__(self):
        return repr(self.mapping)

class WearLevelingVictimBlocks(object):
    TYPE_DATA = 'TYPE_DATA'
    TYPE_TRANS = 'TYPE_TRANS'
    def __init__(self, conf, block_pool, oob, n_victims):
        self._conf = conf
        self._block_pool = block_pool
        self._oob = oob
        self.n_victims = n_victims

    def iterator_verbose(self):
        """
        Pick the 10% least erased USED Blocks
        """
        erasure_cnt = self._block_pool.get_erasure_count()
        least_used_blocks = reversed(erasure_cnt.most_common())

        used_data_blocks = self._block_pool.data_usedblocks
        used_trans_blocks = self._block_pool.trans_usedblocks

        self._block_pool.remove_full_cur_blocks()
        cur_blocks = self._block_pool.current_blocks()

        # we need used data or trans block
        victim_cnt = 0
        for blocknum, count in least_used_blocks:
            if blocknum in cur_blocks:
                # skip current blocks
                # continue
                pass

            valid_ratio = self._oob.states.block_valid_ratio(blocknum)
            if blocknum in used_data_blocks:
                yield valid_ratio, self.TYPE_DATA, blocknum
                victim_cnt += 1
            elif blocknum in used_trans_blocks:
                yield valid_ratio, self.TYPE_TRANS, blocknum
                victim_cnt += 1

            if victim_cnt >= self.n_victims:
                break


class VictimBlocks(object):
    TYPE_DATA = 'TYPE_DATA'
    TYPE_TRANS = 'TYPE_TRANS'
    def __init__(self, conf, block_pool, oob):
        self._conf = conf
        self._block_pool = block_pool
        self._oob = oob

    def iterator(self):
        for (_, _, block_num) in self.iterator_verbose():
            yield block_num

    def __str__(self):
        return repr(list(self.iterator_verbose()))

    def iterator_verbose(self):
        candidate_tuples = self._candidate_priorityq()
        while True:
            try:
                valid_ratio, block_type, block_num = heapq.heappop(candidate_tuples)
                yield valid_ratio, block_type, block_num
            except IndexError:
                # Out of victim blocks
                raise StopIteration

    def get_valid_ratio_counter_of_used_blocks(self):
        used_blocks = self._block_pool.used_blocks
        counter = Counter()
        for block in used_blocks:
            valid_ratio = self._oob.states.block_valid_ratio(block)
            ratio_str = "{0:.2f}".format(valid_ratio)
            counter[ratio_str] += 1
        return counter

    def _candidate_priorityq(self):
        candidate_tuples = self._victim_candidates()
        heapq.heapify(candidate_tuples)
        return candidate_tuples

    def _form_tuples(self, used_blocks, block_type):
        assert block_type in (self.TYPE_DATA, self.TYPE_TRANS)

        self._block_pool.remove_full_cur_blocks()
        cur_blocks = self._block_pool.current_blocks()

        victim_candidates = []
        for block in used_blocks:
            if block in cur_blocks:
                # skip current blocks
                continue

            valid_ratio = self._oob.states.block_valid_ratio(block)
            if valid_ratio == 1:
                # skip all-valid blocks
                continue
            if valid_ratio > self._conf['max_victim_valid_ratio']:
                # If valid ratio is too big, moving it does not provide
                # too much benefit.
                continue

            victim_candidates.append( (valid_ratio, block_type, block) )

        return victim_candidates

    def _victim_candidates(self):
        used_data_blocks = self._block_pool.data_usedblocks
        used_trans_blocks = self._block_pool.trans_usedblocks

        candidate_tuples = self._form_tuples(used_data_blocks, self.TYPE_DATA) + \
            self._form_tuples(used_trans_blocks, self.TYPE_TRANS)

        return candidate_tuples


class Cleaner(object):
    def __init__(self, conf, flash, oob, block_pool, mappings, directory, rec,
            env, trans_page_locks):
        self.conf = conf
        self.flash = flash
        self.oob = oob
        self.block_pool = block_pool
        self.mappings = mappings
        self.directory = directory
        self.recorder = rec
        self.env = env

        self.assert_threshold_sanity()

        self._trans_page_locks = trans_page_locks

        self._datablockcleaner = DataBlockCleaner(
            conf = self.conf,
            flash = self.flash,
            oob = self.oob,
            block_pool = self.block_pool,
            mappings = self.mappings,
            rec = self.recorder,
            env = self.env
            )

        self._transblockcleaner = TransBlockCleaner(
            conf = self.conf,
            flash = self.flash,
            oob = self.oob,
            block_pool = self.block_pool,
            mappings = self.mappings,
            directory = self.directory,
            rec = self.recorder,
            env = self.env,
            trans_page_locks = self._trans_page_locks
            )

        # limit number of cleaner processes
        # self.n_cleaners = self.conf.n_channels_per_dev * 64
        self.n_cleaners = self.conf['n_gc_procs']
        print 'n_cleaners:', self.n_cleaners
        self._block_cleaner_res = simpy.Resource(self.env, capacity=self.n_cleaners)

        self.n_victim_per_batch = self.conf.n_channels_per_dev * 2

        # only allow one cleaner instance at a time
        self._cleaner_res = simpy.Resource(self.env, capacity=1)

        self.gc_time_recorded = False

    def assert_threshold_sanity(self):
        if self.conf['do_not_check_gc_setting'] is True:
            return

        # check high threshold
        # TODO: this check is not sufficient. We may use more pages
        # than the file system size because the translation page overhead
        min_high = 1 / float(self.conf.over_provisioning)
        if self.conf.GC_high_threshold_ratio < min_high:
            raise RuntimeError("GC_high_threshold_ratio is too low. "\
                "When file system is full, we will consistently try to "\
                "collect garbage. Too much overhead.")

        # check minimum spare blocks
        # we need some free blocks to use for GC
        n_spare_blocks = (1.0 - self.conf.GC_high_threshold_ratio) * \
                self.conf.n_blocks_per_dev
        if n_spare_blocks < 32:
            raise RuntimeError("We may not have spare blocks to use when "\
                    "cleaning (for translation page, etc.). # of spare blocks:" \
                    " {}.".format(n_spare_blocks))

    def is_cleaning_needed(self):
        return self.block_pool.used_ratio() > self.conf.GC_high_threshold_ratio

    def is_stopping_needed(self):
        return self.block_pool.used_ratio() < self.conf.GC_low_threshold_ratio

    def level_wear(self):
        """
        Move victim to a new location
        """
        req = self._cleaner_res.request()
        yield req

        print 'start wear leveling....'
        print self.block_pool.get_erasure_count_dist()

        victim_blocks = WearLevelingVictimBlocks(self.conf,
                self.block_pool, self.oob, 0.1 * self.conf.n_blocks_per_dev)

        all_victim_tuples = list(victim_blocks.iterator_verbose())
        batches = utils.group_to_batches(all_victim_tuples, self.n_victim_per_batch)

        for batch in batches:
            yield self.env.process(self._clean_batch(batch, purpose=PURPOSE_WEAR_LEVEL))

        print 'after wear leveling'
        print self.block_pool.get_erasure_count_dist()
        self._cleaner_res.release(req)

    def clean(self):
        """
        cleaning WILL start if you call this function. So make sure you check
        if you need cleaning before calling.
        """
        req = self._cleaner_res.request()
        yield req

        victim_blocks = VictimBlocks(self.conf, self.block_pool, self.oob)
        self.recorder.append_to_value_list('clean_func_valid_ratio_snapshot',
                victim_blocks.get_valid_ratio_counter_of_used_blocks())

        if self.gc_time_recorded == False:
            self.recorder.set_result_by_one_key('gc_trigger_timestamp',
                    self.env.now / float(SEC))
            self.gc_time_recorded = True
            print 'GC time recorded!........!'

        all_victim_tuples = list(victim_blocks.iterator_verbose())
        batches = utils.group_to_batches(all_victim_tuples, self.n_victim_per_batch)

        for batch in batches:
            if self.is_stopping_needed():
                break
            yield self.env.process(self._clean_batch(batch, purpose=PURPOSE_GC))

        self._cleaner_res.release(req)

    def _clean_batch(self, victim_tuples, purpose):
        procs = []
        for valid_ratio, block_type, block_num in victim_tuples:
            p = self.env.process(
                    self._clean_block(block_type, block_num, purpose))
            procs.append(p)

        yield simpy.AllOf(self.env, procs)

    def _clean_block(self, block_type, block_num, purpose):
        req = self._block_cleaner_res.request()
        yield req

        if block_type == VictimBlocks.TYPE_DATA:
            yield self.env.process(
                    self._datablockcleaner.clean(block_num, purpose))
        elif block_type == VictimBlocks.TYPE_TRANS:
            yield self.env.process(
                    self._transblockcleaner.clean(block_num, purpose))

        self._block_cleaner_res.release(req)


class DataBlockCleaner(object):
    """
    Note that this class does not maintain any private state.
    It is a set of methods to change other states.
    """
    def __init__(self, conf, flash, oob, block_pool, mappings, rec, env):
        self.conf = conf
        self.flash = flash
        self.oob = oob
        self.block_pool = block_pool
        self.mappings = mappings
        self.recorder = rec
        self.env = env

        self.gcid = 0

    def log(self, blocknum):
        if self.conf['write_gc_log'] is False:
            return

        valid_ratio = self.oob.states.block_valid_ratio(blocknum)
        if valid_ratio == 0:
            return

        ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
        for ppn in range(ppn_start, ppn_end):
            try:
                lpn = self.oob.ppn_to_lpn_or_mvpn(ppn)
            except KeyError:
                lpn = 'NA'

            self.recorder.write_file('gc.log',
                    gcid=self.gcid,
                    blocknum=blocknum,
                    lpn=lpn,
                    valid=self.oob.states.is_page_valid(ppn))
        self.gcid += 1

    def clean(self, blocknum, purpose = PURPOSE_GC):
        '''
        for each valid page, move it to another block
        invalidate pages in blocknum and erase block
        '''
        assert blocknum in self.block_pool.used_blocks
        # assert blocknum not in self.block_pool.current_blocks()

        self.log(blocknum)

        ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
        for ppn in range(ppn_start, ppn_end):
            if self.oob.states.is_page_valid(ppn):
                yield self.env.process(self._clean_page(ppn, purpose))

        yield self.env.process(
            self.flash.erase_pbn_extent(blocknum, 1,
                tag=self.recorder.get_tag('erase.data.gc', None)))
        self.recorder.count_me("gc", "erase.data.block")

        self.oob.erase_block(blocknum)
        self.block_pool.move_used_data_block_to_free(blocknum)

    def _clean_page(self, ppn, purpose):
        """
        read ppn, write to new ppn, update metadata
        """
        assert self.oob.states.is_page_valid(ppn) is True

        if purpose == PURPOSE_GC:
            self.recorder.count_me("gc", "user.page.moves")
        elif purpose == PURPOSE_WEAR_LEVEL:
            self.recorder.count_me("wearleveling", "user.page.moves")

        yield self.env.process(
            self.flash.rw_ppn_extent(ppn, 1, 'read',
                tag=self.recorder.get_tag('read.data.gc', None)))

        if purpose == PURPOSE_GC:
            choice = LEAST_ERASED
        elif purpose == PURPOSE_WEAR_LEVEL:
            choice = MOST_ERASED
        new_ppn = self.block_pool.next_gc_data_page_to_program(choice)

        yield self.env.process(
            self.flash.rw_ppn_extent(new_ppn, 1, 'write',
                tag=self.recorder.get_tag('write.data.gc', None)))

        lpn = self.oob.ppn_to_lpn_or_mvpn(ppn)

        # mappings in cache
        yield self.env.process(
            self.mappings.update(lpn=lpn, ppn=new_ppn, tag=None))

        # mappings on flash
        # handled by self.mappings

        # translation directory
        # handled by self.mappings

        # oob state
        self.oob.relocate_data_page(lpn=lpn, old_ppn=ppn, new_ppn=new_ppn,
                update_time=False)

        # oob ppn->lpn/vpn
        # handled above

        # blockpool
        # handled by next_gc_data_page_to_program


class TransBlockCleaner(object):
    """
    Note that this class does not maintain any private state.
    It is a set of methods to change other states.
    TODO: some code is duplicated with DataBlockCleaner
    """
    def __init__(self, conf, flash, oob, block_pool, mappings, directory, rec,
            env, trans_page_locks):
        self.conf = conf
        self.flash = flash
        self.oob = oob
        self.block_pool = block_pool
        self.mappings = mappings
        self.directory = directory
        self.recorder = rec
        self.env = env
        self._trans_page_locks = trans_page_locks

    def clean(self, blocknum, purpose = PURPOSE_GC):
        assert blocknum in self.block_pool.used_blocks
        # assert blocknum not in self.block_pool.current_blocks()

        ppn_start, ppn_end = self.conf.block_to_page_range(blocknum)
        for ppn in range(ppn_start, ppn_end):
            if self.oob.states.is_page_valid(ppn):
                yield self.env.process(self._clean_page(ppn, purpose))

        yield self.env.process(
            self.flash.erase_pbn_extent(blocknum, 1,
                tag=self.recorder.get_tag('erase.trans.gc', None)))
        self.recorder.count_me("gc", "erase.trans.block")

        self.oob.erase_block(blocknum)
        self.block_pool.move_used_trans_block_to_free(blocknum)

    def _clean_page(self, ppn, purpose):
        assert self.oob.states.is_page_valid(ppn) is True

        if purpose == PURPOSE_GC:
            self.recorder.count_me("gc", "trans.page.moves")
        elif purpose == PURPOSE_WEAR_LEVEL:
            self.recorder.count_me("wearleveling", "trans.page.moves")

        m_vpn = self.oob.ppn_to_lpn_or_mvpn(ppn)

        tp_req = self._trans_page_locks.get_request(m_vpn)
        yield tp_req
        self._trans_page_locks.locked_addrs.add(m_vpn)

        yield self.env.process(
            self.flash.rw_ppn_extent(ppn, 1, 'read',
                tag=self.recorder.get_tag('read.trans.gc', None)))

        if purpose == PURPOSE_GC:
            choice = LEAST_ERASED
        elif purpose == PURPOSE_WEAR_LEVEL:
            choice = MOST_ERASED
        new_ppn = self.block_pool.next_gc_translation_page_to_program(choice)

        yield self.env.process(
            self.flash.rw_ppn_extent(new_ppn, 1, 'write',
                tag=self.recorder.get_tag('write.trans.gc', None)))


        # mappings in cache
        # mapping cache is only for data pages, so we don't need to update

        # mappings on flash
        # this is for data pages only

        # translation directory
        self.directory.update_mapping(m_vpn=m_vpn, m_ppn=new_ppn)

        # oob state
        self.oob.relocate_trans_page(m_vpn=m_vpn, old_ppn=ppn, new_ppn=new_ppn,
                update_time=False)

        # oob ppn->lpn/vpn
        # handled above

        # blockpool
        # handled by next_gc_trans_page_to_program

        self._trans_page_locks.release_request(m_vpn, tp_req)
        self._trans_page_locks.locked_addrs.remove(m_vpn)


class OutOfBandAreas(object):
    """
    It is used to hold page state and logical page number of a page.
    It is not necessary to implement it as list. But the interface should
    appear to be so.  It consists of page state (bitmap) and logical page
    number (dict).  Let's proivde more intuitive interfaces: OOB should accept
    events, and react accordingly to this event. The action may involve state
    and lpn_of_phy_page.
    """
    def __init__(self, confobj):
        self.conf = confobj

        self.flash_num_blocks = confobj.n_blocks_per_dev
        self.flash_npage_per_block = confobj.n_pages_per_block
        self.total_pages = self.flash_num_blocks * self.flash_npage_per_block

        # Key data structures
        self.states = FlashBitmap2(confobj)
        # ppn->lpn mapping stored in OOB, Note that for translation pages, this
        # mapping is ppn -> m_vpn
        self.ppn_to_lpn_mvpn = {}
        # Timestamp table PPN -> timestamp
        # Here are the rules:
        # 1. only programming a PPN updates the timestamp of PPN
        #    if the content is new from FS, timestamp is the timestamp of the
        #    LPN
        #    if the content is copied from other flash block, timestamp is the
        #    same as the previous ppn
        # 2. discarding, and reading a ppn does not change it.
        # 3. erasing a block will remove all the timestamps of the block
        # 4. so cur_timestamp can only be advanced by LBA operations
        self.timestamp_table = {}
        self.cur_timestamp = 0

        # flash block -> last invalidation time
        # int -> timedate.timedate
        self.last_inv_time_of_block = {}

    ############# Time stamp related ############
    def _incr_timestamp(self):
        """
        This function will advance timestamp
        """
        t = self.cur_timestamp
        self.cur_timestamp += 1
        return t

    def set_timestamp_of_ppn(self, ppn):
        self.timestamp_table[ppn] = self._incr_timestamp()

    def copy_timestamp(self, src_ppn, dst_ppn):
        self.timestamp_table[dst_ppn] = self.timestamp_table[src_ppn]

    def ppn_to_lpn_or_mvpn(self, ppn):
        return self.ppn_to_lpn_mvpn[ppn]

    def erase_block(self, flash_block):
        self.states.erase_block(flash_block)

        start, end = self.conf.block_to_page_range(flash_block)
        for ppn in range(start, end):
            try:
                del self.ppn_to_lpn_mvpn[ppn]
                # if you try to erase translation block here, it may fail,
                # but it is expected.
                del self.timestamp_table[ppn]
            except KeyError:
                pass

        try:
            del self.last_inv_time_of_block[flash_block]
        except KeyError:
            pass

    def relocate_data_page(self, lpn, old_ppn, new_ppn, update_time=True):
        self._relocate_page(virtual_pn=lpn, old_ppn=old_ppn, new_ppn=new_ppn,
                update_time=update_time)

    def relocate_trans_page(self, m_vpn, old_ppn, new_ppn, update_time=True):
        self._relocate_page(virtual_pn=m_vpn, old_ppn=old_ppn, new_ppn=new_ppn,
                update_time=update_time)

    def _relocate_page(self, virtual_pn, old_ppn, new_ppn, update_time=True):
        """
        mark the new_ppn as valid
        update the virtual page number in new page's OOB to virtual_pn
        invalidate the old_ppn, so cleaner can GC it
        """
        if update_time is True:
            self.set_timestamp_of_ppn(new_ppn)
        else:
            self.copy_timestamp(old_ppn, new_ppn)

        self.states.validate_page(new_ppn)
        self.ppn_to_lpn_mvpn[new_ppn] = virtual_pn

        if old_ppn != UNINITIATED:
            self.invalidate_ppn(old_ppn)

    def invalidate_ppns(self, ppns):
        for ppn in ppns:
            self.invalidate_ppn(ppn)

    def invalidate_ppn(self, ppn):
        self.states.invalidate_page(ppn)
        block, _ = self.conf.page_to_block_off(ppn)
        self.last_inv_time_of_block[block] = datetime.datetime.now()

    def validate_ppns(self, ppns):
        for ppn in ppns:
            self.validate_ppn(ppn)

    def validate_ppn(self, ppn):
        self.states.validate_page(ppn)

    def data_page_move(self, lpn, old_ppn, new_ppn):
        # move data page does not change the content's timestamp, so
        # we copy
        self.copy_timestamp(src_ppn = old_ppn, dst_ppn = new_ppn)
        self.relocate_data_page(lpn, old_ppn, new_ppn, update_time=False)

    def lpns_of_block(self, flash_block):
        s, e = self.conf.block_to_page_range(flash_block)
        lpns = []
        for ppn in range(s, e):
            lpns.append(self.ppn_to_lpn_mvpn.get(ppn, 'NA'))

        return lpns


class Config(config.ConfigNCQFTL):
    def __init__(self, confdic = None):
        super(Config, self).__init__(confdic)

        local_itmes = {
            # number of bytes per entry in mapping_on_flash
            "translation_page_entry_bytes": 4, # 32 bits
            "cache_entry_bytes": 8, # 4 bytes for lpn, 4 bytes for ppn
            "GC_high_threshold_ratio": 0.95,
            "GC_low_threshold_ratio": 0.9,
            "over_provisioning": 1.28, #TODO: this is not used
            "mapping_cache_bytes": None, # cmt: cached mapping table
            "do_not_check_gc_setting": False,
            "write_gc_log": True,
            }
        self.update(local_itmes)
        self['segment_bytes'] = 1*TB

        # self['keeping_all_tp_entries'] = False
        self['keeping_all_tp_entries'] = True

    def get_segment_id(self, lpn):
        return (lpn * self.page_size) / self['segment_bytes']

    @property
    def n_pages_per_segment(self):
        return self['segment_bytes'] / self.page_size

    @property
    def keeping_all_tp_entries(self):
        return self['keeping_all_tp_entries']

    @keeping_all_tp_entries.setter
    def keeping_all_tp_entries(self, value):
        self['keeping_all_tp_entries'] = value

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

    @over_provisioning.setter
    def over_provisioning(self, value):
        self['over_provisioning'] = value

    @property
    def GC_high_threshold_ratio(self):
        return self['GC_high_threshold_ratio']

    @GC_high_threshold_ratio.setter
    def GC_high_threshold_ratio(self, value):
        self['GC_high_threshold_ratio'] = value

    @property
    def GC_low_threshold_ratio(self):
        return self['GC_low_threshold_ratio']

    @GC_low_threshold_ratio.setter
    def GC_low_threshold_ratio(self, value):
        self['GC_low_threshold_ratio'] = value

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

    def lpn_to_m_vpn(self, lpn):
        return lpn / self.n_mapping_entries_per_page

    def m_vpn_to_lpns(self, m_vpn):
        start_lpn = m_vpn * self.n_mapping_entries_per_page
        return range(start_lpn, start_lpn + self.n_mapping_entries_per_page)

    def total_translation_pages(self):
        """
        total number of translation pages needed. It is:
        total_entries * entry size / page size
        """
        n_entries = self.total_num_pages()
        entry_bytes = self.translation_page_entry_bytes
        flash_page_size = self.page_size
        # play the ceiling trick
        return (n_entries * entry_bytes + \
                (flash_page_size -1)) / flash_page_size


def write_timeline(conf, recorder, op_id, op, arg, start_time, end_time):
    if conf.get('write_timeline', False) is True:
        recorder.write_file('timeline.txt',
            op_id = op_id, op = op, arg = arg,
            start_time = start_time, end_time = end_time)






