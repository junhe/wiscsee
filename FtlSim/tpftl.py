import os

import dftl2
from dftl2 import TRANS_CACHE, TRANS_CLEAN, DATA_USER, DATA_CLEANING
import lrulist
import recorder
import utils
import traceback


class EntryNode(object):
    def __init__(self, lpn, value, owner_list):
        self.lpn = lpn
        self.value = value # it may be ANYTHING, don't make assumption
        self.owner_list = owner_list
        self.timestamp = 0

    def __str__(self):
        return "lpn:{}({})".format(self.lpn, self.timestamp)

class EntryList(lrulist.LinkedList):
    def __init__(self, owner_page_node):
        super(EntryList, self).__init__()
        self.owner_page_node = owner_page_node

class PageNode(object):
    def __init__(self, m_vpn, owner_list):
        self.m_vpn = m_vpn
        self.entry_list = EntryList(owner_page_node = self)
        self.entry_table = {}
        self.owner_list = owner_list
        self.hotness = 0 # sum of timestamps of entry nodes

    def __str__(self):
        return "m_vpn:{}({})\n entrylist:{}".format(self.m_vpn, self.hotness,
            str(self.entry_list))

class PageNodeList(lrulist.LinkedList):
    pass

class TwoLevelMppingCache(object):
    """
    It implements the two level cache described in TPFTL paper EuroSys'15.
    You should use lrulist.Node as the node.
    You can assume the key is lpn, but you cannot assume anything about
    the value.

    Structure:
    page_node_list: node{m_vpn, entry_list:LinkedList, entry_table[], owner_list}...
                                            |
                                            |
                        entry_node{key:lpn, value:ANYTHING, owner_page_node}
                                            |
                                            |
                                        entry_node
                                            |
                                            |
                                        entry_node

    """
    def __init__(self, confobj):
        """
        Each node in toplist has an attribute entry_list and entry_table.
        entry_table is indexed by lpn.
        toplist is index by m_vpn.
        """
        self.conf = confobj

        self.page_node_list  = PageNodeList()
        self.page_node_table = {} # indexed by m_vpn

        self.page_node_bytes = self.conf['dftl']['tpftl']['page_node_bytes']
        self.entry_node_bytes = self.conf['dftl']['tpftl']['entry_node_bytes']

        self._timestamp = 0


    def timestamp(self):
        self._timestamp += 1
        return self._timestamp

    def _traverse_entry_nodes(self):
        for page_node in self.page_node_list:
            for entry_node in page_node.entry_list:
                yield entry_node

    def _traverse_page_nodes(self):
        for page_node in self.page_node_list:
            yield page_node

    def has_key(self, lpn):
        "check if lpn->ppn is in the cache"
        has_it, _ = self._get_entry_node(lpn)
        return has_it

    def _get_entry_node(self, lpn):
        "return tuple: (True, class Node) or (False, None)"
        m_vpn = self.conf.dftl_lpn_to_m_vpn(lpn)
        if self.page_node_table.has_key(m_vpn):
            page_node = self.page_node_table[m_vpn]
            if page_node.entry_table.has_key(lpn):
                return True, page_node.entry_table[lpn]

        return False, None

    def _add_entry_node(self, lpn, value):
        "lpn should not exist before calling this."
        m_vpn = self.conf.dftl_lpn_to_m_vpn(lpn)
        if not self.page_node_table.has_key(m_vpn):
            # add page node first
            self._add_empty_page_node(m_vpn)

        page_node = self.page_node_table[m_vpn]

        new_entry_node = self._add_entry_node_to_page_node(lpn,
            value, page_node)

        return new_entry_node

    def _overwrite_entry_node(self, lpn, value):
        "lpn must exist"
        _, entry_node = self._get_entry_node(lpn)
        entry_node.value = value

    def _add_entry_node_to_page_node(self, lpn, value, page_node):
        """
        This does not change the hotness of page node because the default is 0
        """
        entry_list = page_node.entry_list

        new_entry_node = EntryNode(lpn = lpn, value = value,
            owner_list = entry_list)
        entry_list.add_to_head(new_entry_node)
        page_node.entry_table[lpn] = new_entry_node

        return new_entry_node

    def _add_empty_page_node(self, m_vpn):
        "m_vpn should not exist in page_node_list before calling this"
        node = PageNode(m_vpn = m_vpn, owner_list = self.page_node_list)

        # add node to page_node_list
        self.page_node_table[m_vpn] = node
        self.page_node_list.add_to_head(node)

        return node

    def _hit(self, entry_node):
        """
        We hit entry node, move it up in entry_list, and move the page_node
        ahead
        """
        # move up in the entrylist
        entry_node.owner_list.move_to_head(entry_node)
        oldtimestamp = entry_node.timestamp
        entry_node.timestamp = self.timestamp()

        # move according to hotness
        page_node = entry_node.owner_list.owner_page_node
        page_node.hotness = page_node.hotness - oldtimestamp + \
            entry_node.timestamp
        self._adjust_by_hotness(page_node)

    def _adjust_by_hotness(self, page_node):
        """
        shift towards head util
        page_node.prev.hotness > page_node.hotness > page_node.next.hotness
        """
        while page_node != self.page_node_list.head() and \
            page_node.hotness > page_node.prev.hotness:
            self.page_node_list.move_toward_head_by_one(page_node)

        while page_node != self.page_node_list.tail() and \
            page_node.hotness < page_node.next.hotness:
            self.page_node_list.move_toward_tail_by_one(page_node)

    ############### APIs  ################
    def items(self):
        for entry_node in self._traverse_entry_nodes():
            yield entry_node.lpn, entry_node.value

    def __getitem__(self, lpn):
        "It affects order"
        has_it, entry_node = self._get_entry_node(lpn)
        if has_it:
            try:
                self._hit(entry_node)
            except RuntimeError:
                print 'inquired entry', entry_node
                print 'list', self.__str__()
                raise
            return entry_node.value
        else:
            raise KeyError

    def get(self, lpn, default = None):
        "It affects order"
        has_it, entry_node = self._get_entry_node(lpn)
        if has_it:
            return entry_node.value
        else:
            return default

    def peek(self, lpn):
        "It does NOT affects order"
        has_it, entry_node = self._get_entry_node(lpn)
        if has_it:
            return entry_node.value
        else:
            return default

    def __setitem__(self, lpn, value):
        "It affects order"
        has_it, entry_node = self._get_entry_node(lpn)
        if has_it == True:
            entry_node.value = value
            self._hit(entry_node)
        else:
            # need to add it
            new_entry_node = self._add_entry_node(lpn, value)
            self._hit(new_entry_node)

    def victim_key(self):
        tail_page_node = self.page_node_list.tail()
        if tail_page_node == None:
            return None

        tail_entry = tail_page_node.entry_list.tail()
        if tail_entry == None:
            return None
        else:
            return tail_entry.lpn

    def is_full(self):
        raise NotImplementedError

    def __delitem__(self, lpn):
        has_it, entry_node = self._get_entry_node(lpn)
        assert has_it == True

        entry_list = entry_node.owner_list
        page_node = entry_list.owner_page_node
        entry_timestamp = entry_node.timestamp

        entry_list.delete(entry_node)
        del page_node.entry_table[lpn]
        page_node.hotness -= entry_timestamp

        if len(entry_list) == 0:
            # this page node's entry list is empty
            # we need to remove the page node from page_node_list
            m_vpn = self.conf.dftl_lpn_to_m_vpn(lpn)
            del self.page_node_table[m_vpn]
            self.page_node_list.delete(page_node)
        else:
            self._adjust_by_hotness(page_node)

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        "Number of entries"
        total = 0
        for page_node in self.page_node_list:
            total += len(page_node.entry_list)

        return total

    def bytes(self):
        total = 0
        for page_node in self.page_node_list:
            total += self.page_node_bytes + \
                len(page_node.entry_list) * self.entry_node_bytes

        return total

    def show_hotness(self):
        ret = 'current timestamp:{}\n'.format(self._timestamp)
        ret += 'm_vpn\thotness\n'
        for page_node in self._traverse_page_nodes():
            ret += "{}\t{}\n".format(page_node.m_vpn, page_node.hotness)

        return ret

    def __str__(self):
        rep = ''
        for page_node in self.page_node_list:
            rep += 'page_node:' + str(page_node) + '\n'

        return rep


class CachedMappingTable(dftl2.CachedMappingTable):
    def __init__(self, confobj):
        self.conf = confobj

        self.max_bytes = self.conf['dftl']['max_cmt_bytes']
        self.lowest_n_entries = self.max_bytes / \
            self.conf['dftl']['tpftl']['page_node_bytes']

        print 'self.lowest_n_entries', self.lowest_n_entries

        self.entries = TwoLevelMppingCache(confobj)

    def victim_entry(self):
        classname = type(self.entries).__name__
        if classname in ('SegmentedLruCache', 'LruCache',
            'TwoLevelMppingCache'):
            lpn = self.entries.victim_key()
        else:
            raise RuntimeError("You need to specify victim selection")

        # lpn, Cacheentrydata
        return lpn, self.entries.peek(lpn)

    def is_full(self):
        return self.entries.bytes() >= self.max_bytes

class MappingManager(dftl2.MappingManager):
    def __init__(self, confobj, block_pool, flashobj, oobobj):
        "completely overwrite base class"
        self.conf = confobj

        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool

        # managed and owned by Mappingmanager
        self.global_mapping_table = dftl2.GlobalMappingTable(confobj, flashobj)
        # used CMT defined in this file (tpftl.py)
        self.cached_mapping_table = CachedMappingTable(confobj)
        self.directory = dftl2.GlobalTranslationDirectory(confobj)

    def lpn_to_ppn(self, lpn):
        """
        OVERRIDE dftl2
        This method does not fail. It will try everything to find the ppn of
        the given lpn.
        return: real PPN or UNINITIATED
        """
        # try cached mapping table first.
        ppn = self.cached_mapping_table.lpn_to_ppn(lpn)
        if ppn == dftl2.MISS:
            # cache miss
            if self.cached_mapping_table.is_full():
                self.evict_cache_entry()

            # find the physical translation page holding lpn's mapping in GTD
            ppn = self.load_mapping_entry_to_cache(lpn)

            self.flash.recorder.count_me("cache", "miss")
        else:
            self.flash.recorder.count_me("cache", "hit")

        return ppn

    def read_mapping_page(self, m_vpn):
        """
        NEW FUNCTION in tpftl
        This function returns all mapping in a translation page
        Note that this function does not add the mapping to cache. You need to
        do it elsewhere.
        """
        m_ppn = self.directory.m_vpn_to_m_ppn(m_vpn)

        # read it up, this operation is just for statistics
        self.flash.page_read(m_ppn, TRANS_CACHE)

        # now we return all the mappings of m_vpn
        mappings = {}
        for lpn in self.directory.m_vpn_to_lpns(m_vpn):
            ppn = self.global_mapping_table.lpn_to_ppn(lpn)
            mappings[lpn] = ppn

        return mappings

    def load_mapping_for_extent(self, start_lpn, npages):
        """
        NEW FUNCTION in tpftl
        """
        start_m_vpn = self.directory.m_vpn_of_lpn(start_lpn)
        end_m_vpn = self.directory.m_vpn_of_lpn(start_lpn + npages - 1)

        mappings = {}
        for m_vpn in range(start_m_vpn, end_m_vpn + 1):
            m = self.read_mapping_page(m_vpn)
            # add mappings in m_vpn to mappings
            mappings.update(m)

        # add only the needed to cache
        # the number of new entries should not exceed the ideal number of
        # entries of the cache, otherwise, it will evict some just-added
        # entries
        new_count = min(npages, self.cached_mapping_table.lowest_n_entries)
        # new_count = npages
        for lpn in range(start_lpn, start_lpn + new_count):
            ppn = mappings[lpn]
            # Note that the mapping may already exist in cache. If it does, we
            # don't add the just-loaded mapping to cache because the one in
            # cache is newer. If it's not in cache, we add, but we may
            # need to evict entry from cache to accomodate the new one.
            if not self.cached_mapping_table.entries.has_key(lpn):
                while self.cached_mapping_table.is_full():
                    self.evict_cache_entry()
                self.cached_mapping_table.add_new_entry(lpn = lpn, ppn = ppn,
                    dirty = False)

    def evict_cache_entry(self):
        """
        Select one entry in cache
        If the entry is dirty, write it back to GMT.
        If it is not dirty, simply remove it.
        """
        vic_lpn, vic_entrydata = self.cached_mapping_table.victim_entry()

        # if vic_entrydata.dirty == True:
            # # update every data structure related
            # self.update_entry(vic_lpn, vic_entrydata.ppn)

        batch_entries = self.cmt_entries_in_same_trans_page(vic_lpn)
        for entry in batch_entries:
            if entry.dirty == True:
                self.update_entry(entry.lpn, entry.ppn, tag=TRANS_CACHE)

        # remove only the victim entry
        # if self.flash.recorder.enabled:
            # print 'evict lpn', vic_lpn
        self.cached_mapping_table.remove_entry_by_lpn(vic_lpn)



class Tpftl(dftl2.Dftl):
    def __init__(self, confobj, recorderobj, flashobj):
        super(dftl2.Dftl, self).__init__(confobj, recorderobj, flashobj)

        # bitmap has been created parent class
        # Change: we now don't put the bitmap here
        # self.bitmap.initialize()
        del self.bitmap

        self.block_pool = dftl2.BlockPool(confobj)
        self.oob = dftl2.OutOfBandAreas(confobj)

        ###### the managers ######
        # Note that we are using Mappingmanager defined in tpftl.py
        self.mapping_manager = MappingManager(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = flashobj,
            oobobj=self.oob)

        self.garbage_collector = dftl2.GarbageCollector(
            confobj = self.conf,
            flashobj = flashobj,
            oobobj=self.oob,
            block_pool = self.block_pool,
            mapping_manager = self.mapping_manager
            )

        # We should initialize Globaltranslationdirectory in Dftl
        self.mapping_manager.initialize_mappings()

        self.gcstats = recorder.Recorder(output_target = recorder.FILE_TARGET,
            path = os.path.join(self.conf['result_dir'], 'gcstats.log'),
            verbose_level = 1)

    # @utils.debug_decor
    def read_range(self, lpn, npages):
        # prefetch mapping
        self.mapping_manager.load_mapping_for_extent(lpn, npages)

        for page in range(lpn, lpn + npages):
            self.lba_read(page)

    # @utils.debug_decor
    def write_range(self, lpn, npages):
        # prefetch mapping
        self.mapping_manager.load_mapping_for_extent(lpn, npages)

        for page in range(lpn, lpn + npages):
            self.lba_write(page)

    # @utils.debug_decor
    def discard_range(self, lpn, npages):
        # prefetch mapping
        self.mapping_manager.load_mapping_for_extent(lpn, npages)

        for page in range(lpn, lpn + npages):
            self.lba_discard(page)

def main(conf):
    cache = TwoLevelMppingCache(conf)
    cache[3] = 33
    cache[4] = 44
    cache[5] = 55
    cache[600] = 600600

    cache[1024] = 'page1024'

    cache[3] = 33

    cache[1025] = 'page1024'

    a = cache[5]
    a = cache[600]

    cache[3000] = 3000
    cache[3000] = 3000
    cache[3000] = 3000

    del cache[5]
    del cache[3]
    del cache[4]

    print cache


if __name__ == '__main__':
    main()


