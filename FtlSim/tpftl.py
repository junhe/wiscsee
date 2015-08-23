"""
************* Selective prefetching *************
The problem is that how it cooperates with request-level prefetching. Some
requests are large and may trigger selective prefetching.

The motivation of selective prefetching is that it may reduce the page reads
that request-level prefetching cannot reduce. Why? Because they request-level
prefetching only takes care of pages within a request, not between. So if you
have several consecutive requests, prefetching the first request only guarantee
the translation of the first request, not the second and thereafter. But if you
have selective prefetching, when you missed the first page of the second
request, selective prefetching may be activated and prefetching the mappings
for request 3, 4, 5, ... So request 3, 4, 5, ... don't even need request-level
prefetching (which needs to read flash pages).

To impelement it:

1. in mapping_manager.lpn_to_ppn(), if missed, check
cache.selective_prefetch_enabled, if enabled, disable it and start selective
prefetching.

2. to do selective prefetching, first we need to figure the prefetch length.
The length is p_len = min(consecutive predecessors of lpn, number of entries in
the coldest tp node). If lpn will be in the coldest TP node, it is OK to evict
its entry and prefetch to it. Because we need to read the translation page
anyway (lpn is missing).

3. evict p_len entries from the coldest tp node

4. read lpn's translation page and add lpn and lpn's p_len successors to tp
node

5. let the ftl use the cache to do the translation. For the next a few
translations, selective prefetching will not be triggered because there will
not be misses.

- mapping_manager.selective_prefetch_length(lpn)
- evict_n_from_tp_node(n, m_vpn)
- load_mapping_for_extent2(self, start_lpn, npages):
    - version 2 will raise an exception if the extent is across larger than
    one tp node. So you should make sure there npages free slots in cache


"""

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

        # For selective prefetching
        self._page_node_change = 0
        self._selective_threshold = \
            self.conf['dftl']['tpftl']['selective_threshold']
        # It can only be set True by page_node_change()
        # It can only be set False by the executor of selective prefetch
        # How to use it?
        #   if True: do selective prefetching; set it to false
        #   if false: do nothing
        self.selective_prefetch_enabled = False

        # self.localrec = recorder.Recorder(recorder.STDOUT_TARGET)
        # self.localrec.enable()

    @property
    def page_node_change(self):
        return self._page_node_change

    @page_node_change.setter
    def page_node_change(self, val):
        """
        It can only be increased/decreased by 1.
        """
        self._page_node_change = val

        # self.localrec.count_me("page_node_change", val)

        if self._page_node_change == self._selective_threshold:
            self.selective_prefetch_enabled = False
            # reset when reaching threshold
            self._page_node_change = 0

        if self._page_node_change == -self._selective_threshold:
            self.selective_prefetch_enabled = True
            # reset when reaching threshold
            self._page_node_change = 0

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

        # update page_node_change
        self.page_node_change += 1

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
        self._update_hotness(page_node)

        # page_node.owner_list.move_to_head(page_node)
        self._adjust_by_hotness(page_node)

    def _update_hotness(self, page_node):
        """
        Calculate hotness by averaging entry timestamp
        """
        total = 0
        cnt = 0
        for entry_node in page_node.entry_list:
            total += entry_node.timestamp
            cnt += 1
        page_node.hotness = (total / float(cnt)) if cnt > 0 else 0

        return page_node.hotness

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
            self._hit(entry_node)
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

    def peek(self, lpn, default = None):
        "It does NOT affect order"
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

    def _del_page_node(self, m_vpn):
        page_node = self.page_node_table[m_vpn]
        del self.page_node_table[m_vpn]
        self.page_node_list.delete(page_node)

        self.page_node_change -= 1

    def __delitem__(self, lpn):
        has_it, entry_node = self._get_entry_node(lpn)
        assert has_it == True

        entry_list = entry_node.owner_list
        page_node = entry_list.owner_page_node

        entry_list.delete(entry_node)
        del page_node.entry_table[lpn]

        if len(entry_list) == 0:
            # this page node's entry list is empty
            # we need to remove the page node from page_node_list
            m_vpn = self.conf.dftl_lpn_to_m_vpn(lpn)
            self._del_page_node(m_vpn)
        else:
            self._update_hotness(page_node)
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
    def __init__(self, confobj, recorderobj):
        self.conf = confobj
        self.recorder = recorderobj

        self.max_bytes = self.conf['dftl']['max_cmt_bytes']
        self.lowest_n_entries = self.max_bytes / \
            self.conf['dftl']['tpftl']['page_node_bytes']

        print 'self.lowest_n_entries', self.lowest_n_entries

        self.entries = TwoLevelMppingCache(confobj)

    def is_full(self):
        return self.entries.bytes() >= self.max_bytes

    def victim_entry(self):
        """
        Return the least used entry node in the coldest TP node
        """
        page_node = self.entries.page_node_list.tail()
        vic_entry_node = self.victim_entry_in_page_node(page_node)
        return vic_entry_node.lpn, vic_entry_node.value

    def victim_entry_in_page_node(self, page_node):
        """
        Return the victim entry in a particular page node
        """
        entry_list = page_node.entry_list
        last_entry = entry_list.tail()
        entry_node = last_entry
        while entry_node.value.dirty == True and \
            entry_node != entry_list.head():
            entry_node = entry_node.prev

        if entry_node.value.dirty == False:
            self.recorder.count_me('CMT', 'clean.victim')
            return entry_node
        else:
            self.recorder.count_me('CMT', 'last.victim')
            return last_entry


class MappingManager(dftl2.MappingManager):
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj):
        "completely overwrite base class"
        self.conf = confobj

        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj

        # managed and owned by Mappingmanager
        self.global_mapping_table = dftl2.GlobalMappingTable(confobj, flashobj)
        # used CMT defined in this file (tpftl.py)
        self.cached_mapping_table = CachedMappingTable(confobj, recorderobj)
        self.directory = dftl2.GlobalTranslationDirectory(confobj)

        self.entries_per_m_vpn = self.conf.dftl_n_mapping_entries_per_page()

    def lpn_range_of_m_vpn(self, m_vpn):
        start_lpn = self.entries_per_m_vpn * m_vpn
        end_lpn = start_lpn + self.entries_per_m_vpn
        return start_lpn, end_lpn

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

    def _get_vic_m_vpn(self, avoid_m_vpn):
        vic_lpn, _ = self.cached_mapping_table.victim_entry()
        vic_m_vpn = self.directory.m_vpn_of_lpn(vic_lpn)

        # try not to be the same m_vpn
        if vic_m_vpn == avoid_m_vpn:
            page_node = self.cached_mapping_table.entries.\
                page_node_table[vic_m_vpn]
            if page_node != self.cached_mapping_table.entries.page_node_list\
                .head():
                vic_m_vpn = page_node.prev.m_vpn
            elif page_node != self.cached_mapping_table.entries.page_node_list\
                .tail():
                vic_m_vpn = page_node.next.m_vpn
            else:
                raise RuntimeError(
                    "Maybe you should increase your mapping cache size")

        return vic_m_vpn

    def is_extent_in_cache(self, start_lpn, npages):
        """
        See if all mappings for the extent are in cache

        This function should not affect cache.
        """
        for lpn in range(start_lpn, start_lpn + npages):
            if not self.cached_mapping_table.entries.has_key(lpn):
                return False

        return True

    def load_mapping_for_extent(self, start_lpn, npages):
        """
        Try to load mappings for start_lpn, npages

        (start_lpn, npages) must be within the same m_vpn
        It is possible that less than npages are loaded

        NEW FUNCTION in tpftl
        """

        start_m_vpn = self.directory.m_vpn_of_lpn(start_lpn)
        end_m_vpn = self.directory.m_vpn_of_lpn(start_lpn + npages - 1)
        assert start_m_vpn == end_m_vpn

        mappings = {}
        for m_vpn in range(start_m_vpn, end_m_vpn + 1):
            m = self.read_mapping_page(m_vpn)
            # add mappings in m_vpn to mappings
            mappings.update(m)

        vic_m_vpn = None
        for lpn in range(start_lpn, start_lpn + npages):
            # Note that the mapping may already exist in cache. If it does, we
            # don't add the just-loaded mapping to cache because the one in
            # cache is newer. If it's not in cache, we add, but we may
            # need to evict entry from cache to accomodate the new one.
            if not self.cached_mapping_table.entries.has_key(lpn):
                while self.cached_mapping_table.is_full():
                    if vic_m_vpn == None:
                        vic_m_vpn = self._get_vic_m_vpn(
                            avoid_m_vpn = start_m_vpn)

                    evicted = self.evict_cache_entry_of_m_vpn(vic_m_vpn)
                    if evicted == False:
                        # nothing evicted, return the number of new mappings
                        # added
                        return lpn - start_lpn

                ppn = mappings[lpn]
                self.cached_mapping_table.add_new_entry(lpn = lpn, ppn = ppn,
                    dirty = False)

        # all mappings are loaded
        return npages

    def dirty_entries_of_translation_page(self, m_vpn):
        """
        Get all dirty entries in translation page m_vpn.
        """
        # iterating the entry list
        page_node = self.cached_mapping_table.entries.page_node_table.get(
            m_vpn, None)

        if page_node == None:
            return []
        else:
            retlist = []
            for entry_node in page_node.entry_list:
                retlist.append(entry_node.value)

            return retlist

    def selective_prefetch_length(self, lpn):
        """
        Decide how long we need to prefetch

        lpn is the entry that we miss.
        The length is p_len = min(consecutive predecessors of lpn,
            max number of successors in the same tp node with lpn,
            number of entries in the coldest TP node).
        """
        m_vpn = self.directory.m_vpn_of_lpn(lpn)

        # Consecutive predecessors
        entry_table = self.cached_mapping_table.entries.\
            page_node_table[m_vpn].entry_table
        pred_lpn = lpn - 1
        consec_len = 1 # at least we have lpn
        while entry_table.has_key(pred_lpn):
            # complexity O(nlogn), same as sorting
            consec_len += 1
            pred_lpn -= 1

        # max number of successors in the same tp node with lpn
        # this is to ensure the prefetch does not go over one TP
        next_start = (m_vpn + 1) * self.entries_per_m_vpn
        max_successors = next_start - lpn

        # number of entries in the coldes tp node
        n_entries = len(self.cached_mapping_table.entries.page_node_list\
            .tail().entry_list)

        # if lpn == 3642:
            # import pdb; pdb.set_trace()
        return min(consec_len, max_successors, n_entries)

    def evict_cache_entry_of_m_vpn(self, m_vpn, n = 1):
        """
        Evict n entries from a particular m_vpn

        m_vpn: the TP node to evict from
        n: the number of entries to evict

        Return the number of evicted entries

        This is a new function of TPFTL.
        """
        self.recorder.count_me('cache', 'evict_cache_entry_of_m_vpn')

        page_node = self.cached_mapping_table.entries.page_node_table.get(
                m_vpn, None)

        if page_node == None:
            return 0

        n_left = n
        while n_left > 0 and self.cached_mapping_table.entries.page_node_table\
            .has_key(m_vpn):
            vic_entry_node = self.cached_mapping_table\
                .victim_entry_in_page_node(page_node)
            if vic_entry_node.value.dirty == True:
                # after this, noboy in the tp node will be dirty
                # so we will not enter here again
                # if there is no dirty entry in the tp node, we never enter
                # here
                self.batch_write_back(m_vpn)
            self.cached_mapping_table.remove_entry_by_lpn(vic_entry_node.lpn)

            n_left -= 1

        return n - n_left

    def selective_prefetch(self, lpn):
        """
        lpn: the lpn in the last cache miss.

        You can only do this when cache.selective_prefetch_enabled == True
        """
        m_vpn = self.directory.m_vpn_of_lpn(lpn)

        # prefetch length includes lpn itself
        pref_len =  self.selective_prefetch_length(lpn)
        vic_m_vpn = self.cached_mapping_table.entries.page_node_list.tail()\
            .m_vpn
        evicted = self.evict_cache_entry_of_m_vpn(vic_m_vpn, pref_len)
        # assert pref_len == evicted

        self.load_mapping_for_extent(lpn, pref_len)

    def lpn_to_ppn(self, lpn):
        """
        This method does not fail. It will try everything to find the ppn of
        the given lpn.
        return: real PPN or UNINITIATED
        """
        # try cached mapping table first.
        ppn = self.cached_mapping_table.lpn_to_ppn(lpn)
        if ppn == dftl2.MISS:
            # Cache miss
            # Use selective prefetching
            self.selective_prefetch(lpn)

            # now it should have it
            ppn = self.cached_mapping_table.lpn_to_ppn(lpn)

            self.recorder.count_me("cache", "miss")
        else:
            self.recorder.count_me("cache", "hit")

        return ppn


class Tpftl(dftl2.Dftl):
    """
    Thu 06 Aug 2015 06:17:14 PM CDT
    ***** Request-level prefetching *****
    1 find out the extent of (lpn L, last lpn of L's TP node)
    2 prefetch this extent
    3 access th extent
    3 update the remaining request
    4 go to 1

    """
    def __init__(self, confobj, recorderobj, flashobj):
        # Note that this calls grandpa
        super(dftl2.Dftl, self).__init__(confobj, recorderobj, flashobj)

        self.block_pool = dftl2.BlockPool(confobj)
        self.oob = dftl2.OutOfBandAreas(confobj)

        ###### the managers ######
        # Note that we are using Mappingmanager defined in tpftl.py
        self.mapping_manager = MappingManager(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = flashobj,
            oobobj=self.oob,
            recorderobj = recorderobj
            )

        self.garbage_collector = dftl2.GarbageCollector(
            confobj = self.conf,
            flashobj = flashobj,
            oobobj=self.oob,
            block_pool = self.block_pool,
            mapping_manager = self.mapping_manager,
            recorderobj = recorderobj
            )

        # We should initialize Globaltranslationdirectory in Dftl
        self.mapping_manager.initialize_mappings()

    def split_request(self, lpn, npages):
        """
        Split the request to multiple subrequest aligned with tp nodes
        """
        total_pages = npages
        while total_pages > 0:
            sub_lpn, sub_npages = self.subrequest(lpn, total_pages)

            lpn += sub_npages
            total_pages -= sub_npages

            yield sub_lpn, sub_npages

    def subrequest(self, lpn, npages):
        m_vpn = self.conf.dftl_lpn_to_m_vpn(lpn)
        s, e = self.mapping_manager.lpn_range_of_m_vpn(m_vpn)
        new_npages = min(e - lpn, npages)

        return lpn, new_npages

    def prefetch_and_access(self, lpn, npages, page_access_func):
        """
        From TPFTL authors:

        Request-level prefetching is activated under two conditions:
        1. the first page access split from a request is not in the mapping
        cache;
        2. if the request-level prefetching length is reduced according to
        Section 4.5, the request-level prefetching will be activated when a
        cache miss starts again during the address translation of the request.

        JUN: efffectively, what they are saying is, to try prefetching whenever
        there is a miss for page i. The prefetching length is limited by
        min(size of page i's TP, size of the coldest TP node, size of the
        request).

        Considering the example (a large request LPN 0--2047), the
        request-level prefetching length is firstly set as 2048, but then
        reduced to 1024.  When LPN-0 misses in the cache,  LPN-0 to LPN-1023
        from translation page 0 is firstly loaded; then LPN-1024 misses in the
        cache, the request-level prefetching is activated again since the large
        request is not completed.  This time the length is set as 1024
        (2048-1024), so LPN-1024 to LPN-2047 from translation page 1 will be
        loaded.
        """

        # The following for loop would behave the same as the author indicated
        for sub_lpn, sub_npages in self.split_request(lpn, npages):
            if not self.mapping_manager.is_extent_in_cache(
                sub_lpn, 1):
                # prefetch the whole TP when the first page misses
                self.mapping_manager.load_mapping_for_extent(sub_lpn,
                    sub_npages)
            for page in range(sub_lpn, sub_lpn + sub_npages):
                # self.lba_read(page)
                page_access_func(page)

    # @utils.debug_decor
    def read_range(self, lpn, npages):
        self.prefetch_and_access(lpn, npages, self.lba_read)

    # @utils.debug_decor
    def write_range(self, lpn, npages):
        self.prefetch_and_access(lpn, npages, self.lba_write)

    # @utils.debug_decor
    def discard_range(self, lpn, npages):
        self.prefetch_and_access(lpn, npages, self.lba_discard)

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


