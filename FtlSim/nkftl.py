import copy
from collections import deque

import config
import ftlbuilder
import recorder
import utils

"""
############## Checklist ###############
When you conduct an operation, consider how it affects the following data
structure:
1. OOB
2. Flash
3. Block Pool
4. DataBlockMappingTable
5. LogBlockMappingTable
6. LogPageMappingTable
7. Garbage Collector
8. Appending points
"""


DATA_USER = "data.user"
PPN_NOT_EXIST = "PPN_NOT_EXIST"
PPN_NOT_VALID = "PPN_NOT_VALID"
PHYSICAL_BLK_NOT_EXIST = "PHYSICAL_BLK_NOT_EXIST"
ERR_NEED_NEW_BLOCK, ERR_NEED_MERGING = ('ERR_NEED_NEW_BLOCK', 'ERR_NEED_MERGING')
IN_LOG_BLOCK = "IN_LOG_BLOCK"
IN_DATA_BLOCK = "IN_DATA_BLOCK"

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

        self.flash_num_blocks = confobj['flash_num_blocks']
        self.flash_npage_per_block = confobj['flash_npage_per_block']
        self.total_pages = self.flash_num_blocks * self.flash_npage_per_block

        # Key data structures
        self.states = ftlbuilder.FlashBitmap2(confobj)
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
    def timestamp(self):
        """
        This function will advance timestamp
        """
        t = self.cur_timestamp
        self.cur_timestamp += 1
        return t

    def timestamp_set_ppn(self, ppn):
        self.timestamp_table[ppn] = self.timestamp()

    def timestamp_copy(self, src_ppn, dst_ppn):
        self.timestamp_table[dst_ppn] = self.timestamp_table[src_ppn]

    def translate_ppn_to_lpn(self, ppn):
        return self.ppn_to_lpn_mvpn[ppn]

    def wipe_ppn(self, ppn):
        self.states.invalidate_page(ppn)
        block, _ = self.conf.page_to_block_off(ppn)
        self.last_inv_time_of_block[block] = datetime.datetime.now()

        # It is OK to delay it until we erase the block
        # try:
            # del self.ppn_to_lpn_mvpn[ppn]
        # except KeyError:
            # # it is OK that the key does not exist, for example,
            # # when discarding without writing to it
            # pass

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

        del self.last_inv_time_of_block[flash_block]

    def new_write(self, lpn, old_ppn, new_ppn):
        """
        mark the new_ppn as valid
        update the LPN in new page's OOB to lpn
        invalidate the old_ppn, so cleaner can GC it
        """
        self.states.validate_page(new_ppn)
        self.ppn_to_lpn_mvpn[new_ppn] = lpn

        if old_ppn != None:
            # the lpn has mapping before this write
            self.wipe_ppn(old_ppn)

    def new_lba_write(self, lpn, old_ppn, new_ppn):
        """
        This is exclusively for lba_write(), so far
        """
        self.timestamp_set_ppn(new_ppn)
        self.new_write(lpn, old_ppn, new_ppn)

    def data_page_move(self, lpn, old_ppn, new_ppn):
        # move data page does not change the content's timestamp, so
        # we copy
        self.timestamp_copy(src_ppn = old_ppn, dst_ppn = new_ppn)
        self.new_write(lpn, old_ppn, new_ppn)

    def lpns_of_block(self, flash_block):
        s, e = self.conf.block_to_page_range(flash_block)
        lpns = []
        for ppn in range(s, e):
            lpns.append(self.ppn_to_lpn_mvpn.get(ppn, 'NA'))

        return lpns


class BlockPool(object):
    def __init__(self, confobj):
        self.conf = confobj

        self.freeblocks = deque(range(self.conf['flash_num_blocks']))

        # initialize usedblocks
        self.log_usedblocks = []
        self.data_usedblocks  = []

    def pop_a_free_block(self):
        if self.freeblocks:
            blocknum = self.freeblocks.popleft()
        else:
            # nobody has free block
            raise RuntimeError('No free blocks in device!!!!')

        return blocknum

    def pop_a_free_block_to_log_blocks(self):
        "take one block from freelist and add it to log block list"
        blocknum = self.pop_a_free_block()
        self.log_usedblocks.append(blocknum)
        return blocknum

    def pop_a_free_block_to_data_blocks(self):
        "take one block from freelist and add it to data block list"
        blocknum = self.pop_a_free_block()
        self.data_usedblocks.append(blocknum)
        return blocknum

    def move_used_data_block_to_free(self, blocknum):
        self.data_usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def move_used_log_block_to_free(self, blocknum):
        self.log_usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def total_used_blocks(self):
        return len(self.log_usedblocks) + len(self.data_usedblocks)

    def used_blocks(self):
        return self.log_usedblocks + self.data_usedblocks

    def __str__(self):
        ret = ' '.join(['freeblocks', repr(self.freeblocks)]) + '\n' + \
            ' '.join(['log_usedblocks', repr(self.trans_usedblocks)]) + \
            '\n' + \
            ' '.join(['data_usedblocks', repr(self.data_usedblocks)])
        return ret

    def visual(self):
        block_states = [ 'O' if block in self.freeblocks else 'X'
                for block in range(self.conf['flash_num_blocks'])]
        return ''.join(block_states)

    def used_ratio(self):
        return (len(self.log_usedblocks) + len(self.data_usedblocks))\
            / float(self.conf['flash_num_blocks'])

class MappingBase(object):
    """
    This class defines a __init__() that passes in necessary objects to the
    mapping object.
    """
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj

class MappingManager(MappingBase):
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj):
        super(MappingManager, self).__init__(confobj, block_pool, flashobj,
                oobobj, recorderobj)
        self.data_block_mapping_table = DataBlockMappingTable(confobj,
                block_pool, flashobj, oobobj, recorderobj)
        self.log_mapping_table = LogMappingTable(confobj,
                block_pool, flashobj, oobobj, recorderobj)

    def __str__(self):
        ret = []
        ret.append('-------------------------- MAPPING MANAGER --------------------------')
        ret.append('--------- data block mapping table ------------')
        ret.append(str(self.data_block_mapping_table))
        ret.append('--------- log mapping table -------------------')
        ret.append(str(self.log_mapping_table))
        ret.append('=====================================================================')

        return '\n'.join(ret)

    def lpn_to_ppn(self, lpn):
        """
        Return Found?, PPN, STATE
        """

        # Try log blocks
        ppn = self.log_mapping_table.lpn_to_ppn(lpn)

        if ppn != PPN_NOT_EXIST:
            return True, ppn, IN_LOG_BLOCK

        # Try data blocks
        ppn = self.data_block_mapping_table.lpn_to_ppn(lpn)

        if ppn == PHYSICAL_BLK_NOT_EXIST:
            return False, None, PHYSICAL_BLK_NOT_EXIST
        elif self.oob.states.is_page_valid(ppn):
            return True, ppn, IN_DATA_BLOCK
        else:
            return False, None, PPN_NOT_VALID

class DataBlockMappingTable(MappingBase):
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj):
        super(DataBlockMappingTable, self).__init__(confobj, block_pool, flashobj,
                oobobj, recorderobj)

        self.logical_to_physical_block = {}

    def lbn_to_pbn(self, lbn):
        return self.logical_to_physical_block.get(lbn, PHYSICAL_BLK_NOT_EXIST)

    def lpn_to_ppn(self, lpn):
        """
        Note that the return ppn may not be valid. The caller needs to check.
        """
        logical_block, off = self.conf.page_to_block_off(lpn)
        physical_block = self.lbn_to_pbn(logical_block)
        if physical_block == PHYSICAL_BLK_NOT_EXIST:
            return PHYSICAL_BLK_NOT_EXIST

        # Now we know the physical block exist, but we still need to check if
        # the corresponding page is valid or not
        ppn = block_off_to_page(physical_block, off)
        return ppn

    def add_mapping(self, lbn, pbn):
        self.logical_to_physical_block[lbn] = pbn

    def remove_mapping(self, lbn):
        del self.logical_to_physical_block[lbn]

    def __str__(self):
        return str(self.logical_to_physical_block)


class DataGroupInfo(object):
    """
    It is essentially a better name for dict, holding LPN->PPN for
    log page mapping table. The problem is you need to do bookkeeping
    for info like: the current/next page to program.
    """
    def __init__(self, confobj):
        self.conf = confobj
        self.page_map = {} # lpn->ppn
        self.log_blocks = []
        # offset within the data group
        self.last_programmed_offset = -1
        self.max_log_pages = self.conf.nkftl_max_n_log_pages_in_data_group()

    def add_mapping(self, lpn, ppn):
        """
        Note that this function may overwrite existing mapping. If later you
        need keeping everything, add one data structure.
        """
        self.page_map[lpn] = ppn

    def add_log_block(self, block_num):
        """
        It returns the ppn of the first page in the block, because usually you will
        program after adding a log block.
        """
        self.log_blocks.append(block_num)
        # assert len(self.log_blocks) <= self.conf['nkftl']['max_blocks_in_log_group'], \
            # "{}, {}".format(len(self.log_blocks), self.
                    # conf['nkftl']['max_blocks_in_log_group'])

    def offset_to_ppn(self, offset):
        in_block_page_off = offset % self.conf['flash_npage_per_block']
        block_off = offset / self.conf['flash_npage_per_block']
        block_num = self.log_blocks[block_off]
        ppn = self.conf.block_off_to_page(block_num, in_block_page_off)
        return ppn

    def next_ppn_to_program(self):
        """
        This function returns the next free ppn to program.
        This function fails when:
            1. the current log block has no free pages
            2. the number of log blocks have reached its max

        return Found, ppn/states
        ************************************************************
        Note that this function may increment last_programmed_offset
        ************************************************************
        """
        print self.last_programmed_offset, self.max_log_pages
        print 'log blocks:', len(self.log_blocks)
        if self.last_programmed_offset == self.max_log_pages - 1:
            return False, ERR_NEED_MERGING

        npages_per_block = self.conf['flash_npage_per_block']
        next_offset = self.last_programmed_offset + 1
        block_of_next_offset = next_offset / npages_per_block

        print 'block_of_next_offset', block_of_next_offset, \
                'log_blocks', len(self.log_blocks)
        if block_of_next_offset >= len(self.log_blocks):
            # block index >= number of blocks
            # the next page is out of the current available blocks
            print 'ERR_NEED_NEW_BLOCK'
            return False, ERR_NEED_NEW_BLOCK


        self.last_programmed_offset += 1
        return True, self.offset_to_ppn(next_offset)

    def lpn_to_ppn(self, lpn):
        return self.page_map.get(lpn, PPN_NOT_EXIST)

    def __str__(self):
        ret = []
        ret.append("page_map:" + str(self.page_map))
        ret.append("log_blocks:" + str(self.log_blocks))
        ret.append("last_programmed_offset:"
            + str(self.last_programmed_offset))
        return '\n'.join(ret)

class LogMappingTable(MappingBase):
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj):
        super(LogMappingTable, self).__init__(confobj, block_pool, flashobj,
                oobobj, recorderobj)

        self.dgn_to_data_group_info = {} # dgn -> data group info

    def __str__(self):
        ret = []
        for k, v in self.dgn_to_data_group_info.items():
            ret.append('-- data group no.' + str(k))
            ret.append(str(v))
        return '\n'.join(ret)

    def add_log_mapping(self, lpn, ppn):
        """
        ppn must belong to to log block of this data group
        """
        dgn = self.conf.nkftl_data_group_number_of_lpn(lpn)
        data_group_info = self.dgn_to_data_group_info.setdefault(dgn,
            DataGroupInfo(self.conf))
        data_group_info.page_map[lpn] = ppn

    def add_log_block(self, dgn, block_num):
        """
        Add a log block to data group dgn
        """
        return self.dgn_to_data_group_info[dgn].add_log_block(block_num)

    def next_ppn_to_program(self, dgn):
        page_map = self.dgn_to_data_group_info.setdefault(dgn,
            DataGroupInfo(self.conf))
        return page_map.next_ppn_to_program()

    def lpn_to_ppn(self, lpn):
        dgn = self.conf.nkftl_data_group_number_of_lpn(lpn)
        data_group_info = self.dgn_to_data_group_info.get(dgn, None)
        if data_group_info == None:
            return PPN_NOT_EXIST
        return data_group_info.lpn_to_ppn(lpn)

    def remove_log_block(self,
            data_group_no, log_block_num, logical_block_num):
        data_group_info = self.dgn_to_data_group_info[data_group_no]
        data_group_info.log_blocks.remove(log_block_num)
        lpn_start, lpn_end = self.conf.block_to_page_range(logical_block_num)
        for lpn in range(lpn_start, lpn_end):
            # all mappings should exist
            del data_group_info.page_map[lpn]

        assert (data_group_info.last_programmed_offset + 1) % \
                self.conf['flash_npage_per_block'] == 0, \
                "last_programmed_offset + 1:{}".format(data_group_info.last_programmed_offset + 1)
        data_group_info.last_programmed_offset -= self.conf['flash_npage_per_block']

    def remove_lpn(self, lpn):
        """
        Remove lpn->ppn, and invalidate ppn. It does not affect the log_blocks
        """
        dgn = self.conf.nkftl_data_group_number_of_lpn(lpn)
        ppn = self.lpn_to_ppn(lpn)
        if ppn == PPN_NOT_EXIST:
            return

        self.oob.states.invalidate_page(ppn)
        del self.dgn_to_data_group_info[dgn].page_map[lpn]

class GarbageCollector(object):
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj,
            mappingmanagerobj):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj
        self.mapping_manager = mappingmanagerobj

    def merge_log_block(self, log_block_num):
        """
        1. Try switch merge
        2. Try copy merge
        3. Try full merge
        """
        is_mergable, logical_block = self.is_switch_mergable(log_block_num)
        print 'switch merge  is_mergable:', is_mergable, 'logical_block:', logical_block
        if is_mergable == True:
            self.switch_merge(log_block_num = log_block_num,
                    logical_block = logical_block)
            return

        is_mergable, logical_block, offset = self.is_partial_mergable(
            log_block_num)
        print 'partial merge  is_mergable:', is_mergable, 'logical_block:', logical_block
        if is_mergable == True:
            partial_merge(log_block_num = log_block_num,
                logical_block_num = logical_block,
                first_free_offset = offset)
            return

        full_merge(log_block_num)

    def collect_garbage_for_data_group(self, data_group_no):
        """
        This function will merge the contents of all log blocks associated
        with data_group_no into data blocks. After calling this function,
        there should be no log blocks remaining for this data group.
        """
        print '======= collect_garbage_for_data_group()'
        print str(self.mapping_manager)

        # We make local copy since we may need to modify the original data
        # in the loop
        log_block_list = copy.copy(self.mapping_manager.log_mapping_table\
                .dgn_to_data_group_info[data_group_no].log_blocks)
        for log_block in log_block_list:
            print 'merging log block ------>', log_block
            self.merge_log_block(log_block)

        print '=========== after garbage collection ========'
        print str(self.mapping_manager)

    def full_merge(self, log_block_num):
        """
        This log block (log_block_num) could contain pages from many different
        logical blocks. For each logical block we find in this log block, we
        iterate all LPNs to and copy their data to a new free block.
        """
        ppn_start, ppn_end = self.conf.block_to_page_range(log_block_num)
        logical_blocks = set()
        for ppn in range(ppn_start, ppn_end):
            is_valid = self.oob.states.is_page_valid(ppn)
            lpn = self.oob.ppn_to_lpn_mvpn(ppn)
            logical_block, _ = self.conf.page_to_block_off(lpn)
            logical_blocks.add(logical_block)

        for logical_block in logical_blocks:
            aggregate_logical_block(logical_block)

    def aggregate_logical_block(self, logical_block_num, tag):
        """
        This function gathers all the logical pages in logical_block_num
        and put them to a new physical block.

        The input logical block should have at least one valid page.
        Otherwise we will create a block with no valid pages.
        """
        dst_phy_block_num == self.block_pool.pop_a_free_block_to_data_blocks()

        lpn_start, lpn_end = self.conf.block_to_page_range(logical_block_num)
        for lpn in range(lpn_start, lpn_end):
            found, src_ppn, loc = self.mapping_manager.lpn_to_ppn(lpn)
            in_block_page_off = lpn - lpn_start
            dst_ppn = self.conf.block_off_to_page(dst_phy_block_num,
                in_block_page_off)
            if found == True:
                self.flash.page_read(src_ppn, tag)
                self.flash.page_write(dst_ppn, tag)

                if loc == IN_LOG_BLOCK:
                    self.mapping_manager.log_mapping_table.remove_lpn(lpn)
                elif loc == IN_DATA_BLOCK:
                    self.oob.states.invalidate_page(src_ppn)
                else:
                    raise RuntimeError("Why did you come here? A ppn can only"
                            "be in data blocks or in log blocks.")
            else:
                # This lpn does not exist, so we just invalidate the
                # destination page. We have to do this because we can only
                # program flash sequentially.
                self.flash.page_write(dst_ppn, tag)
                self.oob.states.invalidate_page(src_ppn)

        # Now we have all the pages in new block, we make the new block
        # the data block for logical_block_num
        self.mapping_manager.data_block_mapping_table.add_mapping(
            lbn = logical_block_num, pbn = dst_phy_block_num)

    def is_partial_mergable(self, log_block_num):
        """
        This function tells if log_block_num is partial mergable.

        To be partial mergable, you need:
        1. first k pages are valid, the rest are erased
        2. the first k pages are aligned with logical block
        3. the kth-nth pages exist in the data block

        Return: True/False, logical block, offset of the first erased page
        """
        ppn_start, ppn_end = self.conf.block_to_page_range(log_block_num)
        lpn_start = None
        logical_block = None
        check_mode = 'VALID'
        first_free_ppn = None
        for ppn in range(ppn_start, ppn_end):
            if check_mode == 'VALID':
                # For the first x pages, check if they are valid
                if self.oob.states.is_page_valid(ppn):
                    # valid, check if it is aligned
                    lpn = self.oob.ppn_to_lpn_mvpn(ppn)
                    if lpn_start == None:
                        logical_block, logical_off = self.conf.page_to_block_off(lpn)
                        if logical_off != 0:
                            # Not aligned
                            return False, None, None
                        lpn_start = lpn
                        # Now we know at least the lpn_start and ppn_start are aligned
                        continue
                    if lpn - lpn_start != ppn - ppn_start:
                        # Not aligned
                        return False, None, None
                else:
                    # Not valid
                    if ppn == ppn_start:
                        # The first ppn is not valid, not partial mergable
                        return False, None, None
                    # if we find any page that is not valid, we start checking
                    # erased pages, starting from this page
                    check_mode = 'ERASED'

            if check_mode == 'ERASED':
                if not self.oob.states.is_page_erased(ppn):
                    return False, None, None
                if first_free_ppn == None:
                    first_free_ppn = ppn

                lpn = lpn_start + (ppn - ppn_start)
                has_it, tmp_ppn, loc = self.mapping_manager.lpn_to_ppn(lpn)
                if has_it == False or loc != IN_DATA_BLOCK:
                    # ppn not exist
                    return False, None, None

        return True, logical_block, first_free_ppn - ppn_start

    def partial_merge(self, log_block_num, logical_block_num,
            first_free_offset):
        """
        Copy logical pages to log_block_num, then invalidate the previous
        """
        for offset in range(first_free_offset,
                self.conf['flash_npage_per_block']):
            lpn = self.conf.block_off_to_page(logical_block_num, offset)
            has_it, src_ppn, location = self.mapping_manager.lpn_to_ppn(lpn)
            assert has_it
            assert location == IN_DATA_BLOCK

            dst_ppn = self.conf.block_off_to_page(log_block_num, offset)

            self.flash.page_read(src_ppn, 'partial_merge')
            self.flash.page_write(dst_ppn, 'partial_merge')

            # src_ppn could be in data block or in log block
            # if it is in data block, we only need to invalidate it
            self.oob.states.invalidate_page(src_ppn)

        # Now all pages belong to logical_block_num is in log_block_num
        # We can erase the old data block
        phy_block_num = self.mapping_manager.data_block_mapping_table\
                .lbn_to_pbn[logical_block_num]
        self.flash.block_erase(phy_block_num)
        del self.mapping_manager.data_block_mapping_table\
                .lbn_to_pbn[logical_block_num]

    def is_switch_mergable(self, log_block_num):
        """
        To be switch mergable, the block has to satisfy the following
        conditions:
        1. all pages are valid
        2. all LPNs are 'aligned' with block page numbers

        It also returns the corresponding logical block number if it is
        switch mergable.
        """
        ppn_start, ppn_end = self.conf.block_to_page_range(log_block_num)
        lpn_start = None
        logical_block = None
        for ppn in range(ppn_start, ppn_end):
            if not self.oob.states.is_page_valid(ppn):
                return False, None
            lpn = self.oob.ppn_to_lpn_mvpn[ppn]
            if lpn_start == None:
                logical_block, logical_off = self.conf.page_to_block_off(lpn)
                if logical_off != 0:
                    return False, None
                lpn_start = lpn
                # Now we know at least the lpn_start and ppn_start are aligned
                continue
            if lpn - lpn_start != ppn - ppn_start:
                return False, None

        return True, logical_block

    def switch_merge(self, log_block_num, logical_block):
        """
        Merge log_block_num, which corresponds to logical_block

        1. Before calling this function, make sure log_block_num is switch
        mergable
        2. Find and erase the old physical block corresponding to the logical
        block in Data Block Mapping Table, put it to free block pool
        3. Update the mapping logical block -> log_block_num
        4. Remove log_block_num from log mapping table
        """
        # erase
        old_physical_block = self.mapping_manager.data_block_mapping_table\
            .lbn_to_pbn(logical_block)
        print 'old_physical_block', old_physical_block
        if old_physical_block != PHYSICAL_BLK_NOT_EXIST:
            # clean up old_physical_block
            self.oob.erase_block(old_physical_block)
            self.flash.block_erase(old_physical_block, 'switch.merge')
            self.block_pool.move_used_log_block_to_free(old_physical_block)

        # update data block mapping table
        # This will override the old mapping if there is one
        self.mapping_manager.data_block_mapping_table.add_mapping(
            logical_block, log_block_num)

        # Update log mapping table
        # We need to remove log_block_num from Log Block Mapping Table and
        # all the page mapping of logical_block from log page mapping table
        data_group_no = self.conf.nkftl_data_group_number_of_logical_block(
                logical_block)
        self.mapping_manager.log_mapping_table.remove_log_block(
                data_group_no = data_group_no,
                log_block_num = log_block_num,
                logical_block_num = logical_block)


class Nkftl(ftlbuilder.FtlBuilder):
    """
    This is an FTL implemented according to paper:
        A reconfigurable FTL Architecture for NAND Flash-Based Applications
    """
    def __init__(self, confobj, recorderobj, flashobj):
        super(Nkftl, self).__init__(confobj, recorderobj, flashobj)

        self.block_pool = BlockPool(confobj)
        self.oob = OutOfBandAreas(confobj)

        ###### the managers ######
        self.mapping_manager = MappingManager(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = flashobj,
            oobobj=self.oob,
            recorderobj = recorderobj
            )

        self.garbage_collector = GarbageCollector(
            confobj = self.conf,
            flashobj = flashobj,
            oobobj=self.oob,
            block_pool = self.block_pool,
            mappingmanagerobj = self.mapping_manager,
            recorderobj = recorderobj
            )

    def lba_read(self, lpn):
        """
        Look for log blocks first since they have the latest data
        Then go to data blocks
        """
        pass

    @utils.debug_decor
    def lba_write(self, lpn):
        """
        1. get data group number of lpn
        2. check if it has a writable log block by LBMT
        3. it does not have writable log block, and the number of log blocks
        have not reached max, get one block from free block pool and add to
        LGMT as a log block.
        4. if it does not have writable log block and the number of log blocks
        have reached max, merge the log blocks first and then get a free
        block as log block
        5. Add the mapping of LPN to PPN to LPMT
        6. if we are out of free blocks, start garbage collection.
        """
        data_group_no = self.conf.nkftl_data_group_number_of_lpn(lpn)

        # find old ppn, we have to invalidate it
        # Try log block first, then data block, it may not exist
        hasit, old_ppn, _ = self.mapping_manager.lpn_to_ppn(lpn)
        if hasit == False:
            old_ppn = None

        found, new_ppn = self.mapping_manager.log_mapping_table\
                .next_ppn_to_program(data_group_no)

        # loop until we find a new ppn to program
        while found == False:
            print 'new_ppn', new_ppn
            if new_ppn == ERR_NEED_NEW_BLOCK:
                new_block = self.block_pool.pop_a_free_block_to_log_blocks()
                # The add_log_block() function conveniently returns the ppn of
                # the first page in the new block
                self.mapping_manager.log_mapping_table.add_log_block(
                    data_group_no, new_block)
            elif new_ppn == ERR_NEED_MERGING:
                print 'THIS IS NEEED MERGING'
                self.garbage_collector.collect_garbage_for_data_group(
                    data_group_no)
                print 'new_ppn after merging', new_ppn

            found, new_ppn = self.mapping_manager.log_mapping_table\
                .next_ppn_to_program(data_group_no)

        # OOB
        print lpn, old_ppn, new_ppn
        self.oob.new_lba_write(lpn = lpn, old_ppn = old_ppn,
            new_ppn = new_ppn)

        self.flash.page_write(new_ppn, DATA_USER)

        # this may just update the current mapping, instead of 'add'ing.
        self.mapping_manager.log_mapping_table.add_log_mapping(lpn, new_ppn)

        print str(self.mapping_manager)

    def lba_discard(self, lpn):
        pass

if __name__ == '__main__':
    pass


