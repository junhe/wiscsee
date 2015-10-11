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

    def __repr__(self):
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

    def lpn_to_ppn(self, lpn):
        """
        Try log blocks first, if found nothing, try data block, if data block
        doesn't have it, return PPN_NOT_EXIST
        """

        # Try log blocks
        ppn = self.log_mapping_table.lpn_to_ppn(lpn)

        if ppn != PPN_NOT_EXIST:
            return ppn

        # Try data blocks
        ppn = self.data_block_mapping_table.lpn_to_ppn(lpn)

        if ppn == PHYSICAL_BLK_NOT_EXIST:
            return PHYSICAL_BLK_NOT_EXIST
        elif self.oob.stats.is_page_valid(ppn):
            return ppn
        else:
            return PPN_NOT_VALID


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

    def lpn_to_ppn(self, lpn):
        return self.page_map.get(lpn, None)

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

        ************************************************************
        Note that this function may increment last_programmed_offset
        ************************************************************
        """
        print self.last_programmed_offset, self.max_log_pages
        print 'log blocks:', len(self.log_blocks)
        if self.last_programmed_offset == self.max_log_pages - 1:
            return ERR_NEED_MERGING

        npages_per_block = self.conf['flash_npage_per_block']
        next_offset = self.last_programmed_offset + 1
        block_of_next_offset = next_offset / npages_per_block

        print 'block_of_next_offset', block_of_next_offset, \
                'log_blocks', len(self.log_blocks)
        if block_of_next_offset >= len(self.log_blocks):
            # block index >= number of blocks
            # the next page is out of the current available blocks
            print 'ERR_NEED_NEW_BLOCK'
            return ERR_NEED_NEW_BLOCK


        self.last_programmed_offset += 1
        return self.offset_to_ppn(next_offset)

    def lpn_to_ppn(self, lpn):
        return self.page_map.get(lpn, PPN_NOT_EXIST)

class LogMappingTable(MappingBase):
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj):
        super(LogMappingTable, self).__init__(confobj, block_pool, flashobj,
                oobobj, recorderobj)

        self.dgn_to_page_map = {} # dgn -> LogPageMap

    def append_lpn(self, dgn, lpn):
        dg = self.dgn_to_page_map.setdefault(dgn, DataGroupInfo(self.conf))

    def add_log_block(self, dgn, block_num):
        """
        Add a log block to data group dgn
        """
        return self.dgn_to_page_map[dgn].add_log_block(block_num)

    def next_ppn_to_program(self, dgn):
        page_map = self.dgn_to_page_map.setdefault(dgn,
            DataGroupInfo(self.conf))
        return page_map.next_ppn_to_program()

    def lpn_to_ppn(self, lpn):
        dgn = self.conf.nkftl_data_group_number_of_lpn(lpn)
        page_map = self.dgn_to_page_map.get(dgn, None)
        if page_map == None:
            return PPN_NOT_EXIST
        return page_map.lpn_to_ppn(lpn)


class GarbageCollector(object):
    pass

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

        # self.garbage_collector = GarbageCollector(
            # confobj = self.conf,
            # flashobj = flashobj,
            # oobobj=self.oob,
            # block_pool = self.block_pool,
            # mapping_manager = self.mapping_manager,
            # recorderobj = recorderobj
            # )

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
        old_ppn = self.mapping_manager.lpn_to_ppn(lpn)
        if old_ppn in (PHYSICAL_BLK_NOT_EXIST, PPN_NOT_VALID):
            old_ppn = None

        new_ppn = self.mapping_manager.log_mapping_table.next_ppn_to_program(
            data_group_no)

        if new_ppn == ERR_NEED_NEW_BLOCK:
            new_block = self.block_pool.pop_a_free_block_to_log_blocks()
            # The add_log_block() function conveniently returns the ppn of
            # the first page in the new block
            self.mapping_manager.log_mapping_table.add_log_block(
                data_group_no, new_block)
            new_ppn = self.mapping_manager.log_mapping_table\
                .next_ppn_to_program(data_group_no)
        elif new_ppn == ERR_NEED_MERGING:
            raise NotImplementedError()

        # OOB
        self.oob.new_lba_write(lpn = lpn, old_ppn = old_ppn,
            new_ppn = new_ppn)

        self.flash.page_write(new_ppn, DATA_USER)

    def lba_discard(self, lpn):
        pass

if __name__ == '__main__':
    pass


