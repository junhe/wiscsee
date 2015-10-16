import bidict
import copy
from collections import deque
import datetime
import Queue

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


############## TODO: ####################
1. Test partial merge

############# Refactor NKFTL ############
- All physical block number varaible names end with _pbn
- All logical block number varaible names end with _lbn
- Each class should minimize interaction with other class, it should provide
interface for higher level class to implement
For example, mapping classes should not change block pool. Log mapping class
should not change data block mapping. Be modular!!!
If you find that you need information from another class, it is probably that
you are doing things in a level that is too low.
- Data structure should be easier to maintain. No spread-out data structures

"""

ERR_NEED_NEW_BLOCK, ERR_NEED_MERGING = ('ERR_NEED_NEW_BLOCK', 'ERR_NEED_MERGING')
DATA_USER = "data.user"
IN_LOG_BLOCK = "IN_LOG_BLOCK"
IN_DATA_BLOCK = "IN_DATA_BLOCK"
TYPE_LOG_BLOCK, TYPE_DATA_BLOCK = ('TYPE_LOG_BLOCK', 'TYPE_DATA_BLOCK')

class GlobalHelper(object):
    """
    In case you need some global variables. We put all global stuff here so
    it is easier to manage. (And you know all the bad things you did :)
    """
    def __init__(self, confobj):
        # Sort of a counter incremented by lba operations
        self.cur_lba_op_timestamp = 0

    def incr_lba_op_timestamp(self):
        self.cur_lba_op_timestamp += 1


class OutOfBandAreas(object):
    def __init__(self, confobj):
        self.conf = confobj

        self.flash_num_blocks = confobj['flash_num_blocks']
        self.flash_npage_per_block = confobj['flash_npage_per_block']
        self.total_pages = self.flash_num_blocks * self.flash_npage_per_block

        # Key data structures
        self.states = ftlbuilder.FlashBitmap2(confobj)
        # ppn->lpn mapping stored in OOB, Note that for translation pages, this
        # mapping is ppn -> m_vpn
        self.ppn_to_lpn = {}

    def display_bitmap_by_block(self):
        npages_per_block = self.conf['flash_npage_per_block']
        nblocks = self.conf['flash_num_blocks']
        totalpages =  nblocks * npages_per_block
        line = ''
        for i in range(totalpages):
            block_index = i / npages_per_block
            rem = i % npages_per_block
            if rem == 0:
                line += ' ' + str(block_index) + ':'
            line += str(self.states.page_state_human(i)) + '|'

        return line

    def translate_ppn_to_lpn(self, ppn):
        return self.ppn_to_lpn[ppn]

    def wipe_ppn(self, ppn):
        self.states.invalidate_page(ppn)

        # It is OK to delay deleting ppn_to_lpn[ppn] until we erase the block

    def erase_block(self, flash_block):
        """
        Note that it does not call flash.block_erase(), because that's not
        something OOB
        """
        self.states.erase_block(flash_block)

        start, end = self.conf.block_to_page_range(flash_block)
        for ppn in range(start, end):
            try:
                del self.ppn_to_lpn[ppn]
                # if you try to erase translation block here, it may fail,
                # but it is expected.
            except KeyError:
                pass

    def remap(self, lpn, old_ppn, new_ppn):
        """
        It remaps lpn from old_ppn to new_ppn
        mark the new_ppn as valid
        update the LPN in new page's OOB to lpn
        invalidate the old_ppn, so cleaner can GC it
        """
        self.states.validate_page(new_ppn)
        self.ppn_to_lpn[new_ppn] = lpn

        if old_ppn != None:
            # the lpn has mapping before this write
            self.states.invalidate_page(old_ppn)

    def lpns_of_block(self, flash_block):
        s, e = self.conf.block_to_page_range(flash_block)
        lpns = []
        for ppn in range(s, e):
            lpns.append(self.ppn_to_lpn.get(ppn, 'NA'))

        return lpns

    def is_any_page_valid(self, flash_block):
        """
        Check if there is any page in the
        """
        ppn_start, ppn_end = self.conf.block_to_page_range(flash_block)
        for ppn in range(ppn_start, ppn_end):
            if self.states.is_page_valid(ppn):
                return True
        return False

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
            # utils.breakpoint()
            raise RuntimeError('No free blocks in device!!!!')

        return blocknum

    def pop_a_free_block_to_log_blocks(self):
        "take one block from freelist and add it to log block list"
        blocknum = self.pop_a_free_block()
        self.log_usedblocks.append(blocknum)
        return blocknum

    def move_used_log_to_data_block(self, blocknum):
        self.log_usedblocks.remove(blocknum)
        self.data_usedblocks.append(blocknum)

    def move_used_data_to_log_block(self, blocknum):
        self.data_usedblocks.remove(blocknum)
        self.log_usedblocks.append(blocknum)

    def pop_a_free_block_to_data_blocks(self):
        "take one block from freelist and add it to data block list"
        blocknum = self.pop_a_free_block()
        self.data_usedblocks.append(blocknum)
        return blocknum

    def free_used_data_block(self, blocknum):
        self.data_usedblocks.remove(blocknum)
        self.freeblocks.append(blocknum)

    def free_used_log_block(self, blocknum):
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
    def __init__(self, confobj, recorderobj, global_helper_obj):
        self.conf = confobj
        self.recorder = recorderobj
        self.global_helper = global_helper_obj


class DataBlockMappingTable(MappingBase):
    def __init__(self, confobj, recorderobj, global_helper_obj):
        super(DataBlockMappingTable, self).__init__(confobj, recorderobj,
            global_helper_obj)

        self.logical_to_physical_block = bidict.bidict()

    def lbn_to_pbn(self, lbn):
        """
        Given a logical block number, it returns the corresponding physical
        block number, according to data block mapping

        Return Found, PBN
        """
        pbn = self.logical_to_physical_block.get(lbn, None)
        if pbn == None:
            return False, None
        else:
            return True, pbn

    def lpn_to_ppn(self, lpn):
        """
        Finds ppn of a lpn in data blocks>

        Note that the return ppn may not be valid. The caller needs to check.

        Return: found, ppn
        """
        logical_block, off = self.conf.page_to_block_off(lpn)
        found, pbn = self.lbn_to_pbn(logical_block)
        if not found:
            return False, None

        ppn = self.conf.block_off_to_page(pbn, off)
        return True, ppn

    def add_data_block_mapping(self, lbn, pbn):
        self.logical_to_physical_block[lbn] = pbn

    def remove_data_block_mapping(self, lbn):
        del self.logical_to_physical_block[lbn]

    def remove_data_block_mapping_by_pbn(self, pbn):
        del self.logical_to_physical_block[:pbn]

    def __str__(self):
        return str(self.logical_to_physical_block)

class SingleLogBlockInfo(object):
    def __init__(self, confobj, flash_block_num, last_used_time = None,
            last_programmed_offset = -1):
        self.conf = confobj
        self.flash_block_num = flash_block_num
        self.last_used_time = last_used_time
        self.last_programmed_offset = last_programmed_offset

    def __str__(self):
        ret = []
        ret.append('---------- SingleLogBlockInfo')
        ret.append('flash_block_num:{}'.format(self.flash_block_num))
        ret.append('last_used_time :{}'.format(self.last_used_time))
        ret.append('last_programmed_offset:{}'.format(
            self.last_programmed_offset))
        return '\n'.join(ret)

    def has_free_page(self):
        return self.last_programmed_offset < \
            self.conf['flash_npage_per_block'] - 1

    def next_ppn_to_program(self):
        """
        Note that this increment self.last_programmed_offset
        If next_ppn_to_program() return True, you have to program the
        returned ppn
        """
        if self.last_programmed_offset + 1 < self.conf['flash_npage_per_block']:
            self.last_programmed_offset += 1
            return True, self.conf.block_off_to_page(self.flash_block_num,
                    self.last_programmed_offset)
        else:
            return False, None


class LogGroupInfo(object):
    """
    It keeps information of a paritical data group.
    """
    def __init__(self, confobj, recorderobj, global_helper_obj):
        self.conf = confobj
        self.recorder = recorderobj
        self.global_helper = global_helper_obj

        # Set them to be private so I understand they are only accessed
        # within this class. Less mess.
        # every time we update __page_map, we should also update
        self.__page_map = bidict.bidict() # lpn->ppn
        # flash block number -> SingleLogBlockInfo
        #                       last_used_time, last_programmed_offset
        self.__log_blocks = {}     #log_pbn -> Singlelogblockinfo
        self.__cur_log_block = None

    def remove_lpn(self, lpn):
        del self.__page_map[lpn]

    def remove_log_block(self, log_pbn):
        # remove all page maps
        ppn_start, ppn_end = self.conf.block_to_page_range(log_pbn)
        for ppn in range(ppn_start, ppn_end):
            try:
                del self.__page_map[:ppn]
            except KeyError:
                pass

        # remove log_pbn from __log_blocks
        del self.__log_blocks[log_pbn]

        # handle current log block
        if self.__cur_log_block == log_pbn:
            self.__cur_log_block = None

    def clear(self):
        """
        Reset it to original status
        """
        self.__page_map.clear()
        self.__log_blocks.clear()
        self.__cur_log_block = None

    def log_blocks(self):
        return self.__log_blocks

    def add_mapping(self, lpn, ppn):
        """
        Note that this function may overwrite existing mapping. If later you
        need keeping everything, add one data structure.
        """
        self.__page_map[lpn] = ppn

    def update_block_use_time(self, blocknum):
        """
        blocknum is a log block.
        The time will be used when garbage collecting
        """
        self.__log_blocks[blocknum].last_used_time = \
            self.global_helper.cur_lba_op_timestamp

    def add_log_block(self, block_num):
        """
        This should only be called when this log block used all free log pages
        """
        assert not self.__log_blocks.has_key(block_num)
        for singleinfo in self.__log_blocks.values():
            if singleinfo.has_free_page():
                raise RuntimeError("Log Block {} should not have free page"\
                    .format(singleinfo.flash_block_num))
        self.__log_blocks[block_num] = SingleLogBlockInfo(self.conf, block_num)
        self.__cur_log_block = block_num

    def cur_log_block_info(self):
        """
        Return the current log block's SingleLogBlockInfo, for convienence.
        """
        if self.__cur_log_block == None:
            return None
        return self.__log_blocks[self.__cur_log_block]

    def reached_max_n_log_blocks(self):
        return len(self.__log_blocks) >= \
            self.conf['nkftl']['max_blocks_in_log_group']

    def next_ppn_to_program(self):
        """
        This function returns the next free ppn to program.
        This function fails when:
            1. the current log block has no free pages
            2. the number of log blocks have reached its max

        return Found, ppn/states
        """
        # if the number of log blocks has reached max and
        # the current log block has not free pages, we need
        # to merge.
        # if the current log block has not free pages, but
        # the number of log blocks has not reached its max,
        # we need to get a new log block.
        if self.__cur_log_block == None or \
            not self.cur_log_block_info().has_free_page():
            if self.reached_max_n_log_blocks():
                return False, ERR_NEED_MERGING
            else:
                return False, ERR_NEED_NEW_BLOCK
        else:
            return self.cur_log_block_info().next_ppn_to_program()

    def lpn_to_ppn(self, lpn):
        """
        return found, ppn
        """
        ppn = self.__page_map.get(lpn, None)
        if ppn == None:
            return False, None
        else:
            return True, ppn

    def __str__(self):
        ret = []
        ret.append("------- LogGroupInfo")
        ret.append("__page_map:" + str(self.__page_map))
        for logblock, info in self.__log_blocks.items():
            ret.append("Logblock {}{}".format(logblock, str(info)))
        ret.append("cur_log_block:{}".format(self.__cur_log_block))
        return '\n'.join(ret)

class MappingManager(MappingBase):
    def __init__(self, confobj, recorderobj, global_helper_obj):
        super(MappingManager, self).__init__(confobj, recorderobj,
            global_helper_obj)
        self.data_block_mapping_table = DataBlockMappingTable(confobj,
            recorderobj, global_helper_obj)
        self.log_mapping_table = LogMappingTable(confobj,
            recorderobj, global_helper_obj)

    def __str__(self):
        ret = []
        ret.append('========================= MAPPING MANAGER ===========================')
        ret.append('------------------------ Data Block Mapping Table -------------------')
        ret.append(str(self.data_block_mapping_table))
        ret.append('------------------------ Log Block Mapping Table --------------------')
        ret.append(str(self.log_mapping_table))
        ret.append('=====================================================================')

        return '\n'.join(ret)

    def lpn_to_ppn(self, lpn):
        """
        This function tries to find the ppn of a lpn. It will try log blocks
        first. If not found, then it will try data blocks.

        Note that you need to check if the returned ppn is valid. This function
        only tells you if there is a valid mapping.

        Return: Found?, PPN, Location
        """

        # Try log blocks
        found, ppn = self.log_mapping_table.lpn_to_ppn(lpn)
        if found == True:
            return True, ppn, IN_LOG_BLOCK

        # Try data blocks
        found, ppn = self.data_block_mapping_table.lpn_to_ppn(lpn)
        if found == True:
            return True, ppn, IN_DATA_BLOCK

        # Cannot find lpn
        return False, None, None

class LogMappingTable(MappingBase):
    def __init__(self, confobj, recorderobj, global_helper_obj):
        super(LogMappingTable, self).__init__(confobj, recorderobj,
            global_helper_obj)

        self.log_group_info = {} # dgn -> log block info of data group

    def add_mapping(self, data_group_no, lpn, ppn):
        self.log_group_info[data_group_no].add_mapping(lpn, ppn)

    def remove_lpn(self, data_group_no, lpn):
        self.log_group_info[data_group_no].remove_lpn(lpn)

    def __str__(self):
        ret = []
        for k, v in self.log_group_info.items():
            ret.append('-- data group no.' + str(k))
            ret.append(str(v))
        return '\n'.join(ret)

    def clear_data_group_info(self, dgn):
        self.log_group_info[dgn].clear()

    def add_log_block(self, dgn, flash_block):
        """
        Add a log block to data group dgn
        """
        loginfo = self.log_group_info.setdefault(dgn,
            LogGroupInfo(self.conf, self.recorder, self.global_helper))
        return loginfo.add_log_block(flash_block)

    def next_ppn_to_program(self, dgn):
        loginfo = self.log_group_info.setdefault(dgn,
            LogGroupInfo(self.conf, self.recorder, self.global_helper))
        # it may return ERR_NEED_NEW_BLOCK or ERR_NEED_MERGING
        return loginfo.next_ppn_to_program()

    def lpn_to_ppn(self, lpn):
        """
        Translate lpn to ppn using log block info. Even the ppn does not
        exist here. It may exist in data blocks.

        Return: found, ppn/None
        """
        dgn = self.conf.nkftl_data_group_number_of_lpn(lpn)
        log_group_info = self.log_group_info.get(dgn, None)
        if log_group_info == None:
            return False, None
        return log_group_info.lpn_to_ppn(lpn)

    def remove_log_block(self, data_group_no, log_pbn):
        """
        It completely removes mapping info of log block log_lbn.
        For example, this can be used after switch merging log_lbn.
        """
        self.log_group_info[data_group_no].remove_log_block(log_pbn)


class GcDecider(object):
    def __init__(self, confobj, block_pool, recorderobj):
        self.conf = confobj
        self.block_pool = block_pool
        self.recorder = recorderobj

        self.high_watermark = self.conf['nkftl']['GC_threshold_ratio'] * \
            self.conf['flash_num_blocks']
        self.low_watermark = self.conf['nkftl']['GC_low_threshold_ratio'] * \
            self.conf['flash_num_blocks']

        self.call_index = -1

    def refresh(self):
        """
        TODO: this class needs refactoring.
        """
        self.call_index = -1
        self.last_used_blocks = None
        self.freeze_count = 0

    def need_cleaning(self):
        "The logic is a little complicated"
        self.call_index += 1

        n_used_blocks = self.block_pool.total_used_blocks()

        if self.call_index == 0:
            # clean when above high_watermark
            ret = n_used_blocks > self.high_watermark
        else:
            if self.freezed_too_long(n_used_blocks):
                ret = False
                print 'freezed too long, stop GC'
            else:
                # Is it higher than low watermark?
                ret = n_used_blocks > self.low_watermark
        return ret

    def improved(self, cur_n_used_blocks):
        """
        wether we get some free blocks since last call of this function
        """
        if self.last_used_blocks == None:
            ret = True
        else:
            # common case
            ret = cur_n_used_blocks < self.last_used_blocks

        self.last_used_blocks = cur_n_used_blocks
        return ret

    def freezed_too_long(self, cur_n_used_blocks):
        if self.improved(cur_n_used_blocks):
            self.freeze_count = 0
            ret = False
        else:
            self.freeze_count += 1

            if self.freeze_count > 2 * self.conf['flash_npage_per_block']:
                ret = True
            else:
                ret = False

        return ret


class BlockInfo(object):
    """
    This is for sorting blocks to clean the victim.
    """
    def __init__(self, block_type, block_num, last_used_time,
            data_group_no = None):
        self.block_type = block_type
        self.block_num = block_num
        self.last_used_time = last_used_time
        self.data_group_no = data_group_no

    def __comp__(self, other):
        """
        Low number will be retrieved first in priority queue
        """
        return cmp(self.last_used_time, other.last_used_time)


class GarbageCollector(object):
    def __init__(self, confobj, block_pool, flashobj, oobobj, recorderobj,
            mappingmanagerobj, global_helper_obj):
        self.conf = confobj
        self.flash = flashobj
        self.oob = oobobj
        self.block_pool = block_pool
        self.recorder = recorderobj
        self.mapping_manager = mappingmanagerobj

        self.decider = GcDecider(self.conf, self.block_pool, self.recorder)

    def try_gc(self):
        triggered = False

        self.decider.refresh()
        while self.decider.need_cleaning():
            if self.decider.call_index == 0:
                triggered = True
                print 'GC is triggerred', self.block_pool.used_ratio(), \
                    'freeblocks:', len(self.block_pool.freeblocks)
                block_iter = self.victim_blocks_iter()
                blk_cnt = 0
            # victim_type, victim_block, valid_ratio = self.next_victim_block()
            # victim_type, victim_block, valid_ratio = \
                # self.next_victim_block_benefit_cost()
            try:
                blockinfo = block_iter.next()
            except StopIteration:
                print 'GC stoped from StopIteration exception'
                self.recorder.count_me("GC", "StopIteration")
                # high utilization, raise watermarkt to reduce GC attempts
                self.decider.raise_high_watermark()
                # nothing to be cleaned
                break

            self.clean_block(blockinfo)

            blk_cnt += 1

        if triggered:
            print 'GC is finished', self.block_pool.used_ratio(), \
                blk_cnt, 'collected', \
                'freeblocks:', len(self.block_pool.freeblocks)
            # raise RuntimeError("intentional exit")


    def victim_blocks_iter(self):
        """
        It goes through all log blocks and sort them. It yields the
        least recently used block first.

        TODO: You need to also consider data blocks!!!
        """
        priority_q = Queue.PriorityQueue()

        for data_block in self.block_pool.log_usedblocks:
            if not self.oob.is_any_page_valid(data_block):
                # no page is valid
                blk_info = BlockInfo(
                    block_type = TYPE_DATA_BLOCK,
                    block_num = data_block,
                    last_used_time = -1)  # high priority
                priority_q.put(blk_info)

        for data_group_no, log_group_info in self.mapping_manager\
            .log_mapping_table.log_group_info.items():
            for log_pbn, single_log_block_info in \
                    log_group_info.log_blocks().items():
                blk_info = BlockInfo(
                    block_type = TYPE_LOG_BLOCK,
                    block_num = log_pbn,
                    last_used_time = single_log_block_info.last_used_time,
                    data_group_no = data_group_no)
                priority_q.put(blk_info)

        while not priority_q.empty():
            b_info =  priority_q.get()
            yield b_info

    def clean_block(self, blk_info):
        """
        Cleans one pariticular block, either data or log block. It will
        update all the relevant information such as mapping, oob, flash.
        """
        if blk_info.block_type == TYPE_DATA_BLOCK:
            self.clean_data_block(blk_info.block_num)
        elif blk_info.block_type == TYPE_LOG_BLOCK:
            self.clean_log_block(blk_info)

    def clean_data_block(self, data_block):
        if data_block in self.block_pool.data_usedblocks and \
            not self.oob.is_any_page_valid(data_block):
            self.oob.erase_block(data_block)
            self.flash.block_erase(data_block, 'gc')
            # need to remove data block mapping
            self.mapping_manager.data_block_mapping_table\
                .remove_data_block_mapping_by_pbn(data_block)
            self.block_pool.free_used_data_block(data_block)

    def clean_log_block(self, blk_info):
        """
        0. If not valid page in log_pbn, simply erase and free it
        1. Try switch merge
        2. Try copy merge
        3. Try full merge
        """
        log_pbn = blk_info.block_num
        data_group_no = blk_info.data_group_no

        if not log_pbn in self.block_pool.log_usedblocks:
            # it is quite dynamic, this log block may have been
            # GCed with previous blocks
            return

        # Check to see if it is really the log block of the speicfed data
        # group
        if not log_pbn in self.mapping_manager.log_mapping_table\
                .log_group_info[data_group_no].log_blocks().keys():
            # TODO: maybe you should just return here instead of panic?
            raise RuntimeError("{} is not a log block of data group {}"\
                .format(log_pbn, data_group_no))

        # Just free it?
        if not self.oob.is_any_page_valid(log_pbn):
            self.oob.erase_block(log_pbn)
            self.flash.block_erase(log_pbn, 'gc')
            self.block_pool.free_used_log_block(log_pbn)
            self.mapping_manager.log_mapping_table\
                .remove_log_block(data_group_no = blk_info.data_group_no,
                log_pbn = log_pbn)
            return

        is_mergable, logical_block = self.is_switch_mergable(log_pbn)
        # print 'switch merge  is_mergable:', is_mergable, 'logical_block:', logical_block
        if is_mergable == True:
            self.switch_merge(log_pbn = log_pbn,
                    logical_block = logical_block)
            return

        is_mergable, logical_block, offset = self.is_partial_mergable(
            log_pbn)
        # print 'partial merge  is_mergable:', is_mergable, 'logical_block:', logical_block
        if is_mergable == True:
            self.partial_merge(log_pbn = log_pbn,
                lbn = logical_block,
                first_free_offset = offset)
            return

        self.full_merge(log_pbn)

    def clean_data_group(self, data_group_no):
        """
        This function will merge the contents of all log blocks associated
        with data_group_no into data blocks. After calling this function,
        there should be no log blocks remaining for this data group.
        """
        # print '======= clean_data_group()'
        # print str(self.mapping_manager)

        # We make local copy since we may need to modify the original data
        # in the loop
        # TODO: You need to GC the log blocks in a better order. This matters
        # because for example the first block may require full merge and the
        # second can be partial merged. Doing the full merge first may change
        # the states of the second log block and makes full merge impossible.
        log_block_list = copy.copy(self.mapping_manager.log_mapping_table\
                .log_group_info[data_group_no].log_blocks().keys())
        for log_block in log_block_list:
            # print 'merging log block ------>', log_block
            # print 'before merge', self.block_pool.visual()
            # A log block may not be a log block anymore after the loop starts
            # It may be freed, it may be a data block now,.. Be careful
            self.clean_log_block(BlockInfo(
                        block_type = TYPE_LOG_BLOCK,
                        block_num = log_block,
                        last_used_time = None,
                        data_group_no = data_group_no))
            # print 'after  merge', self.block_pool.visual()

    def full_merge(self, log_pbn):
        """
        This log block (log_pbn) could contain pages from many different
        logical blocks in the same data group. For each logical block we
        find in this log block, we iterate all LPNs to and copy their
        data to a new free block.
        """
        # print '------------------- full merge {} ------------------'.format(log_pbn)

        # Find all the logical blocks
        ppn_start, ppn_end = self.conf.block_to_page_range(log_pbn)
        logical_blocks = set()
        for ppn in range(ppn_start, ppn_end):
            is_valid = self.oob.states.is_page_valid(ppn)
            # print 'ppn {} is valid: {}'.format(ppn, is_valid)
            if is_valid == True:
                lpn = self.oob.ppn_to_lpn[ppn]
                logical_block, _ = self.conf.page_to_block_off(lpn)
                logical_blocks.add(logical_block)

        # print 'logical_blocks', logical_blocks

        # Move all the pages of a logical block to new block
        for logical_block in logical_blocks:
            self.aggregate_logical_block(logical_block, 'full_merge')

    def debug(self):
        print self.block_pool.visual()
        print 'block_pool.freeblocks', self.block_pool.freeblocks
        print 'block_pool.log_usedblocks', self.block_pool.log_usedblocks
        print 'block_pool.data_usedblocks', self.block_pool.data_usedblocks
        print 'oob.states', self.oob.display_bitmap_by_block()
        print 'oob.ppn_to_lpn', self.oob.ppn_to_lpn
        print str(self.mapping_manager)



    def aggregate_logical_block(self, lbn, tag):
        """
        This function gathers all the logical pages in lbn
        and put them to a new physical block.

        The input logical block should have at least one valid page.
        Otherwise we will create a block with no valid pages.

        The procedure:
        1. for each lpn, if it is valid:
        2. copy content to ppn of the same offset in the new block
        3. invalidate the old ppn, validate the new ppn, update oob.ppn_to_lpn
        4. check old ppn's flash block, old_block
        5. if old_block is data block
        6. see if old_block has any valid page left, if not, erase the
        old flash block and free it. Also delete the data block mapping
        to old_block
        7. if old_block is a log block
        8. see if old_block has any valid page left, if not, erase the
        old flash block and free it. Also delete the mapping in log group info

        Well, you need to also consider the case that lbn has no valid pages,
        in which case you need to specifically remove the data block mapping
        """
        # print '------------------------- aggre logical block -----------------------'
        # print self.block_pool.visual()
        # print 'block_pool.freeblocks', self.block_pool.freeblocks
        # print 'block_pool.log_usedblocks', self.block_pool.log_usedblocks
        # print 'block_pool.data_usedblocks', self.block_pool.data_usedblocks
        # print 'oob.states', self.oob.display_bitmap_by_block()
        # print str(self.mapping_manager)

        # We have to make sure this does not fail, somehow.
        dst_phy_block_num = self.block_pool.pop_a_free_block_to_data_blocks()

        lpn_start, lpn_end = self.conf.block_to_page_range(lbn)
        for lpn in range(lpn_start, lpn_end):
            in_block_page_off = lpn - lpn_start
            dst_ppn = self.conf.block_off_to_page(dst_phy_block_num,
                in_block_page_off)
            data_group_no = self.conf.nkftl_data_group_number_of_lpn(lpn)

            found, src_ppn, loc = self.mapping_manager.lpn_to_ppn(lpn)
            if found == True and self.oob.states.is_page_valid(src_ppn):
                data = self.flash.page_read(src_ppn, tag)
                self.flash.page_write(dst_ppn, tag, data = data)

                # print 'Moved lpn:{} (data:{}, src_ppn:{}) to dst_ppn:{}'.format(
                    # lpn, data, src_ppn, dst_ppn)

                self.oob.remap(lpn = lpn, old_ppn = src_ppn,
                    new_ppn = dst_ppn)

                # Now you've moved lpn, you need to remove lpn mapping if it is
                # in log blocks
                if loc == IN_LOG_BLOCK:
                    self.mapping_manager.log_mapping_table.remove_lpn(
                            data_group_no, lpn)

                # After moving, you need to check if the source block of src_ppn
                # is totally free. If it is, we have to erase it and put it to
                # free block pool
                src_pbn, _ = self.conf.page_to_block_off(src_ppn)
                if not self.oob.is_any_page_valid(src_pbn):
                    # No page is valid
                    # We erase and free src_pbn, and clean up its mappings
                    self.oob.erase_block(src_pbn)
                    self.flash.block_erase(src_pbn, 'full.merge')
                    if loc == IN_DATA_BLOCK:
                        self.block_pool.free_used_data_block(src_pbn)
                        lbn, _ = self.conf.page_to_block_off(lpn)
                        self.mapping_manager.data_block_mapping_table\
                            .remove_data_block_mapping(lbn = lbn)
                    elif loc == IN_LOG_BLOCK:
                        self.block_pool.free_used_log_block(src_pbn)
                        self.mapping_manager.log_mapping_table\
                            .remove_log_block(data_group_no = data_group_no,
                            log_pbn = src_pbn)
            else:
                # This lpn does not exist, so we just invalidate the
                # destination page. We have to do this because we can only
                # program flash sequentially.
                # self.flash.page_write(dst_ppn, tag, data = -1)
                self.oob.states.invalidate_page(dst_ppn)

        # Now we have all the pages in new block, we make the new block
        # the data block for lbn
        found, old_pbn = self.mapping_manager.data_block_mapping_table\
            .lbn_to_pbn(lbn)
        if found == True:
            # old_pbn must not have any valid pages, so we free it
            self.clean_data_block(old_pbn)
        self.mapping_manager.data_block_mapping_table.add_data_block_mapping(
            lbn = lbn, pbn = dst_phy_block_num)

        # print '--------------------AFTER aggregate-------------------'
        # print self.block_pool.visual()
        # print 'block_pool.freeblocks', self.block_pool.freeblocks
        # print 'block_pool.log_usedblocks', self.block_pool.log_usedblocks
        # print 'block_pool.data_usedblocks', self.block_pool.data_usedblocks
        # print 'oob.states', self.oob.display_bitmap_by_block()
        # print str(self.mapping_manager)

    def is_partial_mergable(self, log_pbn):
        """
        This function tells if log_pbn is partial mergable.

        To be partial mergable, you need:
        1. first k pages are valid, the rest are erased
        2. the first k pages are aligned with logical block
        3. the kth-nth pages exist in the data block

        Return: True/False, logical block, offset of the first erased page
        """
        ppn_start, ppn_end = self.conf.block_to_page_range(log_pbn)
        lpn_start = None
        logical_block = None
        check_mode = 'VALID'
        first_free_ppn = None
        for ppn in range(ppn_start, ppn_end):
            if check_mode == 'VALID':
                # For the first x pages, check if they are valid
                if self.oob.states.is_page_valid(ppn):
                    # valid, check if it is aligned
                    lpn = self.oob.ppn_to_lpn[ppn]
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

                # check if the content is in data block
                lpn = lpn_start + (ppn - ppn_start)
                has_it, tmp_ppn, loc = self.mapping_manager.lpn_to_ppn(lpn)
                if has_it == False or loc != IN_DATA_BLOCK or \
                    not self.oob.states.is_page_valid(tmp_ppn):
                    # ppn not exist
                    return False, None, None

        return True, logical_block, first_free_ppn - ppn_start

    def partial_merge(self, log_pbn, lbn, first_free_offset):
        """
        """
        data_group_no = self.conf.nkftl_data_group_number_of_logical_block(
            lbn)
        # Copy
        for offset in range(first_free_offset,
                self.conf['flash_npage_per_block']):
            lpn = self.conf.block_off_to_page(lbn, offset)
            found, src_ppn, location = self.mapping_manager.lpn_to_ppn(lpn)
            # src_ppn should be valid since we have checked it
            # is_partial_mergable()
            assert found
            assert location == IN_DATA_BLOCK

            dst_ppn = self.conf.block_off_to_page(log_pbn, offset)

            data = self.flash.page_read(src_ppn, 'partial_merge')
            self.flash.page_write(dst_ppn, 'partial_merge', data = data)

            self.oob.remap(lpn, old_ppn = src_ppn, new_ppn = dst_ppn)

        # Now the old block should be empty
        src_block, _ = self.conf.page_to_block_off(src_ppn)
        self.oob.erase_block(src_block)
        self.flash.block_erase(src_block, 'partial.merge')
        self.block_pool.free_used_data_block(src_block)
        self.mapping_manager.data_block_mapping_table\
            .remove_data_block_mapping(lbn)

        # Now the log block lgo_pbn has all the content of lbn
        self.mapping_manager.data_block_mapping_table\
            .add_data_block_mapping(lbn = lbn, pbn = log_pbn)

        # Remove log_pbn from log group
        self.mapping_manager.log_mapping_table.remove_log_block(
            data_group_no = data_group_no, log_pbn = log_pbn)

    def is_switch_mergable(self, log_pbn):
        """
        To be switch mergable, the block has to satisfy the following
        conditions:
        1. all pages are valid
        2. all LPNs are 'aligned' with block page numbers

        It also returns the corresponding logical block number if it is
        switch mergable.

        Return Mergable?, logical_block
        """
        ppn_start, ppn_end = self.conf.block_to_page_range(log_pbn)
        lpn_start = None
        logical_block = None
        for ppn in range(ppn_start, ppn_end):
            if not self.oob.states.is_page_valid(ppn):
                return False, None
            lpn = self.oob.ppn_to_lpn[ppn]
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

    def switch_merge(self, log_pbn, logical_block):
        """
        Merge log_pbn, which corresponds to logical_block

        1. Before calling this function, make sure log_pbn is switch
        mergable
        2. Find and erase the old physical block corresponding to the logical
        block in Data Block Mapping Table (if there is one), put it to free
        block pool. Update the mapping logical block -> log_pbn
        3. Update log group info:
             remove log_pbn from log_blocks. This is SingleLogBlockInfo
             remove all page mappings in page_map
             set __cur_log_block to None
        """
        # erase old data block
        found, old_physical_block = self.mapping_manager.data_block_mapping_table\
            .lbn_to_pbn(logical_block)

        if found:
            # clean up old_physical_block
            self.oob.erase_block(old_physical_block)
            self.flash.block_erase(old_physical_block, 'switch.merge')
            self.block_pool.free_used_data_block(old_physical_block)
            # self.mapping_manager.data_block_mapping_table.remove_mapping(
                # logical_block)

        # update data block mapping table
        # This will override the old mapping if there is one
        self.mapping_manager.data_block_mapping_table.add_data_block_mapping(
            logical_block, log_pbn)

        # Update log mapping table
        data_group_no = self.conf.nkftl_data_group_number_of_logical_block(
                logical_block)
        self.mapping_manager.log_mapping_table.remove_log_block(
                data_group_no = data_group_no,
                log_pbn = log_pbn)
        # Need to mark the log block as used data block now
        self.block_pool.move_used_log_to_data_block(log_pbn)

class Nkftl(ftlbuilder.FtlBuilder):
    """
    This is an FTL implemented according to paper:
        A reconfigurable FTL Architecture for NAND Flash-Based Applications
    """
    def __init__(self, confobj, recorderobj, flashobj):
        super(Nkftl, self).__init__(confobj, recorderobj, flashobj)

        self.block_pool = BlockPool(confobj)
        self.oob = OutOfBandAreas(confobj)
        self.global_helper = GlobalHelper(confobj)

        ###### the managers ######
        self.mapping_manager = MappingManager(
            confobj = self.conf,
            recorderobj = recorderobj,
            global_helper_obj = self.global_helper
            )

        # Garbage collector is considered to be in the highest level
        self.garbage_collector = GarbageCollector(
            confobj = self.conf,
            block_pool = self.block_pool,
            flashobj = self.flash,
            oobobj = self.oob,
            recorderobj = recorderobj,
            mappingmanagerobj = self.mapping_manager,
            global_helper_obj = self.global_helper
            )

    def lpn_to_ppn(self, lpn):
        found, ppn, location = self.mapping_manager.lpn_to_ppn(lpn)
        if found == True and self.oob.states.is_page_valid(ppn):
            return found, ppn, location
        return False, None, None

    def lba_read(self, lpn):
        """
        Look for log blocks first since they have the latest data
        Then go to data blocks
        """
        self.global_helper.incr_lba_op_timestamp()

        hasit, ppn, loc = self.lpn_to_ppn(lpn)
        if hasit == True:
            content = self.flash.page_read(ppn, 'user.read')
            if loc == IN_LOG_BLOCK:
                phy_block_num, _ = self.conf.page_to_block_off(ppn)
                data_group_no = self.conf.nkftl_data_group_number_of_lpn(lpn)
                self.mapping_manager.log_mapping_table\
                    .log_group_info[data_group_no]\
                    .update_block_use_time(phy_block_num)
        else:
            content = None

        print 'lba_read', lpn, 'ppn', ppn, 'got', content
        return content

    def lba_write(self, lpn, data = None):
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
        self.global_helper.incr_lba_op_timestamp()

        print 'lba_write', lpn, 'data=', data
        self.recorder.write_file('tmp.lba.trace.txt', operation = 'write',
            page = lpn)

        data_group_no = self.conf.nkftl_data_group_number_of_lpn(lpn)

        found, new_ppn = self.mapping_manager.log_mapping_table\
                .next_ppn_to_program(data_group_no)

        # loop until we find a new ppn to program
        while found == False:
            if new_ppn == ERR_NEED_NEW_BLOCK:
                new_block = self.block_pool.pop_a_free_block_to_log_blocks()
                self.mapping_manager.log_mapping_table.add_log_block(
                    data_group_no, new_block)
            elif new_ppn == ERR_NEED_MERGING:
                self.garbage_collector.clean_data_group(
                    data_group_no)

            found, new_ppn = self.mapping_manager.log_mapping_table\
                .next_ppn_to_program(data_group_no)

        # find old ppn, we have to invalidate it
        # Try log block first, then data block, it may not exist
        # We have to find the old_ppn right before writing the new one.
        # We cannot do it before the loop above because merging may change
        # the location of the old ppn
        found, old_ppn, loc = self.mapping_manager.lpn_to_ppn(lpn)
        if found == False:
            old_ppn = None

        # OOB
        # print "lpn{}, old_ppn{}, new_ppn{}".format(lpn, old_ppn, new_ppn)
        # Note that this may create empty data block, which need to be cleaned
        self.oob.remap(lpn = lpn, old_ppn = old_ppn, new_ppn = new_ppn)

        self.flash.page_write(new_ppn, DATA_USER, data = data)

        phy_block, _ = self.conf.page_to_block_off(new_ppn)
        self.mapping_manager.log_mapping_table\
            .log_group_info[data_group_no]\
            .update_block_use_time(phy_block)

        # this may just update the current mapping, instead of 'add'ing.
        self.mapping_manager.log_mapping_table.add_mapping(data_group_no,
            lpn, new_ppn)

        # print 'AFTER WRITE block_pool', self.block_pool.visual()
        # print 'AFTER WRITE block_pool.freeblocks', self.block_pool.freeblocks
        # print 'AFTER WRITE block_pool.log_usedblocks', self.block_pool.log_usedblocks
        # print 'AFTER WRITE block_pool.data_usedblocks', self.block_pool.data_usedblocks
        # print 'AFTER WRITE oob.states', self.oob.display_bitmap_by_block()
        # print 'AFTER WRITE mappings', str(self.mapping_manager)

        self.garbage_collector.try_gc()

    def lba_discard(self, lpn):
        self.global_helper.incr_lba_op_timestamp()

        print 'lba_discard', lpn
        data_group_no = self.conf.nkftl_data_group_number_of_lpn(lpn)

        found, ppn, loc = self.mapping_manager.lpn_to_ppn(lpn)
        if found == True:
            if loc == IN_LOG_BLOCK:
                self.mapping_manager.log_mapping_table.remove_lpn(
                    data_group_no, lpn)
            self.oob.wipe_ppn(ppn)













