from tagblockpool import *
from ftlsim_commons import random_channel_id
from utilities import utils

class MultiChannelBlockPoolBase(object):
    def __init__(self, n_channels, n_blocks_per_channel, n_pages_per_block, tags,
            leveling_factor=2, leveling_diff=10):
        self.n_channels = n_channels
        self.n_blocks_per_channel = n_blocks_per_channel
        self.n_blocks_per_dev = n_blocks_per_channel * n_channels
        self.n_pages_per_block = n_pages_per_block
        self.n_pages_per_channel = n_pages_per_block * n_blocks_per_channel
        self.total_blocks = n_blocks_per_channel * n_channels
        self.tags = tags

        self._channel_pool = [
            ChannelBlockPool(n_blocks_per_channel, tags, n_pages_per_block, i) \
                for i in range(n_channels)]

        # TODO: each tag has its own _next_channel
        self._next_channel = random_channel_id(self.n_channels)
        # self._next_channel = 0

        self.leveling_factor = leveling_factor
        self.leveling_diff = leveling_diff

    def _incr_next_channel(self):
        self._next_channel = (self._next_channel + 1) % self.n_channels
        return self._next_channel

    def count_blocks(self, tag, channels=None):
        total = 0

        if channels is None:
            # count on all channels
            for pool in self._channel_pool:
                total += pool.count_blocks(tag)
        else:
            for i in channels:
                total += self._channel_pool[i].count_blocks(tag)

        return total

    def get_blocks_of_tag(self, tag, channel_id=None):
        ret = []

        if channel_id is None:
            channels =  self._channel_pool
        else:
            channels =  [self._channel_pool[channel_id]]

        for pool in channels:
            blocks = pool.get_blocks_of_tag(tag)
            ret.extend(self._blocks_channel_to_global(pool.channel_id, blocks))

        return ret

    def get_erasure_count_dist(self):
        aggregated_dist = Counter()
        for pool in self._channel_pool:
            dist = pool.get_erasure_count_dist()
            aggregated_dist += dist

        return aggregated_dist

    def get_top_or_bottom_erasure_total(self, choice, need_nblocks):
        dist = self.get_erasure_count_dist()
        erase_cnt, block_cnt = utils.top_or_bottom_total(dist, need_nblocks, choice)

        return erase_cnt, block_cnt

    def get_wear_status(self):
        """
        Return wear factor and diff
        """
        nblocks = self.total_blocks * 0.1

        top_total, top_count = self.get_top_or_bottom_erasure_total(
                'top', nblocks)
        top_average = float(top_total) / top_count
        bottom_total, bottom_count = self.get_top_or_bottom_erasure_total(
                'bottom', nblocks)
        bottom_average = float(bottom_total) / bottom_count

        diff = top_average - bottom_average
        if bottom_total == 0:
            factor = float('inf')
        else:
            factor = float(top_total) / bottom_total

        return factor, diff

    def need_wear_leveling(self):
        factor, diff = self.get_wear_status()

        print factor, self.leveling_factor
        print diff, self.leveling_diff

        if factor > self.leveling_factor and  diff > self.leveling_diff:
            return True
        else:
            return False

    def get_least_or_most_erased_blocks(self, tag, choice, nblocks):
        global_counter = self.get_erasure_count()
        if choice == LEAST_ERASED:
            blocks_by_cnt = reversed(global_counter.most_common())
        elif choice == MOST_ERASED:
            blocks_by_cnt = global_counter.most_common()
        else:
            raise NotImplementedError

        tag_blocks = self.get_blocks_of_tag(tag)

        # iterate from least used to most used
        blocks = []
        for blocknum, count in blocks_by_cnt:
            if blocknum in tag_blocks:
                blocks.append(blocknum)
                if len(blocks) == nblocks:
                    break

        return blocks

    def get_erasure_count(self):
        global_counter = Counter()
        for pool in self._channel_pool:
            erasure_cnt = pool.get_erasure_count()
            for blocknum, erase_cnt in erasure_cnt.items():
                global_blocknum = self._channel_to_global(
                    pool.channel_id, blocknum)
                global_counter[global_blocknum] = erasure_cnt[blocknum]

        return global_counter

    def pick_and_move(self, src, dst, choice=LEAST_ERASED):
        "This function will advance self._next_channel"
        blocknum = self.pick(tag=src, choice=choice)

        if blocknum is None:
            return None
        else:
            self.change_tag(blocknum, src, dst)
            return blocknum

    def pick(self, tag, channel_id=None, choice=LEAST_ERASED):
        if channel_id is None:
            cur_channel = self._next_channel
            self._incr_next_channel()
        else:
            cur_channel = channel_id

        block_off = self._channel_pool[cur_channel].pick(tag=tag,
                choice=choice)

        if block_off is None:
            return None
        else:
            return self._channel_to_global(cur_channel, block_off)

    def change_tag(self, blocknum, src, dst):
        channel_id, block_off = self._global_to_channel(blocknum)
        self._channel_pool[channel_id].change_tag(block_off, src, dst)

    def _channel_to_global(self, channel_id, blocknum):
        ret = channel_id * self.n_blocks_per_channel + blocknum
        assert ret < self.total_blocks
        return ret

    def _global_to_channel(self, blocknum):
        "return channel_id, block_offset"
        assert blocknum < self.total_blocks
        return blocknum / self.n_blocks_per_channel, \
                blocknum % self.n_blocks_per_channel

    def _blocks_channel_to_global(self, channel_id, blocks):
        return [self._channel_to_global(channel_id, block)
                for block in blocks]

    def _ppn_channel_to_global(self, channel_id, ppn):
        return channel_id * self.n_pages_per_channel + ppn

    def _ppns_channel_to_global(self, channel_id, ppns):
        return [self._ppn_channel_to_global(channel_id, ppn) for ppn in ppns]


class MultiChannelBlockPool(MultiChannelBlockPoolBase):
    """
    This is for DFTL
    """
    def current_blocks(self):
        "return all current block numbers"
        blocknums = []
        for pool in self._channel_pool:
            for tag in self.tags:
                cur_blk_objs = pool.get_cur_block_obj(tag)
                for obj in cur_blk_objs:
                    global_blk = self._channel_to_global(pool.channel_id,
                            obj.blocknum)
                    blocknums.append(global_blk)

        return blocknums

    def remove_full_cur_blocks(self):
        for pool in self._channel_pool:
            pool.remove_full_cur_blocks()

    def next_ppns(self, n, tag, block_index, stripe_size, choice=LEAST_ERASED):
        """
        We will try to use all the available pages in the one channels'
        current block before going to the next.
        """
        remaining = n
        if stripe_size == 'infinity':
            stripe_size = float('inf')

        ret_ppns = []
        empty_channels = set()
        while remaining > 0 and len(empty_channels) < self.n_channels:
            cur_channel_id = self._next_channel
            req = min(remaining, stripe_size)
            ppns = self._next_ppns_in_channel(
                    channel_id=cur_channel_id,
                    n=req, tag=tag, block_index=block_index,
                    choice=choice)
            if len(ppns) == 0:
                # channel out of space
                empty_channels.add(cur_channel_id)

            ppns = self._ppns_channel_to_global(cur_channel_id, ppns)
            ret_ppns.extend(ppns)
            self._incr_next_channel()
            remaining -= len(ppns)

        if remaining > 0:
            # out of space
            raise TagOutOfSpaceError

        return ret_ppns

    def _next_ppns_in_channel(self, channel_id, n, tag, block_index,
            choice=LEAST_ERASED):
        """
        Return ppns we can find. If returning [], it means this channel
        is out of space.
        """
        channel_pool = self._channel_pool[channel_id]
        remaining = n

        ret_ppns = []
        while remaining > 0:
            ppnlist = channel_pool.next_ppns_from_cur_block(n=remaining,
                    tag=tag, block_index=block_index)

            if len(ppnlist) == 0:
                new_block = channel_pool.pick_and_move(src=TFREE, dst=tag,
                        choice=choice)
                if new_block == None:
                    # this channel is out of space of this tag
                    break
                channel_pool.set_new_cur_block(tag, block_index, new_block)

            ret_ppns.extend(ppnlist)
            remaining -= len(ppnlist)
        return ret_ppns


class ChannelBlockPool(BlockPoolWithCurBlocks):
    def __init__(self, n, tags, n_pages_per_block, channel_id):
        super(ChannelBlockPool, self).__init__(n, tags, n_pages_per_block)
        self.channel_id = channel_id


class TagOutOfSpaceError(RuntimeError):
    pass


