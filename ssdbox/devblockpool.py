from tagblockpool import *


class MultiChannelBlockPool(object):
    def __init__(self, n_channels, n_blocks_per_channel, n_pages_per_block, tags):
        self.n_channels = n_channels
        self.n_blocks_per_channel = n_blocks_per_channel
        self.n_pages_per_block = n_pages_per_block
        self.total_blocks = n_blocks_per_channel * n_channels
        self.tags = tags

        self._channel_pool = [
            ChannelBlockPool(n_blocks_per_channel, tags, n_pages_per_block, i) \
                for i in range(n_channels)]

        self._next_channel = 0

    def incr_next_channel(self):
        self._next_channel = (self._next_channel + 1) % self.n_channels
        return self._next_channel

    def count_blocks(self, tag):
        total = 0
        for pool in self._channel_pool:
            total += pool.count_blocks(tag)

        return total

    def get_blocks_of_tag(self, tag):
        ret = []
        for pool in self._channel_pool:
            ret.extend(pool.get_blocks_of_tag(tag))

        return ret

    def pick_and_move(self, src, dst):
        "This function will advance self._next_channel"
        cur_channel = self._next_channel
        self._next_channel += 1

        block_off = self._channel_pool[cur_channel].pick_and_move(src, dst)

        return self.channel_to_global(cur_channel, block_off)

    def change_tag(self, blocknum, src, dst):
        channel_id, block_off = self.global_to_channel(blocknum)
        self._channel_pool[channel_id].change_tag(blocknum, src, dst)

    def channel_to_global(self, channel_id, blocknum):
        ret = channel_id * self.n_blocks_per_channel + blocknum
        assert ret < self.total_blocks
        return ret

    def global_to_channel(self, blocknum):
        "return channel_id, block_offset"
        assert blocknum < self.total_blocks
        return blocknum / self.n_blocks_per_channel, \
                blocknum % self.n_blocks_per_channel

    def current_blocks(self):
        "return all current block numbers"
        objs = []
        for pool in self._channel_pool:
            for tag in self.tags:
                cur_blk_objs = pool.get_cur_block_obj(tag)
                objs.extend(cur_blk_objs)
        return [obj.blocknum for obj in objs]

    def next_ppns(self, n, tag, block_index, stipe_size):
        remaining = n

        ret_ppns = []
        channels_tried = set()
        while remaining > 0:
            # get stripe_size of pages from current channel each time
            cur_channel_id = self._next_channel
            pool = self._channel_pool[cur_channel_id]

            n_for_cur_channel = min(stripe_size, remaining)
            try:
                ppns = pool.next_ppns_from_cur_block(
                    n_for_cur_channel, tag, block_index)
            except TagOutOfSpaceError:
                ppns
            ret_ppns.extend(ppns)

            self.incr_next_channel()




class ChannelBlockPool(BlockPoolWithCurBlocks):
    def __init__(self, n, tags, n_pages_per_block, channel_id):
        super(ChannelBlockPool, self).__init__(n, tags, n_pages_per_block)
        self.channel_id = channel_id


