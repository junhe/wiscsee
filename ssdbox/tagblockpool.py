TFREE = 'TAGFREE'

class TagBlockPool(object):
    def __init__(self, n, tags):
        self._tag_subpool = {tag:[] for tag in tags}
        self._tag_subpool[TFREE] = range(n)

    def get_blocks_of_tag(self, tag):
        return self._tag_subpool[tag]

    def change_tag(self, blocknum, src, dst):
        self._tag_subpool[src].remove(blocknum)
        self._tag_subpool[dst].append(blocknum)

    def count_blocks(self, tag):
        return len(self._tag_subpool[tag])

    def pick_and_move(self, src, dst):
        block = self._tag_subpool[src][-1]
        self.change_tag(block, src, dst)
        return block


class CurrentBlock(object):
    def __init__(self, n_pages_per_block, blocknum):
        self.n_pages_per_block = n_pages_per_block
        self.blocknum = blocknum
        self.next_page_offset = 0

    def next_ppns(self, n):
        end_offset = min(self.next_page_offset + n, self.n_pages_per_block)
        ppns = []
        for offset in range(self.next_page_offset, end_offset):
            ppns.append(self.n_pages_per_block * self.blocknum + offset)

        self.next_page_offset = end_offset
        return ppns

    def is_full(self):
        assert self.next_page_offset <= self.n_pages_per_block
        return self.next_page_offset == self.n_pages_per_block


class BlockPoolWithCurBlocks(TagBlockPool):
    def __init__(self, n, tags, n_pages_per_block):
        super(BlockPoolWithCurBlocks, self).__init__(n, tags)
        self._n_pages_per_block = n_pages_per_block

        # {TAG1: {0: CurrentBlock, 1: CurrentBlock},
        #  TAG2: {0: CurrentBlock, 1: CurrentBlock}}
        self._cur_blocks = {tag:{} for tag in tags}

    def cur_block_obj(self, tag, block_index=0):
        """
        There can be several 'current blocks', use block_index to
        choose.
        """
        return self._cur_blocks[tag].get(block_index, None)

    def next_ppns_from_cur_block(self, n, tag, block_index):
        """
        """
        remaining = n

        ret_ppns = []
        while remaining > 0:
            cur_block_obj = self.cur_block_obj(tag, block_index)
            if cur_block_obj is None or cur_block_obj.is_full():
                cur_block_obj = self._set_new_cur_block(tag, block_index)

            ppns = cur_block_obj.next_ppns(remaining)
            ret_ppns.extend(ppns)

            remaining -= len(ppns)
        return ret_ppns

    def _set_new_cur_block(self, tag, block_index):
        new_block_num = self.pick_and_move(src=TFREE, dst=tag)
        block_obj = CurrentBlock(self._n_pages_per_block, blocknum=new_block_num)
        self._cur_blocks[tag][block_index] = block_obj
        return block_obj






