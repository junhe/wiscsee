from collections import Counter

TFREE = 'TAGFREE'

LEAST_ERASED = 'least'
MOST_ERASED = 'most'


class TagBlockPool(object):
    def __init__(self, n, tags):
        self._tag_subpool = {tag:[] for tag in tags}
        self._tag_subpool[TFREE] = range(n)

        # {blocknum: count}
        self._erasure_cnt = Counter()
        # have to put the block number in the counter
        # otherwise, if a free block is never used, it won't
        # appear in the counter.
        for block in range(n):
            self._erasure_cnt[block] = 0

    def get_blocks_of_tag(self, tag):
        return self._tag_subpool[tag]

    def change_tag(self, blocknum, src, dst):
        self._tag_subpool[src].remove(blocknum)
        self._tag_subpool[dst].append(blocknum)

        if dst == TFREE:
            self._erasure_cnt[blocknum] += 1

    def count_blocks(self, tag):
        return len(self._tag_subpool[tag])

    def pick(self, tag, choice=LEAST_ERASED):
        return self.get_least_or_most_erased_block(tag, choice)

    def pick_and_move(self, src, dst, choice=LEAST_ERASED):
        block = self.pick(src, choice=choice)

        if block is None:
            return None
        else:
            self.change_tag(block, src, dst)
            return block

    def get_erasure_count(self, blocknum=None):
        if blocknum is None:
            return self._erasure_cnt
        else:
            return self._erasure_cnt[blocknum]

    def get_least_or_most_erased_block(self, tag, choice=LEAST_ERASED):
        blocks = self.get_least_or_most_erased_blocks(tag, choice, nblocks=1)

        assert len(blocks) <= 1
        if len(blocks) == 1:
            return blocks[0]
        else:
            return None

    def get_least_or_most_erased_blocks(self, tag, choice, nblocks):
        if choice == LEAST_ERASED:
            blocks_by_cnt = reversed(self._erasure_cnt.most_common())
        elif choice == MOST_ERASED:
            blocks_by_cnt = self._erasure_cnt.most_common()
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

    def get_erasure_count_dist(self):
        return Counter(self._erasure_cnt.values())


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

    def num_free_pages(self):
        return self.n_pages_per_block - self.next_page_offset

    def is_full(self):
        assert self.next_page_offset <= self.n_pages_per_block
        return self.next_page_offset == self.n_pages_per_block


class BlockPoolWithCurBlocks(TagBlockPool):
    def __init__(self, n, tags, n_pages_per_block):
        super(BlockPoolWithCurBlocks, self).__init__(n, tags)
        self._n_pages_per_block = n_pages_per_block

        # {TAG1: {0: CurrentBlock obj, 1: CurrentBlock obj},
        #  TAG2: {0: CurrentBlock obj, 1: CurrentBlock obj}}
        self._cur_blocks = {tag:{} for tag in tags}

    def get_cur_block_obj(self, tag, block_index=None):
        """
        There can be several 'current blocks', use block_index to
        choose.
        """
        if block_index is None:
            # return all cur block objs of a tag if block_index is not
            # specified.
            return [obj for obj in self._cur_blocks[tag].values()]
        else:
            return self._cur_blocks[tag].get(block_index, None)

    def next_ppns_from_cur_block(self, n, tag, block_index):
        """
        Return only what the current block has. If the current block
        is full or is None, return empty list.
        """
        cur_block_obj = self.get_cur_block_obj(tag, block_index)
        if cur_block_obj is None:
            return []
        else:
            ppns = cur_block_obj.next_ppns(n)
            return ppns

    def remove_full_cur_blocks(self):
        """
        If cur block is full, we mark it as NON cur block. So garbage collector
        can clean it.
        """
        for tag, cur_obj_dict in self._cur_blocks.items():
            to_del_block_index = [
                    block_index for block_index, obj in cur_obj_dict.items()
                    if obj.is_full()]
            for block_index in to_del_block_index:
                del cur_obj_dict[block_index]

    def set_new_cur_block(self, tag, block_index, blocknum):
        """
        Set block blocknum to be the current block of tag and block_index.
        blocknum must be a fresh new tagged (tag) block got by, usually
        pick_and_move().
        blocknum must has been tagged $tag before calling this function.
        """
        block_obj = CurrentBlock(self._n_pages_per_block, blocknum=blocknum)
        self._cur_blocks[tag][block_index] = block_obj
        return block_obj


