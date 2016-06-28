TFREE = 'TAGFREE'

class TagBlockPool(object):
    def __init__(self, n, tags):
        self._tag_subpool = {tag:[] for tag in tags}
        self._tag_subpool[TFREE] = range(n)

    def get_blocks_of_tag(self, tag, n=None):
        return self._tag_subpool[tag]

    def change_tag(self, blocknum, src, dst):
        self._tag_subpool[src].remove(blocknum)
        self._tag_subpool[dst].append(blocknum)

    def count_blocks(self, tag):
        return len(self._tag_subpool[tag])





