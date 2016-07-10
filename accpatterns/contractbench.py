from commons import *
from .patterns import *

class Alignment(object):
    def __init__(self, block_size, space_size, aligned, op):
        self.block_size = block_size
        self.space_size = space_size
        self.aligned = aligned
        self.op = op

    def _offset_to_global(self, block_id, offset):
        return block_id * self.block_size + offset

    def __iter__(self):
        n_blocks = self.space_size / self.block_size
        chunk_size = self.block_size / 2
        if self.aligned is True:
            offsets = [0, chunk_size]
        else:
            offsets = [chunk_size, 0]

        for block_id in range(n_blocks):
            for off in offsets:
                req_off = self._offset_to_global(block_id, off)
                req = Request(self.op, req_off, chunk_size)
                yield req


