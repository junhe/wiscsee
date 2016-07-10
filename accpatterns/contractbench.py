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


class RequestScale(object):
    def __init__(self, space_size, chunk_size, traffic_size, op):
        self.space_size = space_size
        self.chunk_size = chunk_size
        self.traffic_size = traffic_size
        self.op = op

    def __iter__(self):
        req_iter = RandomNoHole(op=self.op, zone_offset=None,
                zone_size=self.space_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)
        for req in req_iter:
            yield req


class Locality(RequestScale):
    pass


class GroupByInvTime(object):
    def __init__(self, space_size, traffic_size, chunk_size, byinvtime):
        self.space_size = space_size
        self.traffic_size = traffic_size
        self.chunk_size = chunk_size
        self.byinvtime = byinvtime

    def _get_reqs(self, start, op):
        n_chunks = self.traffic_size / self.chunk_size

        reqs = []
        for i in range(n_chunks):
            off = start + i * self.chunk_size
            req = Request(op, off, self.chunk_size)
            reqs.append(req)

        return reqs

    def __iter__(self):
        reqs1 = self._get_reqs(0, OP_WRITE)
        second_start = self.space_size - self.traffic_size
        reqs_discard = self._get_reqs(0, OP_DISCARD)

        assert second_start > 0 + self.traffic_size
        reqs2 = self._get_reqs(second_start, OP_WRITE)

        if self.byinvtime is True:
            reqs = reqs1 + reqs2
        else:
            reqs = [r for pair in zip(reqs1, reqs2) for r in pair]

        reqs = reqs + reqs_discard

        for req in reqs:
            yield req





