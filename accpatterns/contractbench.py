from commons import *
from .patterns import *
from ssdbox import hostevent

class Alignment(object):
    def __init__(self, block_size, traffic_size, aligned, op):
        self.block_size = block_size
        self.traffic_size = traffic_size
        self.aligned = aligned
        self.op = op

    def _offset_to_global(self, block_id, offset):
        return block_id * self.block_size + offset

    def __iter__(self):
        n_blocks = self.traffic_size / self.block_size
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

class BarrierMixin(object):
    def barrier_events(self):
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)
        yield hostevent.ControlEvent(operation=OP_BARRIER)
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)


class RequestScale(BarrierMixin):
    def __init__(self, space_size, chunk_size, traffic_size, op, n_ncq_slots):
        self.space_size = space_size
        self.chunk_size = chunk_size
        self.traffic_size = traffic_size
        self.op = op
        self.n_ncq_slots = n_ncq_slots

    def get_iter(self):
        req_iter = Random(op=self.op, zone_offset=None,
                zone_size=self.space_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)
        for req in req_iter:
            yield req

    def __iter__(self):
        if self.op == OP_READ:
            yield Request(OP_WRITE, 0, self.space_size)

        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_PURGE_TRANS_CACHE)

        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        for req in self.get_iter():
            yield req

        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_end')

class Locality(RequestScale):
    pass


class GroupByInvTimeAtAccTime(object):
    def __init__(self, space_size, traffic_size, chunk_size, grouping):
        self.space_size = space_size
        self.traffic_size = traffic_size
        self.chunk_size = chunk_size
        self.grouping = grouping

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

        if self.grouping is True:
            reqs = reqs1 + reqs2
        else:
            reqs = [r for pair in zip(reqs1, reqs2) for r in pair]

        reqs = reqs + reqs_discard

        for req in reqs:
            yield req

class GroupByInvTimeInSpace(object):
    def __init__(self, space_size, traffic_size, chunk_size, grouping):
        self.space_size = space_size
        self.traffic_size = traffic_size
        self.chunk_size = chunk_size
        self.grouping = grouping

    def _get_reqs(self, start, op, stride_size):
        n_chunks = self.traffic_size / self.chunk_size

        reqs = []
        for i in range(n_chunks):
            off = start + i * stride_size
            req = Request(op, off, self.chunk_size)
            reqs.append(req)

        return reqs

    def __iter__(self):
        if self.grouping is True:
            reqs1 = self._get_reqs(0, OP_WRITE, stride_size=self.chunk_size)
            reqs1_discard = self._get_reqs(0, OP_DISCARD,
                    stride_size=self.chunk_size)

            second_start = self.space_size - self.traffic_size
            reqs2 = self._get_reqs(second_start, OP_WRITE,
                    stride_size=self.chunk_size)
        else:
            reqs1 = self._get_reqs(0, OP_WRITE, stride_size=2*self.chunk_size)
            reqs1_discard = self._get_reqs(0, OP_DISCARD,
                    stride_size=2*self.chunk_size)
            reqs2 = self._get_reqs(self.chunk_size, OP_WRITE,
                    stride_size=2*self.chunk_size)

        reqs = [r for pair in zip(reqs1, reqs2) for r in pair]
        reqs = reqs + reqs1_discard

        for req in reqs:
            yield req

