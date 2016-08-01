from commons import *
from .patterns import *
from ssdbox import hostevent


class BarrierMixin(object):
    def barrier_events(self):
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)
        yield hostevent.ControlEvent(operation=OP_BARRIER)
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)


class UtilMixin(object):
    def snapshot_before_interest(self):
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                arg1='flash_ops_before_interest')

        yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                arg1='logical_ops_before_interest')

        yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                arg1='hitmiss_before_interest')

    def snapshot_after_interest(self):
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_end')

        yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                arg1='flash_ops_after_interest')

        yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                arg1='logical_ops_after_interest')

        yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                arg1='hitmiss_after_interest')

    def snapshot_before_gc(self):
        yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                arg1='flash_ops_before_gc')

        yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                arg1='logical_ops_before_gc')

        yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                arg1='hitmiss_before_gc')

    def snapshot_after_gc(self):
        yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                arg1='flash_ops_after_gc')

        yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                arg1='logical_ops_after_gc')

        yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                arg1='hitmiss_after_gc')

    def clean(self):
        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start')

        yield hostevent.ControlEvent(operation=OP_CLEAN)

        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_end')

        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_CALC_GC_DURATION)


class Alignment(UtilMixin, BarrierMixin):
    def __init__(self, block_size, traffic_size, aligned, op, n_ncq_slots):
        self.block_size = block_size
        self.traffic_size = traffic_size
        self.aligned = aligned
        self.op = op
        self.n_ncq_slots = n_ncq_slots

    def _offset_to_global(self, block_id, offset):
        return block_id * self.block_size + offset

    def interest_workload(self):
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

    def __iter__(self):
        for req in self.snapshot_before_interest():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        # interest workload
        for req in self.interest_workload():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        for req in self.snapshot_after_interest():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        for req in self.snapshot_before_gc():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        for req in self.clean():
            yield req

        for req in self.barrier_events():
            yield req

        for req in self.snapshot_after_gc():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

class RequestScale(BarrierMixin):
    def __init__(self, space_size, chunk_size, traffic_size, op, n_ncq_slots):
        self.space_size = space_size
        self.chunk_size = chunk_size
        self.traffic_size = traffic_size
        self.op = op
        self.n_ncq_slots = n_ncq_slots

    def get_iter(self):
        req_iter = RandomNoHole(op=self.op, zone_offset=None,
                zone_size=self.space_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)
        for req in req_iter:
            yield req

    def purge_cache(self):
        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_PURGE_TRANS_CACHE)

    def discard_half(self, offset_history):
        # discard half
        offset_history = list(offset_history)
        random.shuffle(offset_history)
        n = len(offset_history)
        for offset in offset_history[0:n/2]:
            req = Request(OP_DISCARD, offset, self.chunk_size)
            yield req

    def clean(self):
        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start')

        yield hostevent.ControlEvent(operation=OP_CLEAN)

        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_end')

        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_CALC_GC_DURATION)


    def __iter__(self):
        if self.op == OP_READ:
            yield Request(OP_WRITE, 0, self.space_size)


        for req in self.purge_cache():
            yield req

        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                arg1='flash_ops_before_interest')

        yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                arg1='logical_ops_before_interest')

        yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                arg1='hitmiss_before_interest')

        # barrier
        for req in self.barrier_events():
            yield req

        offset_history = set()
        for req in self.get_iter():
            offset_history.add(req.offset)
            yield req

        # barrier
        for req in self.barrier_events():
            yield req

        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_end')

        yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                arg1='flash_ops_after_interest')

        yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                arg1='logical_ops_after_interest')

        yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                arg1='hitmiss_after_interest')

        if self.op == OP_WRITE:
            for req in self.discard_half(offset_history):
                yield req

            # barrier
            for req in self.barrier_events():
                yield req

            for req in self.purge_cache():
                yield req

            yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                    arg1='flash_ops_before_gc')

            yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                    arg1='logical_ops_before_gc')

            yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                    arg1='hitmiss_before_gc')

            # barrier
            for req in self.barrier_events():
                yield req

            for req in self.clean():
                yield req

            yield hostevent.ControlEvent(operation=OP_REC_FLASH_OP_CNT,
                    arg1='flash_ops_after_gc')

            yield hostevent.ControlEvent(operation=OP_REC_FOREGROUND_OP_CNT,
                    arg1='logical_ops_after_gc')

            yield hostevent.ControlEvent(operation=OP_REC_CACHE_HITMISS,
                    arg1='hitmiss_after_gc')



class Locality(RequestScale):
    pass


class GroupingBase(object):
    def __init__(self, space_size, traffic_size, chunk_size, grouping,
            n_ncq_slots):
        self.space_size = space_size
        self.traffic_size = traffic_size
        self.chunk_size = chunk_size
        self.grouping = grouping
        self.n_ncq_slots = n_ncq_slots

    def __iter__(self):
        for req in self.snapshot_before_interest():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        # interest workload
        for req in self.interest_workload():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        for req in self.snapshot_after_interest():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        for req in self.discards():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        for req in self.snapshot_before_gc():
            yield req

        # barrier ====================================
        for req in self.barrier_events():
            yield req

        for req in self.clean():
            yield req

        for req in self.snapshot_after_gc():
            yield req

    def _get_reqs(self, start, op, stride_size):
        n_chunks = self.traffic_size / self.chunk_size

        reqs = []
        for i in range(n_chunks):
            off = start + i * stride_size
            req = Request(op, off, self.chunk_size)
            reqs.append(req)

        return reqs


class GroupByInvTimeAtAccTime(GroupingBase, BarrierMixin, UtilMixin):
    def interest_workload(self):
        reqs1 = self._get_reqs(0, OP_WRITE, 2 * self.chunk_size)

        second_start = 0 + self.chunk_size
        reqs2 = self._get_reqs(second_start, OP_WRITE, 2 * self.chunk_size)

        if self.grouping is True:
            reqs = reqs1 + reqs2
        else:
            reqs = [r for pair in zip(reqs1, reqs2) for r in pair]

        for req in reqs:
            yield req

    def discards(self):
        reqs_discard = self._get_reqs(0, OP_DISCARD, 2 * self.chunk_size)

        for req in reqs_discard:
            yield req


class GroupByInvTimeInSpace(GroupingBase, BarrierMixin, UtilMixin):
    def _get_reqs(self, start, op, stride_size):
        n_chunks = self.traffic_size / self.chunk_size

        reqs = []
        for i in range(n_chunks):
            off = start + i * stride_size
            req = Request(op, off, self.chunk_size)
            reqs.append(req)

        return reqs

    def interest_workload(self):
        if self.grouping is True:
            reqs1 = self._get_reqs(0, OP_WRITE, stride_size=self.chunk_size)

            second_start = 0 + self.traffic_size
            reqs2 = self._get_reqs(second_start, OP_WRITE,
                    stride_size=self.chunk_size)
        else:
            reqs1 = self._get_reqs(0, OP_WRITE, stride_size=2*self.chunk_size)
            reqs2 = self._get_reqs(self.chunk_size, OP_WRITE,
                    stride_size=2*self.chunk_size)

        reqs = [r for pair in zip(reqs1, reqs2) for r in pair]

        for req in reqs:
            yield req

    def discards(self):
        if self.grouping is True:
            reqs1_discard = self._get_reqs(0, OP_DISCARD,
                    stride_size=self.chunk_size)
        else:
            reqs1_discard = self._get_reqs(0, OP_DISCARD,
                    stride_size=2*self.chunk_size)

        for req in reqs1_discard:
            yield req


