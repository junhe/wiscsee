import random

from utilities import utils

"""
Minimum coupling with others
"""

READ, WRITE, DISCARD = ('OPREAD', 'OPWRITE', 'OPDISCARD')

class Request(object):
    def __init__(self, op, offset, size):
        self.op = op
        self.offset = offset
        self.size = size

    def __str__(self):
        return "{} {} {}".format(self.op, self.offset, self.size)


class PatternBase(object):
    def __iter__(self):
        raise NotImplementedError()

class InitMixin(object):
    def __init__(self, op, zone_offset, zone_size, chunk_size, traffic_size):
        self.op = op
        self.zone_offset = zone_offset
        self.zone_size = zone_size
        self.chunk_size = chunk_size
        self.traffic_size = traffic_size

        utils.assert_multiple(n=traffic_size, divider=chunk_size)
        utils.assert_multiple(n=zone_size, divider=chunk_size)


class Random(PatternBase, InitMixin):
    def __iter__(self):
        n_req = self.traffic_size / self.chunk_size
        n_chunks = self.zone_size / self.chunk_size
        chunk_idx = list(range(n_chunks))

        for i in range(n_req):
            chunk_id = random.choice(chunk_idx)
            req_offset = chunk_id * self.chunk_size
            req_size = self.chunk_size
            req = Request(self.op, req_offset, req_size)
            yield req

        raise StopIteration


class Sequential(PatternBase, InitMixin):
    def __iter__(self):
        n_req = self.traffic_size / self.chunk_size
        n_chunks = self.zone_size / self.chunk_size
        chunk_idx = list(range(n_chunks))

        for i in range(n_req):
            chunk_id = i % n_chunks
            req_offset = chunk_id * self.chunk_size
            req_size = self.chunk_size
            req = Request(self.op, req_offset, req_size)
            yield req

        raise StopIteration


class HotNCold(PatternBase, InitMixin):
    """
    Half of chunks are accessed once, half of chunks are accessed
    decidec by traffic_size

    We access sequentially first, then only the hot chunks
    """
    def __iter__(self):
        n_req = self.traffic_size / self.chunk_size
        n_chunks = self.zone_size / self.chunk_size
        chunk_idx = list(range(n_chunks))

        req_cnt = 0

        # first pass, one by one
        chunk_id = 0
        while chunk_id < n_chunks and  req_cnt < n_req:
            req_offset = chunk_id * self.chunk_size
            req_size = self.chunk_size
            req = Request(self.op, req_offset, req_size)
            yield req

            req_cnt += 1
            chunk_id += 1

        # second pass, only evens
        for i in range(n_req - req_cnt):
            chunk_id = (i * 2) % n_chunks
            req_offset = chunk_id * self.chunk_size
            req_size = self.chunk_size
            req = Request(self.op, req_offset, req_size)
            yield req

        raise StopIteration


def mix(*pattern_iters):
    pattern_iters = list(pattern_iters)
    while True:
        to_del = []
        for req_iter in pattern_iters:
            try:
                yield req_iter.next()
            except StopIteration:
                to_del.append(req_iter)

        for req_iter in to_del:
            pattern_iters.remove(req_iter)

        if len(pattern_iters) == 0:
            raise StopIteration




