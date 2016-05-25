import random
from commons import *

from utilities import utils

"""
Minimum coupling with others
"""

READ, WRITE, DISCARD = (OP_READ, OP_WRITE, OP_DISCARD)

class Request(object):
    def __init__(self, op, offset, size):
        self.op = op
        self.offset = offset
        self.size = size

    def get_operation(self):
        return self.op

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

class RandomNoHole(PatternBase, InitMixin):
    def __iter__(self):
        n_req = self.traffic_size / self.chunk_size
        n_chunks = self.zone_size / self.chunk_size
        chunk_idx = list(range(n_chunks))
        random.shuffle(chunk_idx)

        for i in range(n_req):
            chunk_id = chunk_idx[i % n_chunks]
            req_offset = chunk_id * self.chunk_size
            req_size = self.chunk_size
            req = Request(self.op, req_offset, req_size)
            yield req

        raise StopIteration

class Sequential(PatternBase, InitMixin):
    def __iter__(self):
        n_req = self.traffic_size / self.chunk_size
        n_chunks = self.zone_size / self.chunk_size

        for i in range(n_req):
            chunk_id = i % n_chunks
            req_offset = chunk_id * self.chunk_size
            req_size = self.chunk_size
            req = Request(self.op, req_offset, req_size)
            yield req

        raise StopIteration


class Strided(PatternBase, InitMixin):
    """
    Stride includes the data and hole

    |      stride      |
    | data |   hole    |
    """
    def __init__(self, op, zone_offset, zone_size, chunk_size, traffic_size,
            stride_size):
        super(Strided, self).__init__(op, zone_offset, zone_size, chunk_size,
                traffic_size)

        self.stride_size = stride_size
        utils.assert_multiple(self.zone_size, self.stride_size)

    def __iter__(self):
        n_req = self.traffic_size / self.chunk_size
        n_strides = self.zone_size / self.stride_size

        for i in range(n_req):
            stride_id = i % n_strides
            req_offset = stride_id * self.stride_size
            req_size = self.chunk_size
            req = Request(self.op, req_offset, req_size)
            yield req

        raise StopIteration


class Snake(PatternBase, InitMixin):
    """
    Like the snake walking in the space. If the snake appears to be too long,
    its tail will be cut (invalidated)
    """
    def __init__(self, zone_offset, zone_size, chunk_size, traffic_size,
            snake_size):
        # op is set to None
        super(Snake, self).__init__(None, zone_offset, zone_size, chunk_size,
                traffic_size)

        self.snake_size = snake_size
        assert self.snake_size < self.zone_size

        utils.assert_multiple(self.snake_size, self.chunk_size)

    def __iter__(self):
        n_w_req = self.traffic_size / self.chunk_size
        n_chunks = self.zone_size / self.chunk_size
        n_snake_chunks = self.snake_size / self.chunk_size

        cur_snake_size = 0
        w_req_cnt = 0
        write_chunk_id = 0
        while w_req_cnt < n_w_req:
            req_offset = (write_chunk_id % n_chunks) * self.chunk_size
            req = Request(op=WRITE, offset=req_offset, size=self.chunk_size)
            yield req

            w_req_cnt += 1
            cur_snake_size += self.chunk_size

            if cur_snake_size > self.snake_size:
                # cut the tail
                req_offset = ((write_chunk_id - n_snake_chunks) % n_chunks) * self.chunk_size
                req = Request(op=DISCARD, offset=req_offset, size=self.chunk_size)
                yield req

                cur_snake_size -= self.chunk_size

            write_chunk_id += 1

        raise StopIteration


class FadingSnake(PatternBase, InitMixin):
    """
    Like the snake walking in the space. Once the snake reaches its adult size,
    for each chunk write, one existing chunk will be selected randomly and
    invalidated.
    """
    def __init__(self, zone_offset, zone_size, chunk_size, traffic_size,
            snake_size):
        # op is set to None
        super(FadingSnake, self).__init__(None, zone_offset, zone_size, chunk_size,
                traffic_size)

        self.snake_size = snake_size
        assert self.snake_size < self.zone_size

        utils.assert_multiple(self.snake_size, self.chunk_size)

    def __iter__(self):
        n_w_req = self.traffic_size / self.chunk_size
        n_chunks = self.zone_size / self.chunk_size
        n_snake_chunks = self.snake_size / self.chunk_size

        cur_snake_size = 0
        w_req_cnt = 0
        write_chunk_id = 0
        valid_chunks = []
        while w_req_cnt < n_w_req:
            write_chunk_id = write_chunk_id % n_chunks

            req_offset = write_chunk_id * self.chunk_size
            req = Request(op=WRITE, offset=req_offset, size=self.chunk_size)
            yield req

            valid_chunks.append(write_chunk_id)
            w_req_cnt += 1
            cur_snake_size += self.chunk_size

            if cur_snake_size > self.snake_size:
                # discard some chunk
                victim_chunk_id = random.choice(valid_chunks)
                valid_chunks.remove(victim_chunk_id)
                req_offset = (victim_chunk_id % n_chunks) * self.chunk_size
                req = Request(op=DISCARD, offset=req_offset, size=self.chunk_size)
                yield req

                cur_snake_size -= self.chunk_size

            write_chunk_id += 1

        raise StopIteration




class HotNCold(PatternBase, InitMixin):
    """
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




