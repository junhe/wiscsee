from commons import *
from accpatterns import patterns

class SRandomRead(object):
    "Sequential write and then reandomly read it"
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.Sequential(op=OP_WRITE, zone_offset=0,
                zone_size=writesize, chunk_size=writesize, traffic_size=writesize)

        self.read_iter = patterns.Random(op=OP_READ, zone_offset=0,
                zone_size=writesize, chunk_size=2*KB, traffic_size=writesize)

    def __iter__(self):
        for req in self.write_iter:
            yield req

        for req in self.read_iter:
            yield req


class SRandomWrite(object):
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.Random(op=OP_WRITE, zone_offset=0,
                zone_size=writesize, chunk_size=writesize, traffic_size=writesize)

    def __iter__(self):
        for req in self.write_iter:
            yield req


class SSequentialRead(object):
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.Sequential(op=OP_WRITE, zone_offset=0,
                zone_size=writesize, chunk_size=writesize, traffic_size=writesize)

        self.read_iter = patterns.Sequential(op=OP_READ, zone_offset=0,
                zone_size=writesize, chunk_size=2*KB, traffic_size=writesize)

    def __iter__(self):
        for req in self.write_iter:
            yield req

        for req in self.read_iter:
            yield req


class SSequentialWrite(object):
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.Sequential(op=OP_WRITE, zone_offset=0,
                zone_size=writesize, chunk_size=2*KB, traffic_size=writesize)

    def __iter__(self):
        for req in self.write_iter:
            yield req


class SSnake(object):
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.Snake(zone_offset=0, zone_size=flashbytes/2,
                chunk_size=2*KB, traffic_size=writesize, snake_size=writesize)

    def __iter__(self):
        for req in self.write_iter:
            yield req


class SFadingSnake(object):
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.Snake(zone_offset=0, zone_size=flashbytes/2,
                chunk_size=2*KB, traffic_size=flashbytes/2, snake_size=writesize)

    def __iter__(self):
        for req in self.write_iter:
            yield req


class SStrided(object):
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.Strided(op=OP_WRITE, zone_offset=0,
                zone_size=flashbytes/4, chunk_size=2*KB,
                traffic_size=flashbytes/2, stride_size=4*KB)

    def __iter__(self):
        for req in self.write_iter:
            yield req


class SHotNCold(object):
    def __init__(self, conf):
        self.conf = conf

        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8

        # write half
        self.write_iter = patterns.HotNCold(op=OP_WRITE, zone_offset=0,
                zone_size=writesize/2, chunk_size=2*KB,
                traffic_size=writesize)

    def __iter__(self):
        for req in self.write_iter:
            yield req


