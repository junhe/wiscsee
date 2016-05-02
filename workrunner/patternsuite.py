from commons import *
from accpatterns import patterns
from ssdbox import hostevent


class SuiteBase(object):
    def __init__(self, conf, local_conf=None):
        self.conf = conf
        self.local_conf = local_conf

        flashbytes = self.conf.total_flash_bytes()

        self.chunk_size = self.local_conf['chunk_size']
        self.zone_size = flashbytes / 8
        self.traffic_size = self.zone_size * 2
        self.snake_size = self.zone_size / 2
        self.stride_size = self.chunk_size * 2

class SRandomRead(SuiteBase):
    "Sequential write and then reandomly read it"
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        # write half
        self.write_iter = patterns.Sequential(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.zone_size,
                traffic_size=self.zone_size)

        self.read_iter = patterns.Random(op=OP_READ, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        for req in self.read_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start')

        yield hostevent.ControlEvent(operation=OP_CLEAN)

class SRandomWrite(SuiteBase):
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        # write half
        self.write_iter = patterns.Random(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start')

        yield hostevent.ControlEvent(operation=OP_CLEAN)

class SSequentialRead(SuiteBase):
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        # write half
        self.write_iter = patterns.Sequential(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.zone_size,
                traffic_size=self.zone_size)

        self.read_iter = patterns.Sequential(op=OP_READ, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        for req in self.read_iter:
            yield req


class SSequentialWrite(SuiteBase):
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        # write half
        self.write_iter = patterns.Sequential(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req


class SSnake(SuiteBase):
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        self.write_iter = patterns.Snake(zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size, snake_size=self.snake_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req


class SFadingSnake(SuiteBase):
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        # write half
        self.write_iter = patterns.Snake(zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size, snake_size=self.snake_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req


class SStrided(SuiteBase):
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        # write half
        self.write_iter = patterns.Strided(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size, stride_size=self.stride_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req


class SHotNCold(SuiteBase):
    def _prepare_iter(self):
        flashbytes = self.conf.total_flash_bytes()
        writesize = flashbytes/8
        chunk_size = self.local_conf['chunk_size']

        # write half
        self.write_iter = patterns.HotNCold(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

    def __iter__(self):
        self._prepare_iter()

        for req in self.write_iter:
            yield req


