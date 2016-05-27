from commons import *
from accpatterns import patterns
from ssdbox import hostevent


class SuiteBase(object):
    def __init__(self, zone_size, chunk_size, traffic_size, **kwargs):
        self.zone_size = zone_size
        self.chunk_size = chunk_size
        self.traffic_size = traffic_size

        kwargs.setdefault("snake_size", None)
        kwargs.setdefault("stride_size", None)

        self.snake_size = kwargs['snake_size']
        self.stride_size = kwargs['stride_size']
        self.has_hole = kwargs.get('has_hole', False)
        self.preallocate = kwargs.get('preallocate', False)
        self.keep_size = kwargs.get('keep_size', False)
        self.fsync = kwargs.get('fsync', False)

    def _falloc_keepsize_arg(self):
        if self.keep_size is True:
            return OP_ARG_KEEPSIZE
        else:
            return OP_ARG_NOTKEEPSIZE


class SRandomRead(SuiteBase):
    "Sequential write and then reandomly read it"
    def _prepare_iter(self):
        chunk_size = self.chunk_size

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
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SRandomReadNoPrep(SuiteBase):
    "Sequential write and then reandomly read it"
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        self.read_iter = patterns.Random(op=OP_READ, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

    def __iter__(self):
        self._prepare_iter()

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        for req in self.read_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SRandomWrite(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        # write half
        if self.has_hole is True:
            self.write_iter = patterns.Random(op=OP_WRITE, zone_offset=0,
                    zone_size=self.zone_size, chunk_size=self.chunk_size,
                    traffic_size=self.traffic_size)
        else:
            self.write_iter = patterns.RandomNoHole(op=OP_WRITE, zone_offset=0,
                    zone_size=self.zone_size, chunk_size=self.chunk_size,
                    traffic_size=self.traffic_size)

        if self.fsync is True:
            fsync_req = get_fsync_req()
            self.write_iter = patterns.rep_and_mix(self.write_iter, fsync_req)

    def __iter__(self):
        self._prepare_iter()

        if self.preallocate is True:
            yield hostevent.ControlEvent(operation=OP_FALLOCATE,
                    arg1=self.zone_size, arg2=self._falloc_keepsize_arg())

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SSequentialRead(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

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

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SSequentialReadNoPrep(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        self.read_iter = patterns.Sequential(op=OP_READ, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

    def __iter__(self):
        self._prepare_iter()

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        for req in self.read_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SSequentialWrite(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        # write half
        self.write_iter = patterns.Sequential(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

        if self.fsync is True:
            fsync_req = get_fsync_req()
            self.write_iter = patterns.rep_and_mix(self.write_iter, fsync_req)

    def __iter__(self):
        self._prepare_iter()

        if self.preallocate is True:
            yield hostevent.ControlEvent(operation=OP_FALLOCATE,
                    arg1=self.zone_size, arg2=self._falloc_keepsize_arg())

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)



class SSnake(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        self.write_iter = patterns.Snake(zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size, snake_size=self.snake_size)

        if self.fsync is True:
            fsync_req = get_fsync_req()
            self.write_iter = patterns.rep_and_mix(self.write_iter, fsync_req)

    def __iter__(self):
        self._prepare_iter()

        if self.preallocate is True:
            yield hostevent.ControlEvent(operation=OP_FALLOCATE,
                    arg1=self.zone_size, arg2=self._falloc_keepsize_arg())

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)



class SFadingSnake(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        # write half
        self.write_iter = patterns.FadingSnake(zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size, snake_size=self.snake_size)

        if self.fsync is True:
            fsync_req = get_fsync_req()
            self.write_iter = patterns.rep_and_mix(self.write_iter, fsync_req)

    def __iter__(self):
        self._prepare_iter()

        if self.preallocate is True:
            yield hostevent.ControlEvent(operation=OP_FALLOCATE,
                    arg1=self.zone_size, arg2=self._falloc_keepsize_arg())

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SStrided(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        # write half
        self.write_iter = patterns.Strided(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size, stride_size=self.stride_size)

        if self.fsync is True:
            fsync_req = get_fsync_req()
            self.write_iter = patterns.rep_and_mix(self.write_iter, fsync_req)

    def __iter__(self):
        self._prepare_iter()

        if self.preallocate is True:
            yield hostevent.ControlEvent(operation=OP_FALLOCATE,
                    arg1=self.zone_size, arg2=self._falloc_keepsize_arg())

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SHotNCold(SuiteBase):
    def _prepare_iter(self):
        chunk_size = self.chunk_size

        # write half
        self.write_iter = patterns.HotNCold(op=OP_WRITE, zone_offset=0,
                zone_size=self.zone_size, chunk_size=self.chunk_size,
                traffic_size=self.traffic_size)

        if self.fsync is True:
            fsync_req = get_fsync_req()
            self.write_iter = patterns.rep_and_mix(self.write_iter, fsync_req)

    def __iter__(self):
        self._prepare_iter()

        if self.preallocate is True:
            yield hostevent.ControlEvent(operation=OP_FALLOCATE,
                    arg1=self.zone_size, arg2=self._falloc_keepsize_arg())

        for req in self.write_iter:
            yield req

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


class SNoOp(SuiteBase):
    def _prepare_iter(self):
        pass

    def __iter__(self):
        self._prepare_iter()

        yield hostevent.ControlEvent(operation=OP_BARRIER)
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='gc_start_timestamp')

        yield hostevent.ControlEvent(operation=OP_CLEAN)


def get_fsync_req():
    return patterns.Request(OP_FSYNC, 0, 0)

