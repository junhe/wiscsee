from ftlsim_commons import Extent
from commons import *

class HostEventBase(object):
    def get_operation(self):
        raise NotImplementedError

    def get_type(self):
        raise NotImplementedError


class ControlEvent(HostEventBase):
    def __init__(self, operation, arg1=None, arg2=None, arg3=None):
        self.operation = operation
        self.arg1 = arg1
        self.arg2 = arg2
        self.arg3 = arg3
        self.action = 'D' # following the format of data event

    def get_operation(self):
        return self.operation

    def get_type(self):
        return 'ControlEvent'

    def __str__(self):
        return "ControlEvent: {}: {}, {}, {}".format(self.operation,
                self.arg1, self.arg2, self.arg3)


class Event(HostEventBase):
    def __init__(self, sector_size, pid, operation, offset, size,
            timestamp = None, pre_wait_time = None, sync = True, action = 'D'):
        self.pid = int(pid)
        self.operation = operation
        self.offset = int(offset)
        self.size = int(size)
        self.sync = sync
        self.timestamp = timestamp
        self.pre_wait_time = pre_wait_time
        self.action = action
        assert action in ('D', 'C'), "action:{}".format(action)

        assert self.offset % sector_size == 0,\
            "offset {} is not aligned with sector size {}.".format(
            self.offset, sector_size)
        self.sector = self.offset / sector_size

        assert self.size % sector_size == 0, \
            "size {} is not multiple of sector size {}".format(
            self.size, sector_size)

        self.sector_count = self.size / sector_size

    def get_operation(self):
        return self.operation

    def get_type(self):
        return 'Event'

    def get_lpn_extent(self, conf):
        lpn_start, lpn_count = conf.off_size_to_page_range(
                self.offset, self.size, force_alignment=False)
        return Extent(lpn_start = lpn_start, lpn_count = lpn_count)

    def __str__(self):
        return "Event pid:{pid}, operation:{operation}, offset:{offset}, "\
                "size:{size}, sector:{sector}, sector_count:{sector_count}, "\
                "sync:{sync}, timestamp:{timestamp}, action:{action}"\
                .format(pid = self.pid, operation = self.operation,
                        offset = self.offset, size = self.size,
                        sector = self.sector, sector_count = self.sector_count,
                        sync = self.sync, timestamp = self.timestamp,
                        action = self.action)


class FileLineIterator(object):
    def __init__(self, file_path):
        self.file_path = file_path

    def __iter__(self):
        with open(self.file_path, 'r') as f:
            for line in f:
                line = line.strip()
                yield line


class EventIterator(object):
    """
    Convert string line to event, and iter
    """
    def __init__(self, conf, filelineiter):
        self.conf = conf
        self.sector_size = self.conf['sector_size']
        self.filelineiter = filelineiter
        self.event_file_column_names = self.conf['event_file_column_names']

        self._translation = {'read': OP_READ, 'write': OP_WRITE,
                'discard':OP_DISCARD}

    def _convert(self, op_in_file):
        return self._translation[op_in_file]

    def str_to_event(self, line):
        items = line.split()
        if len(self.event_file_column_names) != len(items):
            raise RuntimeError("Lengths not equal: {} {}".format(
                self.event_file_column_names, items))
        dic = dict(zip(self.event_file_column_names, items))
        dic['sector_size'] = self.sector_size
        if dic['pre_wait_time'] != 'NA':
            dic['pre_wait_time'] = float(dic['pre_wait_time'])

        dic['operation'] = self._convert(dic['operation'])

        return Event(**dic)

    def __iter__(self):
        for line in self.filelineiter:
            yield self.str_to_event(line)


