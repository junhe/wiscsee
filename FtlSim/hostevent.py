import simulator

class EventSimple(object):
    def __init__(self, pid, operation):
        self.pid = int(pid)
        self.operation = operation

    def __str__(self):
        return "EventSimple PID:{} OP:{}".format(self.pid, self.operation)


class Event(object):
    def __init__(self, sector_size, pid, operation, offset, size,
            timestamp = None, pre_wait_time = None, sync = True):
        self.pid = int(pid)
        self.operation = operation
        self.offset = int(offset)
        self.size = int(size)
        self.sync = sync
        self.timestamp = timestamp
        self.pre_wait_time = pre_wait_time

        assert self.offset % sector_size == 0,\
            "offset {} is not aligned with sector size {}.".format(
            self.offset, sector_size)
        self.sector = self.offset / sector_size

        assert self.size % sector_size == 0, \
            "size {} is not multiple of sector size {}".format(
            self.size, sector_size)

        self.sector_count = self.size / sector_size

    def __str__(self):
        return "Event pid:{pid}, operation:{operation}, offset:{offset}, "\
                "size:{size}, sector:{sector}, sector_count:{sector_count}, "\
                "sync:{sync}"\
                .format(pid = self.pid, operation = self.operation,
                        offset = self.offset, size = self.size,
                        sector = self.sector, sector_count = self.sector_count,
                        sync = self.sync)


class FileLineIterator(object):
    def __init__(self, file_path):
        self.file_path = file_path

    def __iter__(self):
        with open(self.file_path, 'r') as f:
            for line in f:
                line = line.strip()
                yield line


class EventIterator(object):
    def __init__(self, conf, filelineiter):
        self.conf = conf
        self.sector_size = self.conf['sector_size']
        self.filelineiter = filelineiter
        self.event_file_columns = self.conf['event_file_columns']

    def str_to_event(self, line):
        items = line.split()
        if len(self.event_file_columns) != len(items):
            raise RuntimeError("Lengths not equal: {} {}".format(
                self.event_file_columns, items))
        dic = dict(zip(self.event_file_columns, items))
        dic['sector_size'] = self.sector_size
        dic['pre_wait_time'] = float(dic['pre_wait_time'])

        return Event(**dic)
        # return Event(sector_size = self.sector_size,
                # pid = items[0], operation = items[1], offset = items[2],
                # size = items[3])

    def __iter__(self):
        for line in self.filelineiter:
            yield self.str_to_event(line)


