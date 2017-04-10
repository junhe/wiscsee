import abc
import random

import config
import workload
from wiscsim import hostevent
from commons import *

from pyreuse.general.zipf import ZipfGenerator

class LBAWorkloadGenerator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
        return

class LBAMultiProcGenerator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_iter_list(self):
        return


class SampleWorkload(LBAWorkloadGenerator):
    def __init__(self, conf):
        self.conf = conf
        self.sector_size = self.conf['sector_size']

    def __iter__(self):
        yield hostevent.Event(sector_size=self.sector_size,
                pid=0, operation=OP_ENABLE_RECORDER,
                offset=0, size=0)

        yield hostevent.Event(sector_size=self.sector_size,
                pid=0, operation=OP_WRITE,
                offset=0, size=3*MB)


class TestWorkload(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

        self.sector_size = self.conf['sector_size']
        self.ftl_type = self.conf['ftl_type']
        if self.ftl_type == 'dftlext':
            self.over_provisioning = self.conf.over_provisioning
        elif self.ftl_type == 'nkftl':
            self.over_provisioning = self.conf['nkftl']['provision_ratio']
        else:
            raise RuntimeError("FTL type {} is not supported".format(
                self.ftl_type))

    def test_random(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        ops = [w, r, d]

        events = []
        maxpage = 0

        lba_span = int(self.conf.total_num_pages() / self.over_provisioning)

        for i in range(10):
            op = random.choice(ops)
            page = int(random.random() * lba_span)
            if maxpage < page:
                maxpage = page
            events.append( (op, page) )

        for page in range(10):
            events.append( (r, page) )

        return events

    def __iter__(self):
        yield hostevent.Event(sector_size = self.sector_size,
                pid = 0, operation = OP_ENABLE_RECORDER,
                offset = 0, size = 0)

        events = self.test_random()

        for op, lpn in events:
            offset = lpn * self.conf.page_size
            size = self.conf.page_size
            event = hostevent.Event(sector_size = self.sector_size,
                    pid = 0, operation = op, offset = offset,
                    size = size)
            yield event


class ExtentTestWorkload(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

        self.sector_size = self.conf['sector_size']
        self.ftl_type = self.conf['ftl_type']
        if self.ftl_type in ('dftlext', 'dftldes', 'dftldes'):
            self.over_provisioning = self.conf.over_provisioning
        elif self.ftl_type == 'nkftl2':
            self.over_provisioning = 1
        else:
            raise RuntimeError("FTL type {} is not supported".format(
                self.ftl_type))

        self.op_count = self.conf['lba_workload_configs']\
            ['ExtentTestWorkload']['op_count']
        if isinstance(self.conf, config.ConfigNewFlash):
            self.page_size = self.conf['flash_config']['page_size']
        else:
            self.page_size = self.conf.page_size

    def test_random(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        ops = [w, r, d]

        events = []
        maxpage = 0

        lba_span = int(self.conf.total_num_pages() / self.over_provisioning)
        print 'total num pages', self.conf.total_num_pages()
        print 'lba_span', lba_span

        max_access_pages = 16
        for i in range(self.op_count):
            op = random.choice(ops)
            page = int(random.random() * (lba_span - max_access_pages))
            npages = random.randint(1, max_access_pages)
            if maxpage < page:
                maxpage = page
            events.append( (op, page, npages) )

        for page in range(maxpage):
            events.append( (r, page, 1) )

        return events

    def __iter__(self):
        yield hostevent.Event(sector_size = self.sector_size,
                pid = 0, operation = OP_ENABLE_RECORDER,
                offset = 0, size = 0)

        events = self.test_random()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = hostevent.Event(sector_size = self.sector_size,
                    pid = 0, operation = op, offset = offset,
                    size = size)
            yield event

class TestWorkloadFLEX3(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

        self.sector_size = self.conf['sector_size']
        self.ftl_type = self.conf['ftl_type']
        if self.ftl_type in ('dftlext', 'dftldes', 'dftldes'):
            self.over_provisioning = self.conf.over_provisioning
        elif self.ftl_type == 'nkftl':
            self.over_provisioning = self.conf['nkftl']['provision_ratio']
        else:
            raise RuntimeError("FTL type {} is not supported".format(
                self.ftl_type))

        self.op_count = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX3']['op_count']
        self.extent_size = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX3']['extent_size']
        self.ops = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX3']['ops']
        self.mode = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX3']['mode']

        if isinstance(self.conf, config.ConfigNewFlash):
            self.page_size = self.conf['flash_config']['page_size']
        else:
            self.page_size = self.conf.page_size

    def generate_events(self):
        ops = self.ops

        events = []
        maxpage = 0

        logical_span = int(self.conf.total_num_pages() / self.over_provisioning)
        print 'Total num pages', self.conf.total_num_pages(), "pages",\
                self.conf.total_num_pages() * \
                self.conf['flash_config']['page_size'] / MB, "MB"
        print "Total blocks", self.conf.n_blocks_per_dev
        print "Total logical blocks", logical_span / \
                self.conf['flash_config']['n_pages_per_block']
        print 'Logical_span', logical_span, "pages", logical_span *\
                self.conf['flash_config']['page_size'] / MB, "MB"

        for i in range(self.op_count):
            op = random.choice(ops)
            if self.mode == 'random':
                page = int(random.random() * (logical_span - self.extent_size))
                assert page < logical_span
            elif self.mode == 'sequential':
                page = i * self.extent_size
            else:
                raise RuntimeError("{} not supported".format(self.mode))
            npages = self.extent_size
            events.append( (op, page, npages) )

        return events

    def __iter__(self):
        yield hostevent.Event(sector_size = self.sector_size,
                pid = 0, operation = OP_ENABLE_RECORDER,
                offset = 0, size = 0)

        events = self.generate_events()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = hostevent.Event(sector_size = self.sector_size,
                    pid = 0, operation = op, offset = offset,
                    size = size)
            yield event


class ExtentTestWorkloadFLEX2(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

        self.sector_size = self.conf['sector_size']
        self.ftl_type = self.conf['ftl_type']

        if self.ftl_type in ('dftlext', 'dftldes'):
            self.over_provisioning = self.conf.over_provisioning
        elif self.ftl_type == 'nkftl':
            self.over_provisioning = self.conf['nkftl']['provision_ratio']
        else:
            raise RuntimeError("FTL type {} is not supported".format(
                self.ftl_type))

        self.events = self.conf['lba_workload_configs']\
            ['ExtentTestWorkloadFLEX2']['events']
        if isinstance(self.conf, config.ConfigNewFlash):
            self.page_size = self.conf['flash_config']['page_size']
        else:
            self.page_size = self.conf.page_size

    def check(self):
        lba_span = int(self.conf.total_num_pages() / self.over_provisioning)
        print 'total num pages', self.conf.total_num_pages()
        print 'lba_span', lba_span

        ret = []
        for event in self.events:
            end_page = event[1] + event[2]
            assert end_page < self.conf.total_num_pages()
            ret.append(event)

        return ret

    def __iter__(self):
        yield hostevent.Event(sector_size = self.sector_size,
                pid = 0, operation = OP_ENABLE_RECORDER,
                offset = 0, size = 0)

        events = self.check()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = hostevent.Event(sector_size = self.sector_size,
                    pid = 0, operation = op, offset = offset,
                    size = size)
            yield event


class AccessesWithDist(LBAWorkloadGenerator):
    def __init__(self, conf):
        self.conf = conf
        self.sector_size = self.conf['sector_size']

        self.distribution = self.conf['AccessesWithDist']['lba_access_dist']
        self.traffic_size = self.conf['AccessesWithDist']['traffic_size']
        self.chunk_size = self.conf['AccessesWithDist']['chunk_size']
        self.space_size = self.conf['AccessesWithDist']['space_size']
        self.skew_factor = self.conf['AccessesWithDist']['skew_factor']
        self.zipf_alpha = self.conf['AccessesWithDist']['zipf_alpha']
        self.lbabytes = self.conf['dev_size_mb'] * MB


    def __iter__(self):
        yield hostevent.Event(sector_size=self.sector_size,
                pid=0, operation=OP_ENABLE_RECORDER,
                offset=0, size=0)

        # yield hostevent.Event(sector_size=self.sector_size,
                # pid=0, operation=OP_WRITE,
                # offset=0, size=3*MB)

        if self.distribution == 'uniform':
            for req in self.uniform_events():
                yield req

        elif self.distribution == 'hotcold':
            for req in self.hot_cold_space_event():
                yield req

        elif self.distribution == 'zipf':
            for req in self.zipf_events():
                yield req

        else:
            raise NotImplementedError('distribution {} not implemented'.format(
                self.distribution))

    def uniform_events(self):
        chunk_size = self.chunk_size
        traffic_size = self.traffic_size

        n_chunks_in_traffic = traffic_size / chunk_size
        n_chunks_in_space = self.space_size / chunk_size

        for i in range(n_chunks_in_traffic):
            chunk_id = random.randint(0, n_chunks_in_space - 1)
            yield self.get_write_event(chunk_id)


    def hot_cold_space_event(self):
        """
        first half cold, second half hot
        """
        chunk_size = self.chunk_size
        traffic_size = self.traffic_size

        n_chunks_in_traffic = traffic_size / chunk_size
        n_chunks_in_space = self.space_size / chunk_size
        n_chunks_in_half_space = int(n_chunks_in_space / 2)

        n_chunks_written = 0

        finished = False
        while finished is False:
            for chunk_id in range(n_chunks_in_half_space):
                yield self.get_write_event(chunk_id)
                n_chunks_written += 1
                if n_chunks_written > n_chunks_in_traffic:
                    finished = True
                    break

            if finished is True:
                break

            # write the second half.
            for i in range(self.skew_factor):
                for chunk_id in range(n_chunks_in_half_space, n_chunks_in_space):
                    yield self.get_write_event(chunk_id)
                    n_chunks_written += 1
                    if n_chunks_written > n_chunks_in_traffic:
                        finished = True
                        break
                if finished is True:
                    break

    def zipf_events(self):
        chunk_size = self.chunk_size
        traffic_size = self.traffic_size

        n_chunks_in_traffic = traffic_size / chunk_size
        n_chunks_in_space = self.space_size / chunk_size

        zipfgen = ZipfGenerator(n_chunks_in_space, self.zipf_alpha)

        for i in range(n_chunks_in_traffic):
            chunk_id = zipfgen.next()
            yield self.get_write_event(chunk_id)

    def get_write_event(self, chunk_id):
        offset = chunk_id * self.chunk_size
        size = self.chunk_size
        return hostevent.Event(sector_size=self.sector_size,
            pid=0, operation=OP_WRITE, offset=offset, size=size)


class BarrierGen(object):
    def __init__(self, n_ncq_slots):
        self.n_ncq_slots = n_ncq_slots

    def barrier_events(self):
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)
        yield hostevent.ControlEvent(operation=OP_BARRIER)
        for i in range(self.n_ncq_slots):
            yield hostevent.ControlEvent(operation=OP_NOOP)


class BlktraceEvents(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

        self.mkfs_event_path = self.conf['lba_workload_configs']\
                ['mkfs_event_path']
        self.ftlsim_event_path = self.conf['lba_workload_configs']\
                ['ftlsim_event_path']

        self.stop_on_bytes = self.conf['stop_sim_on_bytes']

        if str(self.stop_on_bytes).lower() in ('inf', 'infinity', 'infinit'):
            self.stop_on_bytes = float('inf')

    def __iter__(self):
        barriergen = BarrierGen(self.conf.ssd_ncq_depth())

        yield hostevent.ControlEvent(operation=OP_DISABLE_RECORDER)

        # mkfs events
        for event in self.prepfs_events():
            yield event

        # target workload event
        for event in self.target_workload_events():
            yield event

        # may send gc trigger
        for event in self.gc_event():
            yield event

        for req in barriergen.barrier_events():
            yield req
        yield hostevent.ControlEvent(operation=OP_REC_BW)

    def prepfs_events(self):
        prepfs_line_iter = hostevent.FileLineIterator(self.mkfs_event_path)
        event_prepfs_iter = hostevent.EventIterator(self.conf, prepfs_line_iter)

        for event in event_prepfs_iter:
            yield event

    def target_workload_events(self):
        # special event indicates the start of workload
        barriergen = BarrierGen(self.conf.ssd_ncq_depth())
        yield hostevent.ControlEvent(operation=OP_ENABLE_RECORDER)
        for req in barriergen.barrier_events():
            yield req
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_start')

        workload_line_iter = hostevent.FileLineIterator(self.ftlsim_event_path)
        event_workload_iter = hostevent.EventIterator(self.conf, workload_line_iter)

        total_rw_bytes = 0
        for event in event_workload_iter:
            yield event

            if event.operation in [OP_READ, OP_WRITE] and event.action == 'D':
                total_rw_bytes += event.size

                if total_rw_bytes >= self.stop_on_bytes:
                    print 'break! stop on ', self.stop_on_bytes/MB
                    break

        for req in barriergen.barrier_events():
            yield req
        yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                arg1='interest_workload_end')

    def gc_event(self):
        barriergen = BarrierGen(self.conf.ssd_ncq_depth())
        if self.conf['do_gc_after_workload'] is True:
            for req in barriergen.barrier_events():
                yield req
            yield hostevent.ControlEvent(operation=OP_REC_TIMESTAMP,
                    arg1='gc_start_timestamp')

            yield hostevent.ControlEvent(operation=OP_CLEAN)



