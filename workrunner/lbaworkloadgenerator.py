import abc
import random

import config
import workload
from ssdbox import hostevent
from commons import *
from accpatterns import patterns
from patternsuite import *
from accpatterns.contractbench import *

import prepare4pyreuse
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


class Sequential(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

    def __iter__(self):
        n_flash_pages = self.conf.total_num_pages()
        n_lba_pages = int(n_flash_pages * \
            self.conf["LBA"]["lba_to_flash_size_ratio"])

        n_accesses = int(n_lba_pages * self.conf['LBA']['write_to_lba_ratio'])
        maxpage = 0
        for i in range(n_accesses):
            page = i % n_lba_pages # restrict to lba space
            if maxpage < page:
                maxpage = page
            offset = page * self.conf.page_size
            size = self.conf.page_size
            event = 'write {} {}'.format(offset, size)
            yield event

        for i in range(maxpage):
            page = i
            offset = page * self.conf.page_size
            size = self.conf.page_size
            event = 'read {} {}'.format(offset, size)
            yield event

class Manual(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

        self.sector_size = self.conf['sector_size']

    def test1(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        events = [
                (w, 1),
                (w, 1),
                (r, 1)
                ]

        return events

    def test2(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        events = [
                (r, 1)
                ]

        return events

    def test3(self):
        """
        To trigger switch merge
        """
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        events = []

        maxi = 0
        for i in range(34):
            events.append((w, i))
            if i > maxi:
                maxi = i

        for i in range(0, maxi + 1):
            events.append((r, i))

        return events

    def test33(self):
        """
        To trigger partial merge
        """
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD

        events = [
                (w, 9),
                (w, 3),
                (w, 3),
                (w, 3),
                (w, 0),
                (w, 1),
                (w, 2),
                ]

        maxi = 0
        for i in range(0, maxi + 1):
            events.append((r, i))

        return events

    def test4(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        events = [
                (w, 1),
                (d, 1),
                (r, 1),
                (d, 2),
                (w, 2),
                (r, 2)
                ]

        return events

    def test1410(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        events = [
                (w, 1),
                (r, 1),
                (w, 1),
                (r, 1),
                (d, 1),
                (r, 1)
                ]

        return events

    def test_random_dftl(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        ops = [w, r, d]

        events = []
        maxpage = 0

        # block_span = self.conf.nkftl_allowed_num_of_data_blocks()
        # lba_span = block_span * self.conf['flash_npage_per_block']
        lba_span = int(self.conf.total_num_pages() / \
            self.conf['nkftl']['provision_ratio'])

        print "Number of flash blocks:", self.conf['flash_num_blocks']
        print "Number of pages:", self.conf.total_num_pages()
        print "LBA span of lba:", lba_span
        print "LBA span of lba (MB):", lba_span * self.conf.page_size / float(2**20)
        print "LBA span in blocks", lba_span/self.conf['flash_npage_per_block']
        print "flash_npage_per_block:", self.conf['flash_npage_per_block']

        for i in range(10):
            op = random.choice(ops)
            page = int(random.random() * lba_span)
            if maxpage < page:
                maxpage = page
            events.append( (op, page) )

        for page in range(10):
            events.append( (r, page) )

        return events

    def test_random(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        ops = [w, r, d]

        events = []
        maxpage = 0

        # block_span = self.conf.nkftl_allowed_num_of_data_blocks()
        # lba_span = block_span * self.conf['flash_npage_per_block']
        lba_span = int(self.conf.total_num_pages() / \
            self.conf['nkftl']['provision_ratio'])

        print "Number of flash blocks:", self.conf['flash_num_blocks']
        print "Number of pages:", self.conf.total_num_pages()
        print "LBA span of lba:", lba_span
        print "LBA span in blocks", lba_span/self.conf['flash_npage_per_block']
        print "flash_npage_per_block:", self.conf['flash_npage_per_block']
        print "n_blocks_in_data_group:", \
            self.conf['nkftl']['n_blocks_in_data_group']
        print "max_blocks_in_log_group:",\
            self.conf['nkftl']['max_blocks_in_log_group']
        print "GC trigger blocks:", self.conf['flash_num_blocks'] * \
            self.conf['nkftl']['GC_threshold_ratio']

        for i in range(int(lba_span * 4)):
            op = random.choice(ops)
            page = int(random.random() * lba_span)
            if maxpage < page:
                maxpage = page
            events.append( (op, page) )

        for page in range(maxpage):
            events.append( (r, page) )

        return events

    def __iter__(self):
        yield hostevent.Event(sector_size = self.sector_size,
                pid = 0, operation = OP_ENABLE_RECORDER,
                offset = 0, size = 0)

        # events = self.test1410()
        # events = self.test3()
        # events = self.test2()
        # events = self.test1()
        # events = self.test_random()
        events = self.test_random_dftl()

        for op, lpn in events:
            offset = lpn * self.conf.page_size
            size = self.conf.page_size
            event = hostevent.Event(sector_size = self.sector_size,
                    pid = 0, operation = op, offset = offset,
                    size = size)
            yield event

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


class TestWorkloadFLEX4(LBAWorkloadGenerator):
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
            ['TestWorkloadFLEX4']['op_count']
        self.extent_size = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX4']['extent_size']
        self.ops = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX4']['ops']
        self.mode = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX4']['mode']
        self.scope_size = self.conf['lba_workload_configs']\
            ['TestWorkloadFLEX4']['scope_size']

        if isinstance(self.conf, config.ConfigNewFlash):
            self.page_size = self.conf['flash_config']['page_size']
        else:
            self.page_size = self.conf.page_size

    def generate_events(self):
        ops = self.ops

        events = []
        maxpage = 0

        n_chunks = self.scope_size / self.extent_size

        for i in range(self.op_count):
            op = random.choice(ops)
            if self.mode == 'random':
                page = random.choice(range(n_chunks)) * self.extent_size
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



class ExtentTestWorkloadMANUAL(LBAWorkloadGenerator):
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
            ['ExtentTestWorkloadMANUAL']['op_count']
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
        # print 'total num pages', self.conf.total_num_pages()
        # print 'lba_span', lba_span

        # events.append( (op, page, npages) )
        events.append( (w, 0, 8) )
        events.append( (r, 0, 8) )

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

class ExtentTestWorkload4DFTLDES(LBAWorkloadGenerator):
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
            ['ExtentTestWorkload4DFTLDES']['op_count']
        if isinstance(self.conf, config.ConfigNewFlash):
            self.page_size = self.conf['flash_config']['page_size']
        else:
            self.page_size = self.conf.page_size

    def test_random(self):
        w = OP_WRITE
        r = OP_READ
        d = OP_DISCARD
        ops = [w, r]

        events = []
        maxpage = 0

        lba_span = int(self.conf.total_num_pages() / self.over_provisioning)
        print 'total num pages', self.conf.total_num_pages()
        print 'lba_span', lba_span

        max_access_pages = 2
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

class ExtentTestWorkloadFLEX(LBAWorkloadGenerator):
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
            ['ExtentTestWorkloadFLEX']['op_count']
        self.ops = self.conf['lba_workload_configs']\
            ['ExtentTestWorkloadFLEX']['ops']
        if isinstance(self.conf, config.ConfigNewFlash):
            self.page_size = self.conf['flash_config']['page_size']
        else:
            self.page_size = self.conf.page_size

    def test_random(self):
        events = []
        maxpage = 0

        lba_span = int(self.conf.total_num_pages() / self.over_provisioning)
        print 'total num pages', self.conf.total_num_pages()
        print 'lba_span', lba_span

        max_access_pages = 2
        for i in range(self.op_count):
            op = random.choice(self.ops)
            page = int(random.random() * (lba_span - max_access_pages))
            npages = random.randint(1, max_access_pages)
            if maxpage < page:
                maxpage = page
            events.append( (op, page, npages) )

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


class Random(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

    def __iter__(self):
        yield "enable_recorder 0 0"

        n_flash_pages = self.conf.total_num_pages()
        n_lba_pages = int(n_flash_pages * \
            self.conf["LBA"]["lba_to_flash_size_ratio"])

        n_accesses = int(n_lba_pages * self.conf['LBA']['write_to_lba_ratio'])
        n_accesses = 1000
        random.seed(1)
        maxpage = 0
        print ['random'] * 10
        for i in range(n_accesses):
            page = int(random.random() * 10*2**20/4096) # restrict to lba space
            if maxpage < page:
                maxpage = page
            offset = page * self.conf.page_size
            size = self.conf.page_size
            event = 'write {} {}'.format(offset, size)
            yield event

        for i in range(maxpage):
            page = i
            offset = page * self.conf.page_size
            size = self.conf.page_size
            event = 'read {} {}'.format(offset, size)
            yield event

class SeqWithRandomStart(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

    def __iter__(self):
        yield "enable_recorder 0 0"

        n_flash_pages = self.conf.total_num_pages()
        n_lba_pages = int(n_flash_pages * \
            self.conf["LBA"]["lba_to_flash_size_ratio"])

        n_accesses = int(n_lba_pages * self.conf['LBA']['write_to_lba_ratio'])
        random.seed(1)
        maxpage = 0
        for i in range(n_accesses):
            page = int(random.random() * n_lba_pages) # restrict to lba space
            size_in_page = 20
            if  page + size_in_page > n_lba_pages:
                size_in_page = n_lba_pages - page

            offset = page * self.conf.page_size
            size = self.conf.page_size * size_in_page
            event = 'write {} {}'.format(offset, size)
            print event
            yield event

class HotCold(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

    def __iter__(self):
        chunk_bytes = self.conf['LBA']['HotCold']['chunk_bytes']
        chunk_count = self.conf['LBA']['HotCold']['chunk_count']
        n_col = self.conf['LBA']['HotCold']['n_col']
        for chunkid in workload.Bricks(n_col, chunk_count):
            offset = chunkid * chunk_bytes
            size = chunk_bytes
            event = 'write {} {}'.format(offset, size)
            yield event


class MultipleProcess(LBAMultiProcGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

        self.sector_size = self.conf['sector_size']
        self.ftl_type = self.conf['ftl_type']

        self.over_provisioning = self.conf.over_provisioning

        # [
        #   [(xx, xx x), (xx, x, xxx), ...],
        #   [(xx, xx x), (xx, x, xxx), ...],
        #   ...
        # ]
        self.events = self.conf['lba_workload_configs']\
            ['MultipleProcess']['events']
        if isinstance(self.conf, config.ConfigNewFlash):
            self.page_size = self.conf['flash_config']['page_size']
        else:
            self.page_size = self.conf.page_size

    def get_iter_list(self):
        ret = []
        for raw_list in self.events:
            ret.append( self.get_iter(raw_list) )

        return ret

    def get_iter(self, raw_list):
        yield hostevent.Event(sector_size = self.sector_size,
                pid = 0, operation = OP_ENABLE_RECORDER,
                offset = 0, size = 0)

        for op, lpn, npages in raw_list:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = hostevent.Event(sector_size = self.sector_size,
                    pid = 0, operation = op, offset = offset,
                    size = size)
            yield event


class PatternAdapter(LBAWorkloadGenerator):
    def __init__(self, conf):
        self.conf = conf
        self.sector_size = self.conf['sector_size']

        pattern_class_name = \
            self.conf['lba_workload_configs']['PatternAdapter']['class']
        pattern_class = eval(pattern_class_name)
        self.pattern_iter = pattern_class(
            **self.conf['lba_workload_configs']['PatternAdapter']['conf'])

    def __iter__(self):
        yield hostevent.Event(sector_size=self.sector_size,
                pid=0, operation=OP_ENABLE_RECORDER,
                offset=0, size=0)

        for req in self.pattern_iter:
            if isinstance(req, hostevent.HostEventBase):
                yield req
            else:
                # need converting
                yield hostevent.Event(sector_size=self.sector_size,
                    pid=0, operation=req.op,
                    offset=req.offset, size=req.size)


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


class ContractBenchAdapter(LBAWorkloadGenerator):
    def __init__(self, conf):
        self.conf = conf
        self.sector_size = self.conf['sector_size']

        bench_name = \
            self.conf['lba_workload_configs']['ContractBench']['class']
        pattern_class = eval(bench_name)
        self.iterator = pattern_class(
            **self.conf['lba_workload_configs']['ContractBench']['conf'])

    def __iter__(self):
        yield hostevent.Event(sector_size=self.sector_size,
                pid=0, operation=OP_ENABLE_RECORDER,
                offset=0, size=0)

        for req in self.iterator:
            if isinstance(req, hostevent.HostEventBase):
                yield req
            else:
                # need converting
                yield hostevent.Event(sector_size=self.sector_size,
                    pid=0, operation=req.op,
                    offset=req.offset, size=req.size)


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



