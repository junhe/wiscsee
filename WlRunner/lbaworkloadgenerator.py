import abc
import random

import config
import workload
from FtlSim import simulator
from commons import *

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
        w = 'write'
        r = 'read'
        d = 'discard'
        events = [
                (w, 1),
                (w, 1),
                (r, 1)
                ]

        return events

    def test2(self):
        w = 'write'
        r = 'read'
        d = 'discard'
        events = [
                (r, 1)
                ]

        return events

    def test3(self):
        """
        To trigger switch merge
        """
        w = 'write'
        r = 'read'
        d = 'discard'
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
        w = 'write'
        r = 'read'
        d = 'discard'

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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
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
            event = simulator.Event(sector_size = self.sector_size,
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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        events = self.test_random()

        for op, lpn in events:
            offset = lpn * self.conf.page_size
            size = self.conf.page_size
            event = simulator.Event(sector_size = self.sector_size,
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
        elif self.ftl_type == 'nkftl':
            self.over_provisioning = self.conf['nkftl']['provision_ratio']
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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        events = self.test_random()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = simulator.Event(sector_size = self.sector_size,
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        events = self.generate_events()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = simulator.Event(sector_size = self.sector_size,
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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        events = self.test_random()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = simulator.Event(sector_size = self.sector_size,
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
        w = 'write'
        r = 'read'
        d = 'discard'
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        events = self.test_random()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = simulator.Event(sector_size = self.sector_size,
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        events = self.test_random()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = simulator.Event(sector_size = self.sector_size,
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        events = self.check()

        for op, lpn, npages in events:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = simulator.Event(sector_size = self.sector_size,
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
        yield simulator.Event(sector_size = self.sector_size,
                pid = 0, operation = 'enable_recorder',
                offset = 0, size = 0)

        for op, lpn, npages in raw_list:
            offset = lpn * self.page_size
            size = self.page_size * npages
            event = simulator.Event(sector_size = self.sector_size,
                    pid = 0, operation = op, offset = offset,
                    size = size)
            yield event


