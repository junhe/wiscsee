import abc
import random

import config
import workload

class LBAWorkloadGenerator(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __iter__(self):
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
            offset = page * self.conf['flash_page_size']
            size = self.conf["flash_page_size"]
            event = 'write {} {}'.format(offset, size)
            yield event

        for i in range(maxpage):
            page = i
            offset = page * self.conf['flash_page_size']
            size = self.conf["flash_page_size"]
            event = 'read {} {}'.format(offset, size)
            yield event

class Manual(LBAWorkloadGenerator):
    def __init__(self, confobj):
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not config.Config. It is {}".
                format(type(confobj).__name__))
        self.conf = confobj

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
        # ops = [w, r, d]
        ops = [w]

        events = []
        maxpage = 0

        # block_span = self.conf.nkftl_allowed_num_of_data_blocks()
        # lba_span = block_span * self.conf['flash_npage_per_block']
        lba_span = int(self.conf.total_num_pages() / \
            self.conf['nkftl']['provision_ratio'])

        print "Number of flash blocks:", self.conf['flash_num_blocks']
        print "Number of pages:", self.conf.total_num_pages()
        print "LBA span of lba:", lba_span
        print "LBA span of lba (MB):", lba_span * self.conf['flash_page_size'] / float(2**20)
        print "LBA span in blocks", lba_span/self.conf['flash_npage_per_block']
        print "flash_npage_per_block:", self.conf['flash_npage_per_block']

        for i in range(lba_span * 50):
            op = random.choice(ops)
            page = int(random.random() * lba_span)
            if maxpage < page:
                maxpage = page
            events.append( (op, page) )

        for page in range(maxpage):
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

        for i in range(lba_span * 4):
            op = random.choice(ops)
            page = int(random.random() * lba_span)
            if maxpage < page:
                maxpage = page
            events.append( (op, page) )

        for page in range(maxpage):
            events.append( (r, page) )

        return events

    def __iter__(self):
        yield "enable_recorder 0 0"

        # events = self.test1410()
        # events = self.test3()
        # events = self.test2()
        # events = self.test1()
        # events = self.test_random()
        events = self.test_random_dftl()

        for op, lpn in events:
            offset = lpn * self.conf['flash_page_size']
            size = self.conf["flash_page_size"]
            event = '{op} {off} {sz}'.format(op = op, off = offset, sz = size)
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
            offset = page * self.conf['flash_page_size']
            size = self.conf["flash_page_size"]
            event = 'write {} {}'.format(offset, size)
            yield event

        for i in range(maxpage):
            page = i
            offset = page * self.conf['flash_page_size']
            size = self.conf["flash_page_size"]
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

            offset = page * self.conf['flash_page_size']
            size = self.conf["flash_page_size"] * size_in_page
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


