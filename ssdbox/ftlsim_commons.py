import simpy
import random

class Extent(object):
    def __init__(self, lpn_start, lpn_count):
        assert lpn_count > 0
        self.lpn_start = lpn_start
        self.lpn_count = lpn_count

    @property
    def next_lpn(self):
        return self.lpn_start + self.lpn_count

    def last_lpn(self):
        return self.end_lpn() - 1

    def end_lpn(self):
        return self.lpn_start + self.lpn_count

    def lpn_iter(self):
        return range(self.lpn_start, self.end_lpn())

    def __str__(self):
        return "lpn_start: {}, lpn_count: {}".format(
                self.lpn_start, self.lpn_count)

    def __contains__(self, lpn):
        return lpn >= self.lpn_start and lpn < self.end_lpn()

    def __copy__(self):
        return Extent(self.lpn_start, self.lpn_count)


class CacheExtent(Extent):
    def __init__(self, lpn_start, lpn_count, in_cache):
        super(CacheExtent, self).__init__(lpn_start, lpn_count)
        self.in_cache = in_cache

    def __str__(self):
        return "{}, in_cache: {}".format(
            super(CacheExtent, self).__str__(), self.in_cache)


def display_extents(extent_list):
    for ext in extent_list:
        print str(ext)


class SSDRequest(CacheExtent):
    def __init__(self, lpn_start, lpn_count, in_cache, operation):
        super(CacheExtent, self).__init__(lpn_start, lpn_count)
        self.operation = operation

    def __str__(self):
        return "{}, operation: {}".format(
            super(CacheExtent, self).__str__(), self.operation)


def create_ssd_request(conf, event):
    lpn_start, lpn_count = conf.sec_ext_to_page_ext(
            event.sector, event.sector_count)
    return SSDRequest(
            lpn_start,
            lpn_count,
            None,
            event.operation)


class NCQSingleQueue(object):
    """
    User of the queue can take up to depth # of request without
    returning
    """
    def __init__(self, ncq_depth, simpy_env):
        self.ncq_depth = ncq_depth
        self.env = simpy_env
        self.queue = simpy.Store(self.env)
        # ssd need to grab a slot before get item from queue
        self.slots = simpy.Resource(self.env, capacity=ncq_depth)

    def hold_all_slots(self):
        held_slot_reqs = []
        for i in range(self.ncq_depth):
            slot_req  = self.slots.request()
            held_slot_reqs.append(slot_req)

        yield simpy.events.AllOf(self.env, held_slot_reqs)

        self.env.exit(held_slot_reqs)

    def release_all_slots(self, held_slot_reqs):
        """Must be used in pair with hold_all_slots()"""
        assert len(held_slot_reqs) > 0
        for req in held_slot_reqs:
            self.slots.release(req)


def split_ext_by_segment(n_pages_per_segment, extent):
    if extent.lpn_count == 0:
        return None

    last_seg_id = -1
    cur_ext = None
    exts = {}
    for lpn in extent.lpn_iter():
        seg_id = lpn / n_pages_per_segment
        if seg_id == last_seg_id:
            cur_ext.lpn_count += 1
        else:
            if cur_ext is not None:
                exts[last_seg_id] = cur_ext
            cur_ext = Extent(lpn_start=lpn, lpn_count=1)
        last_seg_id = seg_id

    if cur_ext is not None:
        exts[seg_id] = cur_ext

    return exts


class LockPool(object):
    def __init__(self, simpy_env):
        self.resources = {} # addr: lock
        self.env = simpy_env
        self.locked_addrs = set()

    def get_request(self, addr):
        res = self.resources.setdefault(addr,
                                    simpy.Resource(self.env, capacity = 1))
        return res.request()

    def release_request(self, addr, request):
        res = self.resources[addr]
        res.release(request)


random.seed(1)

def random_channel_id(n_channels_per_dev):
    return random.randint(0, n_channels_per_dev - 1)





