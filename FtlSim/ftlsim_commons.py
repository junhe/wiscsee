class Extent(object):
    def __init__(self, lpn_start, lpn_count):
        self.lpn_start = lpn_start
        self.lpn_count = lpn_count

    @property
    def next_lpn(self):
        return self.lpn_start + self.lpn_count

    def lpn_iter(self):
        return range(self.lpn_start, self.lpn_start + self.lpn_count)

    def __str__(self):
        return "lpn_start: {}, lpn_count: {}".format(
                self.lpn_start, self.lpn_count)

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

def create_ssd_request_with_cache(conf, event, in_cache):
    req = create_ssd_request(conf, event)
    req.in_cache(in_cache)
    return req




