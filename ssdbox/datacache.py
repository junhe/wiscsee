import collections

from ftlsim_commons import *
import lrulist
import simpy

class Entry(object):
    def __init__(self, data, dirty):
        self.data = data
        self.dirty = dirty

    def values(self):
        return self.data, self.dirty

    def __str__(self):
        return "{}, {}".format(data, dirty)

class DataCache(object):
    """
    This is a general data cache that should suit the usage of
    most FTLs. The key is address (general any kind). The value
    """
    def __init__(self, max_n_entries, simpy_env):
        self.lrucache = lrulist.LruCache()
        self.env = simpy_env
        self.resource = simpy.Resource(self.env, capacity = 1)

    def evict_n_entries(self, n):
        """
        Find n victims and remove them from lrucache
        It returns a list of
         [ (lpn, entry), .... ]
        """
        victims = []
        while len(self.lrucache) > 0 and n > 0:
            lpn = self.lrucache.victim_key()
            victims.append( (lpn, self.get_entry(lpn)) )
            del self.lrucache[lpn]

            n -= 1

        return victims

    def has_lpn(self, lpn):
        return self.lrucache.has_key(lpn)

    def add_entry(self, lpn, data, dirty):
        self.lrucache[lpn] = Entry(data, dirty)

    def get_entry(self, lpn):
        return self.lrucache[lpn]

    def update_entry(self, lpn, data, dirty):
        self.add_entry(lpn, data, dirty)

    def del_entry(self, lpn):
        del self.lrucache[lpn]

    def append_lpn(self, extent_list, lpn, in_cache):
        if len(extent_list) == 0:
            extent_list.append( CacheExtent(lpn_start = lpn,
                lpn_count = 1, in_cache = in_cache) )
            return

        if extent_list[-1].next_lpn == lpn:
            extent_list[-1].lpn_count += 1
        else:
            extent_list.append( CacheExtent(lpn_start = lpn,
                lpn_count = 1, in_cache = in_cache) )

    def split_extent(self, lpn_start, lpn_count):
        """
        Split the lpn extent to subextent
        Each subextent has the same status (MISS/HIT) in data cache
        """
        # indexed by in_cache
        extents = {True: [], False: []}
        for lpn in range(lpn_start, lpn_start + lpn_count):
            in_cache = self.lrucache.has_key(lpn)
            self.append_lpn(extents[in_cache], lpn, in_cache)

        return extents[True] + extents[False]

    def __str__(self):
        return repr(self.lrucache)






















