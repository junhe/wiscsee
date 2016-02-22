import lrulist

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
    def __init__(self, max_n_entries):
        self.lrucache = lrulist.LruCache()

    def evict_n_entries(self, n):
        pass

    def add_entry(self, lpn, data, dirty):
        self.lrucache[lpn] = Entry(data, dirty)

    def get_entry(self, lpn):
        return self.lrucache[lpn].values()

    def update_entry(self, lpn, data, dirty):
        self.add_entry(lpn, data, dirty)

    def del_entry(self, lpn):
        del self.lrucache[lpn]

    def __str__(self):
        return repr(self.lrucache)


