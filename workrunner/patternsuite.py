from commons import *
from accpatterns import patterns

class Random001(object):
    def __init__(self, conf):
        self.conf = conf

    def __iter__(self):
        pat_iter = patterns.Random(op=patterns.READ, zone_offset=0,
                zone_size=2048*10,
                chunk_size=2048, traffic_size=2048*10)
        for req in pat_iter:
            yield req


