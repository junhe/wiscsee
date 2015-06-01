

secsize = 512

def convert(fpath):
    """
    89966527950584 4253 nfsd 508516672 8 W 6 0 88b93b628d84082186026d9da044f173
    0              1    2    3         4 5 6 7 8
    """
    with open(fpath, 'r') as f:
        cnt = 0
        maxbyte = 0
        for line in f:
            items = line.split()
            offset = int(items[3]) * secsize
            size   = int(items[4]) * secsize
            print 'write', offset, size
            if offset > maxbyte:
                maxbyte = offset

            cnt += 1
            if cnt > 10000:
                break
    # print maxbyte

convert('./../analysis/data/webresearch/webresearch-030409-033109.9.blkparse')

