class BlockClassifierBase(object):
    def classify(self, offset):
        """
        Given an offset set, tell the semantics of data stored in it.
        """
        raise NotImplementedError()

class Ext4BlockClassifier(BlockClassifierBase):
    def __init__(self, range_table, blocksize=4096):
        """
        range_table is
        [
            {'inode': (startblock, endblock)}, e.g. (34, 34)
            {'journal': (startblock, endblock)},
             ...
        ]
        """
        self._range_table = range_table
        self._blocksize = blocksize

    def classify(self, offset):
        blocknum = offset / self._blocksize

        for row in self._range_table:
            for category, (start, end) in row.items():
                if blocknum >= start and blocknum <= end:
                    return category

        return 'UNKNOWN'


class OffsetClassifier(BlockClassifierBase):
    def __init__(self, range_table):
        """
        range_table is
        [
            {'inode': (start offset, end offset)}, e.g. (0, 4096)
            {'journal': (start offset, end offst)},
             ...
        ]
        """
        self._range_table = range_table

    def classify(self, offset):
        for row in self._range_table:
            for category, (start, end) in row.items():
                if offset >= start and offset < end:
                    return category

        return 'UNKNOWN'


class Ext4FileClassifier(BlockClassifierBase):
    def __init__(self, extents, blocksize=4096):
        """
        extents is a list of extents from get_extents_of_dir()
        """
        self._extents = extents
        self._blocksize = blocksize

        self._add_offsets(self._extents)

    def _add_offsets(self, extents):
        blocksize = self._blocksize
        for extent in extents:
            extent['physical_range'] = (extent['Physical_start'] * blocksize,
                    (extent['Physical_end'] + 1) * blocksize)
            extent['logical_range'] = (extent['Logical_start'] * blocksize,
                    (extent['Logical_end'] + 1) * blocksize)

        return extents

    def classify(self, offset):
        return self._find_file_of_offset(offset, self._extents)

    def _find_file_of_offset(self, offset, extents):
        for extent in extents:
            if self._is_physical_in_extent(offset, extent) is True:
                return extent['file_path']

        return None

    def _is_physical_in_extent(self, offset, extent):
        return offset >= extent['physical_range'][0] and \
                offset < extent['physical_range'][1]



