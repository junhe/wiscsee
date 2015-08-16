"""
Parse btrfs debug tree to get the bytenr of various elements.
"""

import collections
import re
import pprint

import utils

def table_to_str(table, adddic = None, sep = ';'):
    if len(table) == 0:
        return None

    tablestr = ''
    colnames = table[0].keys()
    if adddic != None:
        colnames += adddic.keys()
    colnamestr = sep.join([adjust_width(s) for s in colnames]) + '\n'
    tablestr += colnamestr
    for row in table:
        if adddic != None:
            rowcopy = dict(row.items() + adddic.items())
        else:
            rowcopy = row
        rowstr = [rowcopy[k] for k in colnames]
        rowstr = [adjust_width(str(x)) for x in rowstr]
        rowstr = sep.join(rowstr) + '\n'
        tablestr += rowstr

    return tablestr


class Extent(object):
    def __init__(self, elem_type, start, size):
        self.elem_type = elem_type
        self.start = start
        self.size = size

    def to_dict(self):
        d = {'elem_type': self.elem_type,
             'start'    : self.start,
             'size'     : self.size}
        return d

    def __str__(self):
        return ','.join([self.elem_type, self.start, self.size])

    def __str__(self):
        return 'Extent({}, {}, {})'.format(self.elem_type, self.start, self.size)

Item = collections.namedtuple('Item',
    ['item_no', 'objectid', 'typeid', 'offset', 'itemoff', 'itemsize'])

def pre_space_count(line):
    """
    Find the number of preceeding spaces of the line
    """
    return len(line) - len(line.lstrip(' \t'))

def is_indented(line):
    """
    True: having one or more space
    False: having no space
    """
    return pre_space_count(line) > 0

def tree_name(chunklines):
    return '.'.join(chunklines[0].split()[0:2])

def split_to_chunks(lines, chunk_header_test):
    """
    chunk_header_test(line) is a function to test if a line is
    the beginning of a chunk.

    The input lines will be split as follows:

    {
    header line
    line
    line
    }
    {
    header line
    line
    line
    }
    {
    header line
    line
    line
    }
    {
    header line
    line
    line
    }
    """
    chunklines = []
    for line in lines:
        if chunk_header_test(line) == True:
            if len(chunklines) > 0:
                yield chunklines
                chunklines = []

        chunklines.append(line)

    if len(chunklines) > 0:
        yield chunklines

def split_to_trees(lines):
    def tree_header_tester(line):
        return line.split()[1] == 'tree'
    for chunk in split_to_chunks(lines, tree_header_tester):
        yield chunk

def split_to_nodes(lines):
    def node_header_tester(line):
        return line.startswith('leaf') or line.startswith('node')
    for chunk in split_to_chunks(lines, node_header_tester):
        yield chunk

def split_to_items(lines):
    def item_header_tester(line):
        # print line.startswith('\titem'), repr(line)
        return line.startswith('\titem')
    for chunk in split_to_chunks(lines, item_header_tester):
        yield chunk

def node_extent(lines):
    bytenr_list = []
    for line in lines:
        if line.startswith('leaf') or line.startswith('node'):
            # leaf 4882432 items 27 free space 2095 generation 10 owner 2
            ext = Extent(elem_type = 'node_or_leaf',
                start = int(line.split()[1]), size = 4096)
            bytenr_list.append(ext)
    return bytenr_list

def header_and_rest(lines):
    """
    Return header and rest, the input line should be:

    Header xxxxxxx
    Header xxxxxxx
    Header xxxxxxx
        Rest xxxxxxxxx
        Rest xxxxxxxxx
        Rest xxxxxxxxx
        Rest xxxxxxxxx

    """
    header_lines = []
    rest_lines = []
    mode = 'header'
    for line in lines:
        if mode == 'header' and is_indented(line):
            mode = 'rest'

        if mode == 'rest' and not is_indented(line):
            print '--------------------------->', line
            raise RuntimeError("when in rest mode, line must have indent")

        if mode == 'header':
            header_lines.append(line)
        else:
            rest_lines.append(line)

    return header_lines, rest_lines

def parse_extent_tree(lines):
    """
    There may be many nodes in lines.
    """

    # remove tree header
    del lines[0]

    extents = []
    for nodelines in split_to_nodes(lines):
        header, rest = header_and_rest(nodelines)
        print '----------'
        print ''.join(header)
        print '============================='
        for itemlines in split_to_items(rest):
            print '~~~~~~'
            item = item_to_namedtuple(itemlines[0])
            print item
            extent = Extent(elem_type = item.typeid, start = item.objectid,
                size = item.offset)
            extents.append(extent)

    table = extents_to_table(extents)
    print utils.table_to_str(table)

def extents_to_table(extents):
    table = []
    for ext in extents:
        table.append(ext.to_dict())
    return table

def item_to_namedtuple(line):
    mo = re.match(r"\titem (\d+) key \((\w+) (\w+) (\w+)\) itemoff (\d+) itemsize (\d+)", line)
    item = Item(* mo.groups())
    return item

def main():
    # with open('./btrfs-tree-5iter-run.txt', 'r') as f:
    with open('/tmp/results/study-btrfs-latest/btrfs-dftl2-256-cmtsize-15728-outputstuff-2015-08-16-06-05-08/btrfs-debug-tree.txt', 'r') as f:
        lines = f.readlines()
        for chunklines in split_to_trees(lines):
            treename = tree_name(chunklines)
            if treename == 'extent.tree':
                parse_extent_tree(chunklines)


if __name__ == '__main__':
    main()

