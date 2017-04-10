import pprint
import re
import sys

"""
output:
inode table: start, end
inode table: start, end
inode table: start, end
blockbitmap: start, end
blockbitmap: start, end
superblock: start, end
"""

def is_bg_start_line(line):
    return line.startswith("Group")

def convert_to_range(s):
    if '-' in s:
        tup = s.split('-')
        tup = [int(x) for x in tup]
        start, end = tup
    else:
        start = int(s)
        end = start
    return start, end

def parse_superblock(line):
    mo = re.search(r'superblock at (\S+), Group descriptors at (\S+)', line)
    superblock_addr = convert_to_range(mo.group(1))
    groupdesc = convert_to_range(mo.group(2))
    return {'superblock': superblock_addr,
            'groupdesc': groupdesc}

def parse_gdt(line):
    mo = re.search(r'Reserved GDT blocks at (\S+)', line)
    return {'reserved-gdt': convert_to_range(mo.group(1))}

def parse_bitmaps(line):
    mo = re.search(r'Block bitmap at (\S+) .*, Inode bitmap at (\S+) .*', line)
    return {'block-bitmap': convert_to_range(mo.group(1)),
            'inode-bitmap': convert_to_range(mo.group(2))}

def parse_inodetable(line):
    mo = re.search(r'Inode table at (\S+) .*', line)
    return {'inode-table': convert_to_range(mo.group(1))}

def parse_bg_lines(bg_lines):
    results = []
    for line in bg_lines:
        line = line.strip()
        if 'superblock' in line:
            d = parse_superblock(line)
            results.append(d)
        elif 'Reserved GDT' in line:
            d = parse_gdt(line)
            results.append(d)
        elif 'Block bitmap' in line:
            d = parse_bitmaps(line)
            results.append(d)
        elif 'Inode table' in line:
            d = parse_inodetable(line)
            results.append(d)
    return results

def parse_bg_text(text):
    bgs = []
    for line in text.split('\n'):
        if is_bg_start_line(line):
            cur_bg_lines = []
            bgs.append(cur_bg_lines)
        cur_bg_lines.append(line)

    parsed_dicts = []
    for bg in bgs:
        parsed_dicts.extend(parse_bg_lines(bg))
    return parsed_dicts

def as_table(parsed_dicts):
    rows = ["type start end"]
    for dic in parsed_dicts:
        for k, v in dic.items():
            vstr = ' '.join([str(x) for x in v])
            line = ' '.join([k, vstr])
            rows.append(line)

    return rows

def parse_header_text(text):
    """
    header is the first part, before \n\n\n
    """
    lines = text.split('\n')

    d = {}
    for line in lines:
        items = line.split(':')
        if line.startswith("Journal inode:"):
            d['journal-inode'] = int(items[1])
        elif line.startswith("Journal length:"):
            d['journal-length'] = int(items[1])

    return d

def parse_file_text(text):
    # get the second part of dumpe2fs output
    text = text.split("\n\n\n")[1]

    range_table = parse_bg_text(text)
    return range_table

def parse_file(fpath):
    with open(fpath, 'r') as f:
        text = f.read()

    range_table = parse_file_text(text)
    rows = as_table(range_table)
    return '\n'.join(rows)

