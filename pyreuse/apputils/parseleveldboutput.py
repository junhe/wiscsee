import re
import os

from pyreuse.helpers import table_to_str

"""
output:

benchname op_duration bw keysize valuesize entries rawsize filesize
"""

def parse_metadata(lines):
    meta = {}
    for line in lines:
        if line.startswith('CPUCache'):
            meta['CPUCache'] = line.split()[1]
        elif line.startswith('Keys'):
            meta['Keys'] = line.split()[1]
        elif line.startswith('Values'):
            meta['Values'] = line.split()[1]
            meta['ValuesCompressed'] = line.split()[4].strip('(')
        elif line.startswith('Entries'):
            meta['Entries'] = line.split()[1]
        elif line.startswith('RawSize'):
            meta['RawSize'] = line.split()[1]
        elif line.startswith('FileSize'):
            meta['FileSize'] = line.split()[1]

    return meta


def parse_benchresult_line(line):
    if not 'micros/op' in line:
        return None

    d = {}
    if 'MB/s' in line:
        mo = re.search(r'(\w+)\s*:\s*(\S+) micros/op;\s*(\S+) MB/s', line)
        d['bw'] = mo.group(3)
    else:
        mo = re.search(r'(\w+)\s*:\s*(\S+) micros/op;', line)
        d['bw'] = 'NA'

    d['benchname'] = mo.group(1)
    d['op_duration'] = mo.group(2)

    return d


def parse_benchresults(lines):
    table = []
    for line in lines:
        d = parse_benchresult_line(line)
        if d is not None:
            table.append(d)
    return table


def parse_file_text(text):
    parts = text.split('------------------------------------------------')
    meta = parse_metadata(parts[0].split('\n'))
    table = parse_benchresults(parts[1].split('\n'))
    tablestr = table_to_str(table, adddic=meta, width=12)
    return tablestr

def parse_file(filepath):
    with open(filepath, 'r') as f:
        text = f.read()
        tablestr = parse_file_text(text)
        return tablestr


