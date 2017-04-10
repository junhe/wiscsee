import sys,os
import pprint
import glob
import re
from collections import Counter

from pyreuse.helpers import *

"""
You need to run strace with the following arguments:

strace_prefix = ' '.join(['strace',
   '-o', '.strace.out', '-f', '-ttt',
   '-e', 'trace=open,openat,accept,close,fsync,sync,read,'\
         'write,pread,pwrite,lseek,'\
         'dup,dup2,dup3,clone,unlink',
   '-s', '8'])

"""



UNFINISHED_MARK = '<unfinished ...>'

def match_line(line):
    mo = re.match(r'(\S+)\s+(\S+)\s+(\w+)\((.+)\)\s+=\s+(\S+)', line) # with pid
    return mo

def match_line_no_pid(line):
    mo = re.match(r'(\S+)\s+(\w+)\((.+)\)\s+=\s+(\S+)', line) # without pid
    return mo

def line_to_dic_no_pid(line, pid=None):
    """
    This function only handles normal line.
    It does not handle interrupted lines.
    """
    dic = {
            'pid'  :pid,
            'time' :None,
            'callname': None,
            'args':None,
            'ret' :None,
          }
    line = line.replace('"', '')

    mo = match_line_no_pid(line)

    if mo:
        i = 1
        dic['time'] = mo.group(i)
        i += 1
        dic['callname'] = mo.group(i)
        i += 1
        args = mo.group(i).split(',')
        dic['args'] = [ x.strip() for x in args ]
        i += 1
        dic['ret'] = mo.group(i)
        # dic['original_line'] = line
    else:
        print 'cannot parse:', line
        raise RuntimeError('cannot parse')

    return dic


def line_to_dic(line):
    """
    This function only handles normal line.
    It does not handle interrupted lines.
    """
    dic = {
            'pid'  :None,
            'time' :None,
            'callname': None,
            'args':None,
            'ret' :None,
          }
    line = line.replace('"', '')

    mo = match_line(line)

    #fdmap = {}
    if mo:
        #print mo.groups()
        i = 1
        dic['pid'] = mo.group(i)
        i += 1
        dic['time'] = mo.group(i)
        i += 1
        dic['callname'] = mo.group(i)
        i += 1
        args = mo.group(i).split(',')
        dic['args'] = [ x.strip() for x in args ]
        i += 1
        dic['ret'] = mo.group(i)
        # dic['original_line'] = line
    else:
        print 'cannot parse:', line
        raise RuntimeError('cannot parse')

    return dic

def get_dic_from_unfinished(line):
    """
    line has to be .... <unfinished ...>
    """
    mo = re.match(r'(\S+)\s+(\S+)\s+(\w+)\(',
                    line)
    dic = {}
    if mo:
        #print mo.groups()
        i = 1
        dic['pid'] = mo.group(i)
        i += 1
        dic['time'] = mo.group(i)
        i += 1
        dic['callname'] = mo.group(i)

    n = len(line)
    m = len(UNFINISHED_MARK)
    dic['trimedline'] = line[:(n-m)]

    return dic

def get_dic_from_resumed(line):
    """
    line has to be .... <... xxxx resumed>
    """
    mo = re.match(r'(\S+)\s+(\S+)\s+\<\.\.\. (\S+) resumed\>',
                    line)
    dic = {}
    if mo:
        #print mo.groups()
        i = 1
        dic['pid'] = mo.group(i)
        i += 1
        dic['time'] = mo.group(i)
        i += 1
        dic['callname'] = mo.group(i)

    # remove all chars before resumed>
    trimedline = re.sub(r'.*\<\.\.\. \S+ resumed\>', "", line)
    dic['trimedline'] = trimedline

    return dic

def maintain_filep(filep, entrydict):
    """
    structure of filep =
                    {
                      pid001: {
                        fd001: {
                                 'filepath':
                                 'pos':
                               }
                              }
                    }

    ########### OLD ###############
    if entrydict is open:
        add the fd->path mapping to fdmap
    else if entrydict is close:
        remove the fd->path mapping to fdmap
    else if entrydict is read,write,...
        get filepath from fdmap by the fd
    add 'filepath' to entrydict
    """
    callname = entrydict['callname']
    # pid = entrydict['pid']
    pid = 0 # they ususally share the same file desciptors.
    filepath = 'NA'
    offset   = 'NA'
    length   = 'NA'

    if entrydict['ret'] == '-1':
        # failed..
        entrydict['filepath'] = filepath
        entrydict['offset'] = offset
        entrydict['length'] = length
        return

    if callname == 'open':
        filepath = entrydict['args'][0]
        fd = entrydict['ret']
        if not filep.has_key(pid):
            filep[pid] = {}
        if not filep[pid].has_key(fd):
            filep[pid][fd] = {}
        filep[pid][fd]['filepath'] = filepath
        filep[pid][fd]['pos']      = 0
    elif callname == 'openat':
        filepath = entrydict['args'][1]
        fd = entrydict['ret']
        if not filep.has_key(pid):
            filep[pid] = {}
        if not filep[pid].has_key(fd):
            filep[pid][fd] = {}
        filep[pid][fd]['filepath'] = filepath
        filep[pid][fd]['pos']      = 0
    elif callname == 'accept':
        filepath = 'NETWORK'
        fd = entrydict['ret']
        if not filep.has_key(pid):
            filep[pid] = {}
        if not filep[pid].has_key(fd):
            filep[pid][fd] = {}
        filep[pid][fd]['filepath'] = filepath
        filep[pid][fd]['pos']      = 0
    elif callname == 'clone':
        newpid = entrydict['ret']
        if 'CLONE_FILES' in entrydict['args'][1]:
            filep[newpid] = filep[pid]

    elif callname in ['dup', 'dup2', 'dup3']:
        newfd = entrydict['ret']
        assert newfd != '-1'
        oldfd = entrydict['args'][0]
        if filep[pid].has_key(oldfd):
            filep[pid][newfd] = filep[pid][oldfd]
        else:
            print 'dup() an non-existing oldfd'
            exit(1)

        fd = oldfd
        try:
            filepath = filep[pid][fd]['filepath']
        except:
            filepath = fd
    elif callname == 'unlink':
        filepath = entrydict['args'][0]
    elif callname == 'close':
        fd = entrydict['args'][0]
        try:
            filepath = filep[pid][fd]['filepath']
            del filep[pid][fd]
        except:
            filepath = fd
    elif callname in \
            ['write', 'read', 'pwrite', 'pread', 'fsync', 'fdatasync', 'lseek']:
        fd = entrydict['args'][0]
        try:
            filepath = filep[pid][fd]['filepath']
        except KeyError:
            # pprint.pprint( entrydict )
            # pprint.pprint( filep )
            # print 'pid', pid, 'fd', fd
            filepath = fd
            # raise

        try:
            if callname in ['write', 'read']:
                offset = filep[pid][fd]['pos']
                length = int(entrydict['ret'])
                filep[pid][fd]['pos'] = offset + length
            elif callname in ['pread', 'pwrite']:
                # they don't affect filep offset
                offset = int(entrydict['args'][3])
                length = int(entrydict['ret'])
            elif callname in ['lseek']:
                #whence = entrydict['args'][2]
                #offset = int(entrydict['args'][1])
                offset = int(entrydict['ret'])
                filep[pid][fd]['pos'] = int(entrydict['ret'])
        except KeyError:
            # print 'pid', pid, 'fd', fd
            pass

    entrydict['filepath'] = filepath
    entrydict['offset'] = offset
    entrydict['length'] = length


def parse_lines(line_iter, pid=None):
    unfinished_dic = {} #indexed by call name
    filep = {}

    header=['pid', 'time', 'callname',
            'offset', 'length', 'filepath', 'trace_name']

    trace_name = 'tr-name'

    ret_table = []
    for line in line_iter:
        line = line.strip()
        if match_line_no_pid(line):
            entrydict = line_to_dic_no_pid(line)
            entrydict['pid'] = pid
        elif match_line(line):
            entrydict = line_to_dic(line)
        elif line.endswith(UNFINISHED_MARK):
            udic = get_dic_from_unfinished(line)
            unfinished_dic[(udic['pid'], udic['callname'])] = udic
            continue
        elif 'resumed' in line:
            udic = get_dic_from_resumed(line)
            name = udic['callname']
            pid = udic['pid']
            try:
                completeline = unfinished_dic[(pid, name)]['trimedline'] +\
                            udic['trimedline']
                entrydict = line_to_dic(completeline)
                del unfinished_dic[(pid, name)]
            except Exception as ex:
                print ex
                print unfinished_dic
                continue
                #raise

        maintain_filep( filep, entrydict )
        entrydict['trace_name'] = trace_name

        ret_table.append(entrydict)

    return ret_table


class StraceParser(object):
    def __init__(self, lines):
        self.lines = lines

    def parse(self):
        return None


def scan_trace(tracepath):
    pid = tracepath.split('.')[-1]

    try:
        pid = int(pid)
    except:
        pid = None

    with open(tracepath, 'r') as f:
        table = parse_lines(f, pid=pid)

    return table


def parse_file(filepath):
    return scan_trace(filepath)

def parse_to_simple_table(filepath):
    table = scan_trace(filepath)

    for row in table:
        try:
            del row['args']
        except KeyError:
            pass

    return table

def convert_to_dirty_data_table(table):
    dirty_size_dict = Counter()

    dirty_table = []
    for row in table:
        if row['callname'] in ['write', 'pwrite']:
            add_dirty_size(dirty_size_dict, row['filepath'], int(row['ret']))
        elif row['callname'] in ['fdatasync', 'fsync']:
            # new row, and ret
            filepath = row['filepath']
            total = dirty_size_dict[filepath]
            dirty_size_dict[filepath] = 0
            new_row = {'callname': row['callname'],
                       'dirty_size': total,
                       'pid': row['pid'],
                       'filepath': filepath,
                       'time': row['time']}
            dirty_table.append(new_row)

    return dirty_table

def add_dirty_size(dirty_size_dict, filepath, size):
    """
    dirty_size_dict = {filepath: bytes}
    """
    if not dirty_size_dict.has_key(filepath):
        dirty_size_dict[filepath] = 0

    dirty_size_dict[filepath] += size


def parse_and_write_dirty_table(filepath, output_path=None):
    tab = parse_to_simple_table(filepath)
    dirty_tab = convert_to_dirty_data_table(tab)

    if output_path is None:
        output_path = filepath + '.dirty_table'

    print 'write dirty table to', output_path
    with open(output_path, 'w') as f:
        f.write(table_to_str(dirty_tab, width=0))


def main():
    if len(sys.argv) != 2:
        print 'usage: python', sys.argv[0], 'tracepath'
    filepath = sys.argv[1]
    print 'Doing', filepath, '...........'
    df = scan_trace(filepath)

if __name__ == '__main__':
    main()


