import os
from pprint import pprint
import re
import shlex
import subprocess
import time

from common import shcmd
from conf import config

def start_blktrace_on_bg(dev, resultpath):
    cmd = "sudo blktrace -a write -d {dev} -o - | blkparse -i - > "\
        "{resultpath}".format(dev = dev, resultpath = resultpath)
    print cmd
    p = subprocess.Popen(cmd, shell=True)
    time.sleep(0.3) # wait to see if there's any immediate error.
    assert p.poll() == None
    return p

def stop_blktrace_on_bg():
    shcmd('pkill blkparse', ignore_error=True)
    shcmd('pkill blktrace', ignore_error=True)
    shcmd('sync')

    # try:
        # proc.terminate()
    # except Exception, e:
        # print e
        # exit(1)

def is_data_line(line):
    #                       devid    blockstart + nblocks
    match_obj = re.match( r'\d+,\d+.*\d+\s+\+\s+\d+', line)
    if match_obj == None:
        return False
    else:
        return True

def parse_blkparse_to_table(line_iter):
    def line2dic(line):
        "is_data_line() must be true for this line"\
        "['8,0', '0', '1', '0.000000000', '440', 'A', 'W', '12912077', '+', '8', '<-', '(8,2)', '606224']"
        names = ['devid', 'cpuid', 'seqid', 'time', 'pid', 'action', 'RWBS', 'blockstart', 'ignore1', 'size']
        #        0        1         2       3        4      5         6       7             8          9
        items = line.split()

        dic = dict(zip(names, items))
        assert len(items) >= len(names)

        return dic

    table = []
    for line in line_iter:
        line = line.strip()
        # print is_data_line(line), line
        if not is_data_line(line):
            continue
        ret = line2dic(line)
        if ret != None:
            table.append(ret)
    return table

# pprint( parse_blkparse_to_table(open('/tmp/myblkparse.out', 'r')) )
# exit(1)

########################################################
# table = [
#           {'col1':data, 'col2':data, ..},
#           {'col1':data, 'col2':data, ..},
#           ...
#         ]
def table_to_file(table, filepath, adddic=None):
    'save table to a file with additional columns'
    with open(filepath, 'w') as f:
        colnames = table[0].keys()
        if adddic != None:
            colnames += adddic.keys()
        colnamestr = ';'.join(colnames) + '\n'
        f.write(colnamestr)
        for row in table:
            if adddic != None:
                rowcopy = dict(row.items() + adddic.items())
            else:
                rowcopy = row
            rowstr = [rowcopy[k] for k in colnames]
            rowstr = [str(x) for x in rowstr]
            rowstr = ';'.join(rowstr) + '\n'
            f.write(rowstr)

def blkparse_to_files(blkparse_path, table_path):
    table = parse_blkparse_to_table(open(blkparse_path, 'r'))
    # table_to_file(table, table_path)
    finaltable_to_ftlsim_input(table, config['ftlsim_formatted_path'])

def finaltable_to_ftlsim_input(table, out_path):
    out = open(out_path, 'w')
    for row in table:
        blk_start = int(row['blockstart'])
        size = int(row['size'])
        secsize = config['sector_size']

        byte_offset = blk_start * secsize
        byte_size = size * secsize

        if row['RWBS'] == 'D':
            operation = 'discard'
        elif 'W' in row['RWBS']:
            operation = 'write'
        elif 'R' in row['RWBS']:
            operation = 'read'
        else:
            print 'unknow operation'
            exit(1)

        items = [str(x) for x in [operation, byte_offset, byte_size]]
        line = ' '.join(items)+'\n'
        out.write( line )

    out.flush()
    os.fsync(out)
    out.close()

# pprint( parse_blkparse_to_table(open('/tmp/myblkparse.out'), '/tmp/result') )

# time.sleep(2)
# stop_blktrace_on_bg(p)


# blkparse_to_table_file(config['blkparse_result_path'],
    # config['final_table_path'])

