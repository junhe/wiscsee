import os
import re
import subprocess
import time

import utils

class BlockTraceManager(object):
    "This class provides interfaces to interact with blktrace"
    def __init__(self, dev, resultpath, to_ftlsim_path, sector_size):
        self.dev = dev
        self.resultpath = resultpath
        self.to_ftlsim_path = to_ftlsim_path
        self.sector_size = sector_size

    def start_tracing_and_collecting(self):
        self.proc = start_blktrace_on_bg(self.dev, self.resultpath)

    def stop_tracing_and_collecting(self):
        "this is not elegant... TODO:improve"
        stop_blktrace_on_bg()

    def blkparse_file_to_ftlsim_input_file(self):
        table = parse_blkparse_to_table(open(self.resultpath, 'r'))
        utils.prepare_dir_for_path(self.to_ftlsim_path)
        finaltable_to_ftlsim_input(table, self.to_ftlsim_path,
            self.sector_size)

def start_blktrace_on_bg(dev, resultpath):
    utils.prepare_dir_for_path(resultpath)
    # cmd = "sudo blktrace -a write -a read -d {dev} -o - | blkparse -i - > "\
    # cmd = "sudo blktrace -a queue -d {dev} -o - | blkparse -a queue -i - > "\

    kernel_ver = utils.run_and_get_output('uname -r')[0].strip()
    if kernel_ver.startswith('4.1.5'):
        trace_filter = 'complete'
    elif kernel_ver.startswith('3.1.6'):
        trace_filter = 'queue'
    else:
        trace_filter = 'complete'
        print "WARNING: using blktrace filter {} for kernel {}".format(
            trace_filter, kernel_ver)
        time.sleep(5)

    cmd = "sudo blktrace -a {filtermask} -d {dev} -o - | "\
            "blkparse -a {filtermask} -i - > "\
        "{resultpath}".format(dev = dev, resultpath = resultpath,
        filtermask = trace_filter)
    print cmd
    p = subprocess.Popen(cmd, shell=True)
    time.sleep(0.3) # wait to see if there's any immediate error.

    if p.poll() != None:
        raise RuntimeError("tracing failed to start")

    return p

def stop_blktrace_on_bg():
    utils.shcmd('pkill blkparse', ignore_error=True)
    utils.shcmd('pkill blktrace', ignore_error=True)
    utils.shcmd('sync')

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

########################################################
# table = [
#           {'col1':data, 'col2':data, ..},
#           {'col1':data, 'col2':data, ..},
#           ...
#         ]
def table_to_file(table, filepath, adddic=None):
    'save table to a file with additional columns'
    utils.prepare_dir_for_path(filepath)
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

def blkparse_to_parsed_files(blkparse_path):
    table = parse_blkparse_to_table(open(blkparse_path, 'r'))
    # table_to_file(table, table_path)
    table_path = conf.get_ftlsim_events_output_path()
    utils.prepare_dir_for_path(table_path)
    finaltable_to_ftlsim_input(table, table_path)

def finaltable_to_ftlsim_input(table, out_path, sector_size):
    utils.prepare_dir_for_path(out_path)
    out = open(out_path, 'w')
    for row in table:
        blk_start = int(row['blockstart'])
        size = int(row['size'])

        byte_offset = blk_start * sector_size
        byte_size = size * sector_size

        if row['RWBS'] == 'D':
            operation = 'discard'
        elif 'W' in row['RWBS']:
            operation = 'write'
        elif 'R' in row['RWBS']:
            operation = 'read'
        else:
            raise RuntimeError('unknow operation')

        items = [str(x) for x in [operation, byte_offset, byte_size]]
        line = ' '.join(items)+'\n'
        out.write( line )

    out.flush()
    os.fsync(out)
    out.close()


