import os
import re
import subprocess
import time

import utils

class BlockTraceManager(object):
    "This class provides interfaces to interact with blktrace"
    def __init__(self, confobj, dev, resultpath, to_ftlsim_path, sector_size):
        self.conf = confobj
        self.dev = self.conf['device_path']
        self.resultpath = resultpath
        self.to_ftlsim_path = to_ftlsim_path
        self.sector_size = sector_size

    def start_tracing_and_collecting(self):
        self.proc = start_blktrace_on_bg(self.dev, self.resultpath)

    def stop_tracing_and_collecting(self):
        "this is not elegant... TODO:improve"
        stop_blktrace_on_bg()

    def create_event_file_from_blkparse(self):
        table = self.parse_blkparse_result(open(self.resultpath, 'r'))
        utils.prepare_dir_for_path(self.to_ftlsim_path)
        self.create_event_file(table, self.to_ftlsim_path,
            self.sector_size)

    def line2dic(self, line):
        """
        is_data_line() must be true for this line"\
        ['8,0', '0', '1', '0.000000000', '440', 'A', 'W', '12912077', '+', '8', '<-', '(8,2)', '606224']"
        """
        names = ['devid', 'cpuid', 'seqid', 'timestamp', 'pid', 'action', 'RWBS', 'blockstart', 'ignore1', 'size']
        #        0        1         2       3        4      5         6       7             8          9
        items = line.split()

        dic = dict(zip(names, items))
        assert len(items) >= len(names)

        return dic

    def parse_blkparse_result(self, line_iter):
        table = []
        for line in line_iter:
            line = line.strip()
            # print is_data_line(line), line
            if is_data_line(line):
                ret = self.line2dic(line)
                ret['type'] = 'blkparse'
            else:
                ret = None

            if ret != None:
                table.append(ret)

        table.sort(key = lambda k: k['timestamp'])
        self.calculate_pre_wait_time(table)
        return table

    def calculate_pre_wait_time(self, event_table):
        for i, row in enumerate(event_table):
            if i == 0:
                row['pre_wait_time'] = 0
                continue
            row['pre_wait_time'] = float(event_table[i]['timestamp']) - \
                float(event_table[i-1]['timestamp'])
            assert row['pre_wait_time'] >= 0

    def parse_row(self, row):
        """
        Parse a row from blkparse file
        """
        # offset, size
        blk_start = int(row['blockstart'])
        size = int(row['size'])
        byte_offset = blk_start * self.sector_size
        byte_size = size * self.sector_size

        # operation
        if row['RWBS'] == 'D':
            operation = 'discard'
        elif 'W' in row['RWBS']:
            operation = 'write'
        elif 'R' in row['RWBS']:
            operation = 'read'
        else:
            raise RuntimeError('unknow operation')

        line_dict = {
            'pid'          : row['pid'],
            'operation'    : operation,
            'offset'       : byte_offset,
            'size'         : byte_size,
            'timestamp'    : row['timestamp'],
            'pre_wait_time': row['pre_wait_time']
                }

        return line_dict

    def create_event_line(self, line_dict):
        columns = [str(line_dict[colname])
                for colname in self.conf['event_file_columns']]
        line = ' '.join(columns)
        return line

    def create_event_file(self, table, out_path, sector_size):
        utils.prepare_dir_for_path(out_path)
        out = open(out_path, 'w')
        for row in table:
            if row['type'] == 'blkparse':
                line_dict = self.parse_row(row)
                line = create_event_line(line_dict)
            else:
                raise NotImplementedError()

            out.write( line + '\n' )

        out.flush()
        os.fsync(out)
        out.close()


def start_blktrace_on_bg(dev, resultpath):
    utils.prepare_dir_for_path(resultpath)
    # cmd = "sudo blktrace -a write -a read -d {dev} -o - | blkparse -i - > "\
    # cmd = "sudo blktrace -a queue -d {dev} -o - | blkparse -a queue -i - > "\

    kernel_ver = utils.run_and_get_output('uname -r')[0].strip()
    if kernel_ver.startswith('4.1.5'):
        # trace_filter = 'complete'
        trace_filter = 'issue'
    elif kernel_ver.startswith('3.1.6'):
        trace_filter = 'queue'
    else:
        trace_filter = 'issue'
        print "WARNING: using blktrace filter {} for kernel {}".format(
            trace_filter, kernel_ver)
        time.sleep(5)

    cmd = "sudo blktrace -a {filtermask} -d {dev} -o - | "\
            "blkparse -a {filtermask} -i - >> "\
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


