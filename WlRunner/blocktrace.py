import os
import re
import subprocess
import time

import utils
from commons import *

class BlktraceResult(object):
    """
    Parse blkparse output
    """
    def __init__(self, conf, raw_blkparse_file_path, parsed_output_path):
        self.conf = conf
        self.raw_blkparse_file_path = raw_blkparse_file_path
        self.parsed_output_path = parsed_output_path
        self.sector_size = self.conf['sector_size']

        self.__parse_rawfile()

    def __line_to_dic(self, line):
        """
        is_data_line() must be true for this line"\
        ['8,0', '0', '1', '0.000000000', '440', 'A', 'W', '12912077', '+', '8', '<-', '(8,2)', '606224']"
        """
        names = ['devid', 'cpuid', 'seqid', 'timestamp', 'pid', 'action', 'RWBS', 'sector_start', 'ignore1', 'sector_count']
        #        0        1         2       3        4      5         6       7             8          9
        items = line.split()

        dic = dict(zip(names, items))
        assert len(items) >= len(names)

        self.__parse_and_add_operation(dic)
        self.__parse_and_add_offset_size(dic)

        return dic

    def __parse_and_add_operation(self, row):
        if row['RWBS'] == 'D':
            operation = 'discard'
        elif 'W' in row['RWBS']:
            operation = 'write'
        elif 'R' in row['RWBS']:
            operation = 'read'
        else:
            raise RuntimeError('unknow operation')

        row['operation'] = operation

    def __parse_and_add_offset_size(self, row):
        sec_start = int(row['sector_start'])
        sec_count = int(row['sector_count'])
        byte_offset = sec_start * self.sector_size
        byte_size = sec_count * self.sector_size

        row['offset'] = byte_offset
        row['size'] = byte_size

    def __calculate_pre_wait_time(self, event_table):
        event_table.sort(key = lambda k: k['timestamp'])
        for i, row in enumerate(event_table):
            if i == 0:
                row['pre_wait_time'] = 0
                continue
            row['pre_wait_time'] = float(event_table[i]['timestamp']) - \
                float(event_table[i-1]['timestamp'])
            assert row['pre_wait_time'] >= 0

        return event_table

    def __parse_rawfile(self):
        with open(self.raw_blkparse_file_path, 'r') as line_iter:
            table = []
            for line in line_iter:
                line = line.strip()
                # print is_data_line(line), line
                if is_data_line(line):
                    ret = self.__line_to_dic(line)
                    ret['type'] = 'blkparse'
                else:
                    ret = None

                if ret != None:
                    table.append(ret)

        table = self.__calculate_pre_wait_time(table)

        self.__parsed_table = table

    def __create_event_line(self, line_dict):
        columns = [str(line_dict[colname])
                for colname in self.conf['event_file_columns']]
        line = ' '.join(columns)
        return line

    def create_event_file(self):
        utils.prepare_dir_for_path(self.parsed_output_path)
        out = open(self.parsed_output_path, 'w')
        for row_dict in self.__parsed_table:
            if row_dict['type'] == 'blkparse':
                line = self.__create_event_line(row_dict)
            else:
                raise NotImplementedError()

            out.write( line + '\n' )

        out.flush()
        os.fsync(out)
        out.close()

    def get_duration(self):
        return float(self.__parsed_table[-1]['timestamp']) - \
                float(self.__parsed_table[0]['timestamp'])

    def count_sectors(self, operation):
        sectors_cnt = 0
        for row in self.__parsed_table:
            if row['operation'] == operation:
                sectors_cnt += int(row['sector_count'])

        return sectors_cnt

    def get_bandwidth_mb(self, operation):
        sec_cnt = self.count_sectors(operation)
        size_mb = sec_cnt * self.conf['sector_size'] / float(MB)
        duration = self.get_duration()

        return size_mb / duration


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
        stop_blktrace_on_bg()

    def create_event_file_from_blkparse(self):
        rawparser = BlktraceResult(self.conf, self.resultpath, self.to_ftlsim_path)
        rawparser.create_event_file()


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
        time.sleep(1)

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
    #                       devid    sector_start + nblocks
    match_obj = re.match( r'\d+,\d+.*\d+\s+\+\s+\d+', line)
    if match_obj == None:
        return False
    else:
        return True


