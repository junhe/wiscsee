import os
import re
import subprocess
import time

from pyreuse.helpers import *
from pyreuse.macros import *

class BlktraceResult(object):
    """
    Parse blkparse output
    """
    def __init__(self, sector_size, event_file_column_names,
            raw_blkparse_file_path, parsed_output_path,
            padding_bytes=0, do_sort=True):
        self.raw_blkparse_file_path = raw_blkparse_file_path
        self.parsed_output_path = parsed_output_path
        self.sector_size = sector_size
        self.event_file_column_names = event_file_column_names
        self.do_sort = do_sort

        # event offset + padding_bytes = blktrace addr
        #
        #          |--x-- FS address -------
        # Dev:
        # |--8MB---|--y---------------------
        # y - 8MB = x
        # blktrace address - 8MB = event address
        self.padding_bytes = padding_bytes

    def create_event_file(self):
        prepare_dir_for_path(self.parsed_output_path)

        out_file = open(self.parsed_output_path, 'w')
        in_file = open(self.raw_blkparse_file_path, 'r')

        for line in in_file:
            line = line.strip()
            if not is_data_line(line):
                continue

            # get row dict
            row_dict = self.__line_to_dic(line)
            row_dict['type'] = 'blkparse'

            line = self.__create_event_line(row_dict)
            out_file.write( line + '\n' )

        out_file.flush()
        os.fsync(out_file)
        out_file.close()


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
        if 'D' in row['RWBS']:
            operation = 'discard'
        elif 'W' in row['RWBS']:
            operation = 'write'
        elif 'R' in row['RWBS']:
            operation = 'read'
        else:
            raise RuntimeError('unknow operation ' + row['RWBS'])

        row['operation'] = operation

        if 'S' in row['RWBS']:
            row['sync'] = 'True'
        else:
            row['sync'] = 'False'

    def __parse_and_add_offset_size(self, row):
        sec_start = int(row['sector_start'])
        sec_count = int(row['sector_count'])
        byte_offset = sec_start * self.sector_size - self.padding_bytes
        byte_size = sec_count * self.sector_size

        row['offset'] = byte_offset
        row['size'] = byte_size

    def __create_event_line(self, line_dict):
        columns = [str(line_dict.get(colname, 'NA'))
                for colname in self.event_file_column_names]
        line = ' '.join(columns)
        return line


class BlktraceResultInMem(object):
    """
    Parse blkparse output
    """
    def __init__(self, sector_size, event_file_column_names,
            raw_blkparse_file_path, parsed_output_path,
            padding_bytes=0, do_sort=True):
        self.raw_blkparse_file_path = raw_blkparse_file_path
        self.parsed_output_path = parsed_output_path
        self.sector_size = sector_size
        self.event_file_column_names = event_file_column_names
        self.do_sort = do_sort

        # event offset + padding_bytes = blktrace addr
        #
        #          |--x-- FS address -------
        # Dev:
        # |--8MB---|--y---------------------
        # y - 8MB = x
        # blktrace address - 8MB = event address
        self.padding_bytes = padding_bytes

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
        if 'D' in row['RWBS']:
            operation = 'discard'
        elif 'W' in row['RWBS']:
            operation = 'write'
        elif 'R' in row['RWBS']:
            operation = 'read'
        else:
            raise RuntimeError('unknow operation ' + row['RWBS'])

        row['operation'] = operation

        if 'S' in row['RWBS']:
            row['sync'] = 'True'
        else:
            row['sync'] = 'False'

    def __parse_and_add_offset_size(self, row):
        sec_start = int(row['sector_start'])
        sec_count = int(row['sector_count'])
        byte_offset = sec_start * self.sector_size - self.padding_bytes
        byte_size = sec_count * self.sector_size

        row['offset'] = byte_offset
        row['size'] = byte_size

    def __calculate_pre_wait_time(self, event_table):
        if self.do_sort is True:
            event_table.sort(key = lambda k: float(k['timestamp']))

        for i, row in enumerate(event_table):
            if i == 0:
                row['pre_wait_time'] = 0
                continue
            row['pre_wait_time'] = float(event_table[i]['timestamp']) - \
                float(event_table[i-1]['timestamp'])
            if self.do_sort is True:
                assert row['pre_wait_time'] >= 0, "data is {}".format(row['pre_wait_time'])

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
                for colname in self.event_file_column_names]
        line = ' '.join(columns)
        return line

    def create_event_file(self):
        prepare_dir_for_path(self.parsed_output_path)
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
        size_mb = sec_cnt * self.sector_size / float(MB)
        duration = self.get_duration()

        return size_mb / duration


class BlockTraceManager(object):
    "This class provides interfaces to interact with blktrace"
    def __init__(self, dev, event_file_column_names,
            resultpath, to_ftlsim_path, sector_size, padding_bytes=0,
            do_sort=True):
        self.dev = dev
        self.sector_size = sector_size
        self.event_file_column_names = event_file_column_names
        self.resultpath = resultpath
        self.to_ftlsim_path = to_ftlsim_path
        self.sector_size = sector_size
        self.padding_bytes = padding_bytes
        self.do_sort = do_sort

    def start_tracing_and_collecting(self, trace_filter=None):
        self.proc = start_blktrace_on_bg(self.dev, self.resultpath, trace_filter)

    def stop_tracing_and_collecting(self):
        stop_blktrace_on_bg()

    def create_event_file_from_blkparse(self):
        if self.do_sort is True:
            rawparser = BlktraceResultInMem(self.sector_size,
                    self.event_file_column_names,
                    self.resultpath, self.to_ftlsim_path,
                    padding_bytes=self.padding_bytes,
                    do_sort=self.do_sort
                    )
            rawparser.create_event_file()

        else:
            rawparser = BlktraceResult(self.sector_size,
                    self.event_file_column_names,
                    self.resultpath, self.to_ftlsim_path,
                    padding_bytes=self.padding_bytes,
                    do_sort=self.do_sort
                    )
            rawparser.create_event_file()


def start_blktrace_on_bg(dev, resultpath, trace_filter=None):
    prepare_dir_for_path(resultpath)
    # cmd = "sudo blktrace -a write -a read -d {dev} -o - | blkparse -i - > "\
    # cmd = "sudo blktrace -a queue -d {dev} -o - | blkparse -a queue -i - > "\

    if trace_filter is None:
        # trace_filter = '-a issue'
        trace_filter = ''
    else:
        trace_filter = ' '.join(['-a ' + mask for mask in trace_filter])

    cmd = "sudo blktrace {filtermask} -d {dev} -o - | "\
            "blkparse {filtermask} -i - >> "\
        "{resultpath}".format(dev = dev, resultpath = resultpath,
        filtermask = trace_filter)
    print cmd
    p = subprocess.Popen(cmd, shell=True)
    time.sleep(0.3) # wait to see if there's any immediate error.

    if p.poll() != None:
        raise RuntimeError("tracing failed to start")

    return p

def stop_blktrace_on_bg():
    shcmd('pkill blkparse', ignore_error=True)
    shcmd('pkill blktrace', ignore_error=True)
    shcmd('sync')

def is_data_line(line):
    #                       devid    sector_start + nblocks
    match_obj = re.match( r'\d+,\d+.*\d+\s+\+\s+\d+', line)
    if match_obj == None:
        return False
    else:
        return True


