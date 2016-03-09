import os
import re
import subprocess
import time

import utils

class RawParser(object):
    """
    Parse blkparse output
    """
    def __init__(self, conf, raw_blkparse_file_path, parsed_output_path):
        self.conf = conf
        self.raw_blkparse_file_path = raw_blkparse_file_path
        self.parsed_output_path = parsed_output_path
        self.sector_size = self.conf['sector_size']

    def __line_to_dic(self, line):
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

    def __parse_row_dict(self, row):
        """
        Parse a row from blkparse file
        row is a dictionary of the line
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


    def __calculate_pre_wait_time(self, event_table):
        for i, row in enumerate(event_table):
            if i == 0:
                row['pre_wait_time'] = 0
                continue
            row['pre_wait_time'] = float(event_table[i]['timestamp']) - \
                float(event_table[i-1]['timestamp'])
            assert row['pre_wait_time'] >= 0

    def __create_event_line(self, line_dict):
        columns = [str(line_dict[colname])
                for colname in self.conf['event_file_columns']]
        line = ' '.join(columns)
        return line

    def __dump_table(self, table, out_path):
        utils.prepare_dir_for_path(out_path)
        out = open(out_path, 'w')
        for row_dict in table:
            if row_dict['type'] == 'blkparse':
                line_dict = self.__parse_row_dict(row_dict)
                line = self.__create_event_line(line_dict)
            else:
                raise NotImplementedError()

            out.write( line + '\n' )

        out.flush()
        os.fsync(out)
        out.close()

    def parse_raw(self):
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

            table.sort(key = lambda k: k['timestamp'])
            self.__calculate_pre_wait_time(table)
        return table

    def create_event_file(self):
        table = self.parse_raw()
        self.__dump_table(table, self.parsed_output_path)


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
        rawparser = RawParser(self.conf, self.resultpath, self.to_ftlsim_path)
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


