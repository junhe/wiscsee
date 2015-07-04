import os
import sys

import utils

FILE_TARGET, STDOUT_TARGET = ('file', 'stdout')

class Recorder(object):
    def __init__(self, output_target, path=None, verbose_level=1):
        """This can be improved by passing in file descriptor, then you don't
        need output_target"""
        self.output_target = output_target
        self.path = path
        self.verbose_level = verbose_level
        self.counter = {}

        if self.output_target == FILE_TARGET:
            utils.prepare_dir_for_path(path)
            self.fhandle = open(path, 'w')

    def __del__(self):
        if self.output_target == FILE_TARGET:
            self.fhandle.flush()
            os.fsync(self.fhandle)
            self.fhandle.close()

        if self.path:
            # only write stats when we output to file
            stats_path = '.'.join((self.path, 'stats'))
            utils.table_to_file([self.counter], stats_path)

    def output(self, *args):
        line = ' '.join( str(x) for x in args)
        line += '\n'
        if self.output_target == FILE_TARGET:
            self.fhandle.write(line)
        else:
            sys.stdout.write(line)

    def debug(self, *args):
        if self.verbose_level >= 3:
            self.output('DEBUG', *args)

    def debug2(self, *args):
        if self.verbose_level >= 3:
            self.output('DEBUG', *args)

    def put(self, operation, page_num, category):
        # do statistics
        item = '.'.join((operation, category))
        self.counter[item] = self.counter.setdefault(item, 0) + 1

        if self.verbose_level >= 1:
            self.output('RECORD', operation, page_num, category)

    def warning(self, *args):
        if self.verbose_level >= 2:
            self.output('WARNING', *args)

    def error(self, *args):
        if self.verbose_level >= 0:
            self.output('ERROR', *args)


