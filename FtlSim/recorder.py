import collections
import os
import pprint
import sys

import utils

FILE_TARGET, STDOUT_TARGET = ('file', 'stdout')


def switchable(function):
    "decrator for class Recorder's method, so they can be switched on/off"
    def wrapper(self, *args, **kwargs):
        if self.enabled == None:
            raise RuntimeError("You need to explicity enable/disable Recorder."
                " We raise exception here because we think you will create"
                " unexpected behaviors that are hard to debug.")
        if self.enabled == False:
            return
        else:
            return function(self, *args, **kwargs)
    return wrapper

class Recorder(object):
    def __init__(self, output_target, path=None, verbose_level=1):
        """This can be improved by passing in file descriptor, then you don't
        need output_target"""
        self.output_target = output_target
        self.path = path
        self.verbose_level = verbose_level
        self.counter = {}
        self.put_and_count_counter = {}
        self.count_counter = {}
        self.counters = {} # for count_me, counter_name:collections.counter

        self.file_pool = {} # {filename:descriptor}

        # enabled by default
        self.enabled = None

        if self.output_target == FILE_TARGET:
            utils.prepare_dir_for_path(path)
            self.fhandle = open(path, 'w')

    def enable(self):
        print "....Recorder is enabled...."
        self.enabled = True

    def disable(self):
        "Note that this will not clear the previous records"
        self.enabled = False

    def __del__(self):
        if self.output_target == FILE_TARGET:
            self.fhandle.flush()
            os.fsync(self.fhandle)
            self.fhandle.close()

        if self.path:
            # only write stats when we _output to file
            stats_path = '.'.join((self.path, 'stats'))
            utils.table_to_file([self.counter], stats_path)
            print 'stats'
            pprint.pprint(self.counter)

            path2 = '.'.join((self.path, 'put_and_count.stats'))
            utils.table_to_file([self.put_and_count_counter], path2)

            path3 = '.'.join((self.path, 'count.stats'))
            utils.table_to_file([self.count_counter], path3)

            count_table_path = '.'.join((self.path, 'count_table'))
            count_table = self._counters_to_table()
            utils.table_to_file(count_table, count_table_path)

            print '*********  recorder counters (count_me()) **********'
            print utils.table_to_str(count_table, sep = '\t')

        if self.output_target == STDOUT_TARGET:
            count_table = self._counters_to_table()

            print '*********  recorder counters (count_me()) **********'
            print utils.table_to_str(count_table, sep = '\t')

        for fd in file_pool.values():
            fd.close()

    @switchable
    def count_me(self, counter_name, item):
        """
        use counter named counter_name to count the apperance of item_name
        """
        counter = self.counters.setdefault(counter_name, collections.Counter())
        counter[item] += 1

    def _counters_to_table(self):
        """
        columns
        counter.name   item.name    count
        """
        table = []
        for counter_name, counter in self.counters.items():
            for item_name, count in counter.items():
                d = {'counter.name': counter_name,
                     'item.name'   : item_name,
                     'count'       : count}
                table.append(d)

        return table

    def write_file(filename, *args):
        """
        Write args to filename as a line
        """
        fd = self.file_pool.setdefault(filename, open(
            os.path.join( os.path.basename(self.path), filename ), 'w'))
        args = [str(s).rjust(12) for s in args]
        fd.write(' '.join(args))

    @switchable
    def _output(self, *args):
        line = ' '.join( str(x) for x in args)
        line += '\n'
        if self.output_target == FILE_TARGET:
            self.fhandle.write(line)
        else:
            sys.stdout.write(line)

    @switchable
    def debug(self, *args):
        if self.verbose_level >= 3:
            self._output('DEBUG', *args)

    @switchable
    def debug2(self, *args):
        if self.verbose_level >= 3:
            self._output('DEBUG', *args)

    @switchable
    def put(self, operation, page_num, category):
        # do statistics
        item = '.'.join((operation, category))
        self.counter[item] = self.counter.setdefault(item, 0) + 1

        if self.verbose_level >= 1:
            self._output('RECORD', operation, page_num, category)

    @switchable
    def put_and_count(self, item, *args ):
        """ The first parameter will be counted """
        self.put_and_count_counter[item] = self.put_and_count_counter.setdefault(item, 0) + 1

        if self.verbose_level >= 1:
            self._output('PUTCOUNT', item, *args)

    @switchable
    def count(self, item, *args ):
        """ The first parameter will be counted """
        self.count_counter[item] = self.count_counter.setdefault(item, 0) + 1

    @switchable
    def warning(self, *args):
        if self.verbose_level >= 2:
            self._output('WARNING', *args)

    @switchable
    def error(self, *args):
        if self.verbose_level >= 0:
            self._output('ERROR', *args)


