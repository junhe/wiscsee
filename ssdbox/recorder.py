import collections
import os
import pprint
import sys

from utilities import utils

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
    def __init__(self, output_target,
            output_directory = None,
            verbose_level = 1,
            print_when_finished = False):
        self.output_target = output_target
        self.output_directory = output_directory
        self.verbose_level = verbose_level
        self.print_when_finished = print_when_finished

        assert len(self.output_target) > 0

        self.file_pool = {} # {filename:descriptor}
        self.file_colnames = {} # {filename:[colname1, 2, ...]

        # {set name: collections.counter}
        self.general_accumulator = {}
        self.result_dict = {'general_accumulator': self.general_accumulator}

        self.enabled = None

        self.__open_log_file()

        self._unique_num = 0

        self._tag_groups = {
            'read_user': 'foreground',
            'write_user': 'foreground',
            'read_trans': 'background',
            'prog_trans': 'background'}

    def close(self):
        self.__close_log_file()
        self.__save_accumulator()
        self.__save_result_dict()
        self._close_file_pool()

    def enable(self):
        print "....Recorder is enabled...."
        self.enabled = True

    def disable(self):
        "Note that this will not clear the previous records"
        print "....Recorder is DIS-abled. Now not counting anything."
        self.enabled = False

    def _close_file_pool(self):
        for _, file_handle in self.file_pool.items():
            os.fsync(file_handle)
            file_handle.close()

    def __save_result_dict(self):
        result_path = os.path.join(self.output_directory, 'recorder.json')
        utils.dump_json(self.result_dict, result_path)

    def __close_log_file(self):
        self.log_handle.flush()
        os.fsync(self.log_handle)
        self.log_handle.close()

    def __open_log_file(self):
        # open log file
        log_path = os.path.join(self.output_directory, 'recorder.log')
        utils.prepare_dir_for_path(log_path)
        self.log_handle = open(log_path, 'w')

    def __save_accumulator(self):
        counter_set_path = os.path.join(self.output_directory,
            'accumulator_table.txt')
        utils.prepare_dir_for_path(counter_set_path)
        general_accumulator_table = self._parse_accumulator(
                self.general_accumulator)
        utils.table_to_file(general_accumulator_table, counter_set_path)

    def __write_log(self, *args):
        line = ' '.join( str(x) for x in args)
        line += '\n'
        if self.output_target == FILE_TARGET:
            self.log_handle.write(line)
        else:
            sys.stdout.write(line)

    def get_result_summary(self):
        return self.result_dict

    def set_result_by_one_key(self, key, value):
        self.result_dict[key] = value

    def get_result_by_one_key(self, key):
        return self.result_dict[key]

    def append_to_value_list(self, key, addon):
        """
        Append addon to key's value
        {'key': [addon1, addon2]'
        """
        valuelist = self.result_dict.setdefault(key, [])
        valuelist.append(addon)

    @switchable
    def count_me(self, counter_name, item):
        """
        use counter named counter_name to count the apperance of item_name
        """
        self.add_to_general_accumulater(counter_name, item, 1)

    def get_count_me(self, counter_name, item):
        return self.get_general_accumulater_cnt(counter_name, item)

    def get_general_accumulater_cnt(self,
            counter_set_name, item_name):
        counter_dict = self.general_accumulator.setdefault(counter_set_name,
                collections.Counter())
        return counter_dict[item_name]

    @switchable
    def add_to_general_accumulater(self,
            counter_set_name, item_name, addition):
        """
        {counter set 1:
            {counter 1: ##,
             counter 2: #},
         counter set 2:
            {counter 1: ##,
             counter 2: #},
        }
        """
        counter_dict = self.general_accumulator.setdefault(counter_set_name,
                collections.Counter())
        counter_dict[item_name] += addition

    @switchable
    def add_to_timer(self, counter_set_name, item_name, addition):
        self.add_to_general_accumulater(counter_set_name, item_name, addition)

    def get_unique_num(self):
        num = self._unique_num
        self._unique_num += 1
        return num

    def get_tag(self, op, op_id):
        # return '-'.join([op, str(op_id)])
        return {'op': op, 'op_id':op_id}

    def tag_group(self, tag):
        try:
            return self._tag_groups[tag['op']]
        except (KeyError, TypeError):
            return tag
            # return 'TagGroupUnknown'

    def _parse_accumulator(self, counter_sets):
        """
        counter sets
        {counter set 1:
            {counter 1: ##,
             counter 2: #},
         counter set 2:
            {counter 1: ##,
             counter 2: #},
        }


        table columns
        counter.name   item.name    count
        """
        table = []
        for counter_set_name, counter_set in counter_sets.items():
            for counter_name, count in counter_set.items():
                d = {'counter.set.name': counter_set_name,
                     'counter.name'   : counter_name,
                     'count'       : count}
                table.append(d)

        return table

    def write_file(self, filename, **kwargs):
        """
        Write args to filename as a line

        You must provide kwargs with exactly the same keys. And you must
        provide keys in the parameter as they become columns in the file.
        """
        width = 20
        if not self.file_pool.has_key(filename):
            fd = open( os.path.join( self.output_directory, filename ), 'wr+')
            self.file_pool[filename] = fd
            self.file_colnames[filename] = kwargs.keys()
            colnames = [str(colname).rjust(width) for colname in kwargs.keys()]
            fd.write(' '.join(colnames) + '\n')
        else:
            fd = self.file_pool[filename]

        colnames = self.file_colnames[filename]
        args = [str(kwargs[colname]).rjust(width) for colname in colnames]
        fd.write(' '.join(args) + '\n')

    def debug(self, *args):
        if self.verbose_level >= 3:
            self.__write_log('DEBUG', *args)

    @switchable
    def put(self, operation, page_num, category):
        # do statistics
        item = '.'.join((operation, category))
        self.add_to_general_accumulater("put", item, 1)

    def warning(self, *args):
        if self.verbose_level >= 2:
            self.__write_log('WARNING', *args)

    def error(self, *args):
        if self.verbose_level >= 0:
            self.__write_log('ERROR', *args)


