import collections
import multiprocessing
import os
import time

import utils

def stats_worker(stop_event):
    """this worker process counts the number of function called in Ftrace"""
    cnt = collections.Counter()
    # with open("/tmp/ftrace_output_from_pipe", "w") as f_out:
    with open("/sys/kernel/debug/tracing/trace_pipe", "r") as f_in:
        for line in f_in:
            # f_out.write(line)
            if line.startswith("#"):
                next
            try:
                func_name = line.split()[4]
            except IndexError:
                pass
            else:
                cnt[func_name] += 1

            if stop_event.is_set() == True:
                # the main process will set the event to True if it
                # needs to be stoped
                break

    utils.table_to_file([cnt], "/tmp/stats_worker_output")


class Ftrace(object):
    def __init__(self):
        self.rootdir = "/sys/kernel/debug/tracing"

    def write_file(self, filename, msg):
        with utils.cd(self.rootdir):
            with open(filename, 'w') as f:
                print 'writing "{}" to {}'.format(msg, filename)
                f.write(msg)
                f.flush()

    def start_tracing(self):
        self.write_file('tracing_on', '1')

    def stop_tracing(self):
        self.write_file('tracing_on', '0')

    def clean_trace(self):
        self.write_file('trace', '')

    def write_marker(self, msg):
        self.write_file('trace_marker', msg)

    def set_filter(self, filter_str):
        self.write_file('set_ftrace_filter', filter_str)

    def copy_trace(self, target_path):
        with utils.cd(self.rootdir):
            utils.shcmd("cp trace {}".format(target_path))

    def run_stats(self):
        self.stop_event = multiprocessing.Event()
        self.stats_proc = multiprocessing.Process(name='stats',
                                 target=stats_worker,
                                 args=(self.stop_event,))
        self.stats_proc.start()

    def stop_stats(self):
        self.stop_event.set()

if __name__ == '__main__':
    # An example
    ftr = Ftrace()
    ftr.clean_trace()
    ftr.set_filter('')
    ftr.run_stats()
    ftr.start_tracing()
    time.sleep(2)
    ftr.write_marker('Jun marked this beginning.')
    ftr.write_marker('Jun marked this end.')
    ftr.stop_tracing()
    ftr.stop_stats()

