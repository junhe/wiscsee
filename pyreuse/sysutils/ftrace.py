import collections
import multiprocessing
import os
import time

from pyreuse.helpers import shcmd, cd

class Ftrace(object):
    def __init__(self):
        self.rootdir = "/sys/kernel/debug/tracing"

    def write_file(self, filename, msg):
        with cd(self.rootdir):
            with open(filename, 'w') as f:
                print 'writing "{}" to {}'.format(msg, filename)
                f.write(msg)
                f.flush()

    def append_file(self, filename, msg):
        with cd(self.rootdir):
            with open(filename, 'a') as f:
                print 'appending "{}" to {}'.format(msg, filename)
                f.write(msg)
                f.flush()

    def read_file(self, filename):
        with cd(self.rootdir):
            with open(filename, 'r') as f:
                text = f.read()
                return text

    def get_trace(self):
        text = self.read_file('trace')
        return text

    def set_tracer(self, tracer):
        self.write_file('current_tracer', tracer)

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

    def add_filter(self, filter_str):
        self.append_file('set_ftrace_filter', filter_str)

    def copy_trace(self, target_path):
        with cd(self.rootdir):
            shcmd("cp trace {}".format(target_path))


def trace_cmd(cmd, tracer, ffilter):
    """
    tracer: function or function_graph
    """
    ftr = Ftrace()
    ftr.clean_trace()
    ftr.set_tracer(tracer)
    ftr.start_tracing()

    ftr.set_filter(ffilter)

    shcmd(cmd)

    ftr.stop_tracing()
    text = ftr.get_trace()

    return text



if __name__ == '__main__':
    # An example
    ftr = Ftrace()
    ftr.clean_trace()
