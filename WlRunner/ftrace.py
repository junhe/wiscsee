import os

import utils

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

# An example
# ftr = Ftrace()
# ftr.clean_trace()
# ftr.set_filter('')
# ftr.start_tracing()
# ftr.write_marker('Jun marked this beginning.')
# ftr.write_marker('Jun marked this end.')
# ftr.stop_tracing()
# ftr.copy_trace('/tmp/mytrace')

