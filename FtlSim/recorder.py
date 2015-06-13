import sys

import common
import config

FILE_TARGET = 'file'
STDOUT_TARGET = 'stdout'

class Recorder(object):
    """
    The recorder is stateful, so I need to make it a class.
    Recorder should be explicitly initialized, and once initialized,
    it cannot be changed. I have to do this because I don't want it
    to output to file sometimes and stdout sometimes. This is not
    consistent.

    Recorder class is necessary because it may be extended to support
    statistics.
    """
    def __init__(self, output_target, path=None, verbose_level=1):
        self.output_target = output_target
        self.path = path
        self.verbose_level = verbose_level

        if self.output_target == FILE_TARGET:
            self.fhandle = open(path, 'w')

    def __del__(self):
        if self.output_target == FILE_TARGET:
            self.fhandle.flush()
            os.fsync(self.fhandle)
            self.fhandle.close()

    def output(self, *args):
        line = ' '.join( str(x) for x in args)
        line += '\n'
        if self.output_target == FILE_TARGET:
            self.fhandle.write(line)
        else:
            sys.stdout.write(line)

    def debug(self, *args):
        if self.verbose_level >= 3:
            args = ' '.join( str(x) for x in args)
            self.output( 'DEBUG', *args )

    def debug2(self, *args):
        if self.verbose_level >= 3:
            self.output( 'DEBUG', *args )

    def put(self, *args):
        if self.verbose_level >= 1:
            self.output( 'RECORD', *args )

    def warning(self, *args):
        if self.verbose_level >= 2:
            self.output( 'WARNING', *args )

    def error(self, *args):
        if self.verbose_level >= 0:
            self.output( 'ERROR', *args )

# this global Recorder instance should be initialized before using
rec = None

def initialize():
    rec = Recorder(output_target = config.confdic['output_target'],
                   path = config.get_output_file_path(),
                   verbose_level = config.confdic['verbose_level'])

# def output(*args):
    # line = ' '.join( str(x) for x in args)
    # line += '\n'
    # if config.output_target == 'file':
        # outfile.write(line)
    # else:
        # sys.stdout.write(line)

# def debug(*args):
    # if config.verbose_level >= 3:
        # output( 'DEBUG', *args )

# def debug2(*args):
    # if config.verbose_level >= 3:
        # output( 'DEBUG', *args )

# def put(*args):
    # if config.verbose_level >= 1:
        # output( 'RECORD', *args )

# def warning(*args):
    # if config.verbose_level >= 2:
        # output( 'WARNING', *args )

# def error(*args):
    # if config.verbose_level >= 0:
        # output( 'ERROR', *args )

