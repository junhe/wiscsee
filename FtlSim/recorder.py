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
    consistent
    """
    def __init__(self, output_target, path=None, verbose_level=1):
        self.output_target = output_target
        self.path = path
        self.verbose_level = verbose_level

        if self.output_target == FILE_TARGET:
            self.fhandle = open(path, 'w')

    def __del__(self):
        if self.output_target == FILE_TARGET:
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
            self.output( 'DEBUG', *args )

    def warning(self, *args):
        if self.verbose_level >= 2:
            self.output( 'DEBUG', *args )

    def error(self, *args):
        if self.verbose_level >= 0:
            self.output( 'DEBUG', *args )


class Outfile(object):
    def __init__(self, path):
        common.prepare_dir_for_path(path)
        self.fhandle = open(path, 'w')

    def __del__(self):
        self.fhandle.close()

    def write(self, line):
        self.fhandle.write(line)

outfile = None

def initialize():
    global outfile
    if config.output_target == 'file':
        outfile = Outfile(config.get_output_file_path())

def output(*args):
    line = ' '.join( str(x) for x in args)
    line += '\n'
    if config.output_target == 'file':
        outfile.write(line)
    else:
        sys.stdout.write(line)

def debug(*args):
    if config.verbose_level >= 3:
        output( 'DEBUG', *args )

def debug2(*args):
    if config.verbose_level >= 3:
        output( 'DEBUG', *args )

def put(*args):
    if config.verbose_level >= 1:
        output( 'RECORD', *args )

def warning(*args):
    if config.verbose_level >= 2:
        output( 'WARNING', *args )

def error(*args):
    if config.verbose_level >= 0:
        output( 'ERROR', *args )

