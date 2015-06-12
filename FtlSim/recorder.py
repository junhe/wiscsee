import config
import sys

class Outfile(object):
    def __init__(self, path):
        self.fhandle = open(path, 'w')

    def __del__(self):
        self.fhandle.close()

    def write(self, line):
        self.fhandle.write(line)

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
        args = ' '.join( str(x) for x in args)
        output( 'DEBUG', *args )

def debug2(*args):
    if config.verbose_level >= 3:
        output( 'DEBUG', *args )

def put(*args):
    if config.verbose_level >= 1:
        output( 'DEBUG', *args )

def warning(*args):
    if config.verbose_level >= 2:
        output( 'DEBUG', *args )

def error(*args):
    if config.verbose_level >= 0:
        output( 'DEBUG', *args )

