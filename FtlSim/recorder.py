import config

output_method = 'stdout'

def debug(*args):
    if config.verbose_level >= 3:
        line = ' '.join( str(x) for x in args)
        print 'DEBUG', line

def debug2(*args):
    if config.verbose_level >= 3:
        line = ' '.join( str(x) for x in args)
        print 'DEBUG', line

def put(*msg):
    if config.verbose_level >= 1:
        line = ' '.join( str(x) for x in msg)
        print 'RECORD', line

def warning(*msg):
    if config.verbose_level >= 2:
        line = ' '.join( str(x) for x in msg)
        print 'WARNING', line

def error(*msg):
    if config.verbose_level >= 0:
        line = ' '.join( str(x) for x in msg)
        print 'ERROR', line

