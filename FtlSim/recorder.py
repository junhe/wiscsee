
output_method = 'stdout'

def debug(*args):
    return
    line = ' '.join( str(x) for x in args)
    print 'DEBUG', line

def debug2(*args):
    line = ' '.join( str(x) for x in args)
    print 'DEBUG', line

def put(*msg):
    line = ' '.join( str(x) for x in msg)
    print 'RECORD', line

def warning(*msg):
    line = ' '.join( str(x) for x in msg)
    print 'WARNING', line

def error(*msg):
    line = ' '.join( str(x) for x in msg)
    print 'ERROR', line

