import shlex
import subprocess
import time

def start_blktrace_on_bg(dev, basename, resultdir):
    cmd = shlex.split("sudo blktrace -a write -d {dev} -D {resultdir} "\
        "-o {basename}".format(dev = dev, resultdir = resultdir,
        basename = basename))
    print cmd
    p = subprocess.Popen(cmd)
    return p

def stop_blktrace_on_bg(proc):
    try:
        proc.terminate()
    except Exception, e:
        print e
        exit(1)

# p = start_blktrace_on_bg(dev='/dev/loop0', resultdir='/tmp/',
    # basename='tmptrace')






# time.sleep(2)
# stop_blktrace_on_bg(p)

