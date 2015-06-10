import shlex
import subprocess
import time

from common import shcmd

def start_blktrace_on_bg(dev, resultpath):
    cmd = "sudo blktrace -a write -d {dev} -o - | blkparse -i - > "\
        "{resultpath}".format(dev = dev, resultpath = resultpath)
    print cmd
    p = subprocess.Popen(cmd, shell=True)
    time.sleep(0.3) # wait to see if there's any immediate error.
    assert p.poll() == None
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

