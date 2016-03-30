import os
import re
import subprocess
import time

from utilities import utils


class BlktraceWrapper(object):
    """
    It accepts a callback function.
    """
    def __init__(self, dev, trace_filter, result_dir, runner_class):
        cmd = "{classname}("\
            "dev = '{dev}', trace_filter = '{trace_filter}', "\
            "result_dir = '{result_dir}')".format(
                dev = dev, trace_filter = trace_filter,
                result_dir = result_dir, classname = runner_class)
        print cmd
        self.blktrace_runner = eval(cmd)

    def wrapped_run(self, run_func):
        """
        run_func is function to be traced by blktrace
        """
        self.blktrace_runner.run()
        run_func()
        self.blktrace_runner.stop()


class BlktraceRunner(object):
    def __init__(self, dev, trace_filter, result_dir):
        self.dev = dev
        self.trace_filter = trace_filter
        self.result_dir = result_dir

    def run(self):
        raise NotImplementedError()
    def stop(self):
        utils.shcmd("pkill blktrace")

class BlktraceRunnerAlone(BlktraceRunner):
    def run(self):
        # stop whatever blktrace that is running
        try:
            self.stop()
        except RuntimeError:
            pass

        utils.prepare_dir(self.result_dir)

        if self.trace_filter == None or self.trace_filter.strip() == '':
            filter_arg = ''
        else:
            filter_arg = '-a ' + self.trace_filter

        cmd = "sudo blktrace {filter_arg} -d {dev} "\
              "--output-dir={result_dir}"\
                .format(dev = self.dev, result_dir = self.result_dir,
                filter_arg = filter_arg)
        print cmd
        p = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
        time.sleep(0.3) # wait to see if there's any immediate error.

        if p.poll() != None:
            raise RuntimeError("tracing failed to start")

        return p

def replay_by_btreplay(record_dir, replay_dir):
    """
    record_dir is the location where blktrace output is held
    replay_dir is where we put output of btrecord for btreplay to use
    """
    utils.prepare_dir(replay_dir)
    utils.shcmd("btrecord -F --input-directory {record_dir} "\
            "--output-directory {replay_dir}".format(record_dir=record_dir,
            replay_dir = replay_dir))
    utils.shcmd("btreplay --input-directory={replay_dir} -F -W -v -N"\
            .format(replay_dir = replay_dir))


def replay_by_fio(record_dir, fio_replay_dir):
    """
    record_dir is the dir of the blktrace output
    fio_replay_dir is the dir we want to use for blkparse output
    """
    # FIO need the output of blktrace to be processed first
    utils.shcmd("blkparse sdc1 -D {record_dir} -O "\
            "-d {fio_replay_dir}/fio.blktrace.bin".format(
            record_dir=record_dir, fio_replay_dir=fio_replay_dir))

    # Now replay it with FIO
    utils.shcmd("fio --read_iolog={fio_replay_dir}/fio.blktrace.bin "\
            " --ioengine=libaio "\
            "--replay_no_stall=1 --name=replay".format(fio_replay_dir = fio_replay_dir))


def test_func_run():
    utils.shcmd("fio ./reproduce.ini")

def main():
    # runneralone = BlktraceRunnerAlone("/dev/sdc1", "queue", "/tmp/")

    # runneralone.run()
    # print 'waiting'
    # time.sleep(10)
    # runneralone.stop()

    suf = '6'
    record_dir = '/tmp/slowtrace128-rec' + suf
    replay_dir = '/tmp/slowtrace128-replay' + suf
    fio_replay_dir = '/tmp/fioreplay' + suf

    wrapper = BlktraceWrapper("/dev/sdc1", "", record_dir,
            "BlktraceRunnerAlone")
    wrapper.wrapped_run(test_func_run)

if __name__ == '__main__':
    main()


