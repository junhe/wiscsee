from Makefile import *

def get_fio_conf():
    """
    format
    {
        experiment_name: [
                            ("jobname", { ... }),
                            ("jobname", { ... }),
                            ...
                         ],
        experiment_name: [
                            ("jobname", { ... }),
                            ("jobname", { ... }),
                            ...
                         ],
        .
    }
    """
    job_conf = {}

    # stress_n_processes
    job_conf['stress_n_processes'] = [
        ("global", {
            'direct' : 1
            }
        ),
        ("job1", {
            "rw": "write"
            }
        )]




def stress_n_processes():
    class StressNProcesses(object):
        def __init__(self):
            # Get default setting
            self.conf = config.ConfigNewFlash()

        def setup_environment(self):
            pass

        def setup_workload(self):
            tmp_job_conf = [
                ("job1", {
                    'ioengine'  : 'libaio',
                    'io_size'   : '1mb',
                    'size'      : '1mb',
                    'filename'  : '/tmp/sdc',
                    'direct'    : 1,
                    'iodepth'   : 1,
                    'bs'        : '32kb',
                    'fallocate' : 'none',
                    'numjobs'   : 2
                    }
                )]
            self.conf['fio_job_conf'] = {
                    'ini': WlRunner.fio.JobConfig(tmp_job_conf)
                }
            self.conf['workload_conf_key'] = 'fio_job_conf'

        def setup_ftl(self):
            pass

        def run(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = 'testexp',
                    subexpname = 'testsubexp')
            runtime_update(self.conf)

            workload = WlRunner.workload.FIONEW(self.conf,
                    workload_conf_key = 'fio_job_conf')
            workload.run()

        def main(self):
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.run()

    obj = StressNProcesses()
    obj.main()

def main(cmd_args):
    if cmd_args.git == True:
        shcmd("sudo -u jun git commit -am 'commit by Makefile: {commitmsg}'"\
            .format(commitmsg=cmd_args.commitmsg \
            if cmd_args.commitmsg != None else ''), ignore_error=True)
        shcmd("sudo -u jun git pull")
        shcmd("sudo -u jun git push")


def _main():
    parser = argparse.ArgumentParser(
        description="This file hold command stream." \
        'Example: python Makefile.py doexp1 '
        )
    parser.add_argument('-t', '--target', action='store')
    parser.add_argument('-c', '--commitmsg', action='store')
    parser.add_argument('-g', '--git',  action='store_true',
        help='snapshot the code by git')
    args = parser.parse_args()

    if args.target == None:
        main(args)
    else:
        # WARNING! Using argument will make it less reproducible
        # because you have to remember what argument you used!
        targets = args.target.split(';')
        for target in targets:
            eval(target)
            # profile.run(target)

if __name__ == '__main__':
    _main()





