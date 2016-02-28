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
        def __init__(self, para):
            # Get default setting
            self.conf = config.ConfigNewFlash()
            self.para = para

        def setup_environment(self):
            self.conf['device_path'] = "/dev/loop0"
            self.conf['device_type'] = "loop" # loop, real
            self.conf['dev_size_mb'] = 256

            self.conf['use_fs'] = True
            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['workload_class'] = 'FIONEW'

        def setup_workload(self):
            tmp_job_conf = [
                ("job1", {
                    'ioengine'  : 'libaio',
                    'size'      : '1mb',
                    'directory'  : self.conf['fs_mount_point'],
                    'direct'    : 1,
                    'iodepth'   : self.para.iodepth,
                    'bs'        : self.para.bs,
                    'fallocate' : 'none',
                    'numjobs'   : self.para.numjobs,
                    'group_reporting': WlRunner.fio.NOVALUE
                    }
                )]
            self.conf['fio_job_conf'] = {
                    'ini': WlRunner.fio.JobConfig(tmp_job_conf),
                    'runner': {
                        'to_json': False
                    }
                }
            self.conf['workload_conf_key'] = 'fio_job_conf'

        def setup_ftl(self):
            self.conf['enable_blktrace'] = False
            self.conf['enable_simulation'] = False

        def run_fio(self):
            workload = WlRunner.workload.FIONEW(self.conf,
                    workload_conf_key = self.conf['workload_conf_key'])
            workload.run()

        def run(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_str(self.para))
            runtime_update(self.conf)

            # self.run_fio()
            workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.run()

    Parameters = collections.namedtuple("Parameters",
            "filesystem, numjobs, bs, iodepth, expname")

    obj = StressNProcesses( Parameters(
        filesystem = 'ext4', numjobs = 3, bs = 4*KB, iodepth = 1,
        expname = get_expname()
        ))
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





