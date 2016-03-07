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

def stress_n_processes_raw():
    """
    stress the number of processes on file system
    """
    class Experimenter(object):
        def __init__(self, para):
            # Get default setting
            self.conf = config.ConfigNewFlash()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

        def setup_environment(self):
            self.conf['device_path'] = "/dev/sdc1"
            self.conf['dev_size_mb'] = 16*GB/MB

            self.conf['filesystem'] = None
            self.conf["n_online_cpus"] = 'all'

            self.conf['workload_class'] = 'FIONEW'

        def setup_workload(self):
            tmp_job_conf = [
                ("global", {
                    'ioengine'  : 'libaio',
                    'size'      : self.para.size,
                    'direct'    : 1,
                    'filename'  : self.conf['device_path'],
                    'iodepth'   : self.para.iodepth,
                    'bs'        : self.para.bs,
                    'fallocate' : 'none',
                    'offset_increment': self.para.size
                    }
                ),
                ("writer", {
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'numjobs'   : self.para.numjobs,
                    'rw'        : 'write'
                    }
                ),
                ("reader", {
                    'stonewall': WlRunner.fio.NOVALUE,
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'numjobs'   : self.para.numjobs,
                    'rw'        : 'read'
                    }
                ),
                ("readandwrite", {
                    'stonewall': WlRunner.fio.NOVALUE,
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'numjobs'   : self.para.numjobs,
                    'rw'        : 'rw'
                    }
                )
                ]
            self.conf['fio_job_conf'] = {
                    'ini': WlRunner.fio.JobConfig(tmp_job_conf),
                    'runner': {
                        'to_json': True
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
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            # self.run_fio()
            workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.run()

    Parameters = collections.namedtuple("Parameters",
            "numjobs, bs, iodepth, expname, size")
    expname = get_expname()

    total_size = 2*GB
    for i in range(3):
        for numjobs in [1, 4, 16, 32]:
            for bs in [4*KB, 128*KB]:
                for iodepth in [1]:
                    obj = Experimenter( Parameters(
                        numjobs = numjobs,
                        bs = bs, iodepth = iodepth,
                        expname = expname, size = total_size / numjobs
                        ))
                    obj.main()

def stress_n_processes():
    """
    stress the number of processes on file system
    """
    class Experimenter(object):
        def __init__(self, para):
            # Get default setting
            self.conf = config.ConfigNewFlash()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

        def setup_environment(self):
            self.conf['device_path'] = "/dev/sdc1"
            self.conf['dev_size_mb'] = 16*GB/MB

            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['workload_class'] = 'FIONEW'

        def setup_workload(self):
            tmp_job_conf = [
                ("global", {
                    'ioengine'  : 'libaio',
                    'size'      : self.para.size,
                    'directory'  : self.conf['fs_mount_point'],
                    'direct'    : 1,
                    'sync'      : 1,
                    'iodepth'   : self.para.iodepth,
                    'bs'        : self.para.bs,
                    'fallocate' : 'none',
                    'numjobs'   : self.para.numjobs,
                    }
                ),
                ("writer", {
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'rw'        : self.para.rw[0]
                    }
                ),
                ("reader", {
                    'stonewall': WlRunner.fio.NOVALUE,
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'rw'        : self.para.rw[1]
                    }
                ),
                ("mixedreadandwrite", {
                    'stonewall': WlRunner.fio.NOVALUE,
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'rw'        : self.para.rw[2]
                    }
                )
                ]
            self.conf['fio_job_conf'] = {
                    'ini': WlRunner.fio.JobConfig(tmp_job_conf),
                    'runner': {
                        'to_json': True
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
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            # self.run_fio()
            workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.run()

    def test_seq():
        """
        """
        Parameters = collections.namedtuple("Parameters",
                "filesystem, numjobs, bs, iodepth, expname, size, rw")

        expname = get_expname()
        para_dict = {
                'numjobs'        : [1, 4, 16],
                'bs'             : [4*KB, 128*KB],
                'iodepth'        : [1],
                'filesystem'     : ['ext4', 'f2fs', 'xfs', 'btrfs'],
                'expname'        : [expname],
                'rw'             : [('write', 'read', 'rw')]
                }

        parameter_combs = ParameterCombinations(para_dict)
        total_size = 2*GB
        for para in parameter_combs:
            para['size'] =  total_size / para['numjobs']

        for para in parameter_combs:
            obj = Experimenter( Parameters(**para) )
            obj.main()

    def test_rand():
        Parameters = collections.namedtuple("Parameters",
                "filesystem, numjobs, bs, iodepth, expname, size, rw")

        expname = get_expname()
        para_dict = {
                'numjobs'        : [1, 4, 16],
                'bs'             : [4*KB, 128*KB],
                'iodepth'        : [1],
                'filesystem'     : ['ext4', 'f2fs'],
                'expname'        : [expname],
                'rw'             : [('randwrite', 'randread', 'randrw')]
                }

        parameter_combs = ParameterCombinations(para_dict)
        total_size = 2*GB
        for para in parameter_combs:
            para['size'] =  total_size / para['numjobs']

        for para in parameter_combs:
            obj = Experimenter( Parameters(**para) )
            obj.main()


    # test_seq()
    test_rand()


def stress_page_cache():
    """
    stress the number of processes on file system
    """
    class Experimenter(object):
        def __init__(self, para):
            # Get default setting
            self.conf = config.ConfigNewFlash()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

        def setup_environment(self):
            self.conf['device_path'] = "/dev/sdc1"
            self.conf['dev_size_mb'] = 64*GB/MB

            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['workload_class'] = 'FIONEW'

            set_vm_default()
            set_vm("dirty_bytes", self.para.dirty_bytes)

            del self.conf['mnt_opts']['f2fs']['discard']

        def setup_workload(self):
            tmp_job_conf = [
                ("global", {
                    'ioengine'  : 'libaio',
                    'size'      : self.para.size,
                    'directory'  : self.conf['fs_mount_point'],
                    'direct'    : self.para.direct,
                    # 'sync'      : 1 ,
                    'iodepth'   : self.para.iodepth,
                    'bs'        : self.para.bs,
                    'fallocate' : 'none',
                    'numjobs'   : self.para.numjobs,
                    }
                ),
                ("writer", {
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'rw'        : self.para.rw
                    }
                )
                ]
            self.conf['fio_job_conf'] = {
                    'ini': WlRunner.fio.JobConfig(tmp_job_conf),
                    'runner': {
                        'to_json': True
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
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            # self.run_fio()
            workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.run()

    def test_rand():
        Parameters = collections.namedtuple("Parameters",
            "filesystem, numjobs, bs, iodepth, expname, size, rw, direct, "\
            "dirty_bytes")

        expname = get_expname()
        para_dict = {
                'numjobs'        : [1, 16],
                'bs'             : [4*KB, 128*KB],
                'iodepth'        : [1, 16],
                'filesystem'     : ['f2fs'],
                'expname'        : [expname],
                'rw'             : ['randwrite', 'randread', 'randrw'],
                'direct'         : [0, 1],
                'dirty_bytes'    : [4*MB]
                }

        parameter_combs = ParameterCombinations(para_dict)
        total_size = 1*GB
        for para in parameter_combs:
            para['size'] =  total_size / para['numjobs']

        for para in parameter_combs:
            obj = Experimenter( Parameters(**para) )
            obj.main()

    # test_seq()
    test_rand()


def stress_metadata():
    """
    stress the number of processes on file system
    """
    class Experimenter(object):
        def __init__(self, para):
            # Get default setting
            self.conf = config.ConfigNewFlash()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

        def setup_environment(self):
            self.conf['device_path'] = "/dev/sdc1"
            self.conf['dev_size_mb'] = 64*GB/MB

            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['workload_class'] = 'FIONEW'

            set_vm_default()
            set_vm("dirty_bytes", self.para.dirty_bytes)

            del self.conf['mnt_opts']['f2fs']['discard']

        def setup_workload(self):
            tmp_job_conf = [
                ("global", {
                    'ioengine'  : 'libaio',
                    'size'      : self.para.size,
                    'directory'  : self.conf['fs_mount_point'],
                    'direct'    : self.para.direct,
                    # 'sync'      : 1 ,
                    'iodepth'   : self.para.iodepth,
                    'bs'        : self.para.bs,
                    'fallocate' : 'none',
                    'numjobs'   : self.para.numjobs,
                    'create_serialize' : 0,
                    'create_fsync'     : 0,
                    'create_on_open'   : 1
                    }
                ),
                ("job", {
                    'group_reporting': WlRunner.fio.NOVALUE,
                    'rw'        : self.para.rw,
                    'nrfiles'   : self.para.nrfiles
                    }
                )
                ]
            self.conf['fio_job_conf'] = {
                    'ini': WlRunner.fio.JobConfig(tmp_job_conf),
                    'runner': {
                        'to_json': True
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
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            # self.run_fio()
            workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.run()

    def test_rand():
        Parameters = collections.namedtuple("Parameters",
            "filesystem, numjobs, bs, iodepth, expname, size, rw, direct, "\
            "dirty_bytes, nrfiles")

        expname = get_expname()
        para_dict = {
                'numjobs'        : [1, 32],
                'bs'             : [4*KB],
                'iodepth'        : [1, 32],
                'filesystem'     : ['ext4'],
                'expname'        : [expname],
                'rw'             : ['write'],
                'direct'         : [0, 1],
                'dirty_bytes'    : [2*GB],
                'nrfiles'        : [1, 8192]
                }

        parameter_combs = ParameterCombinations(para_dict)
        total_size = 1*GB
        for para in parameter_combs:
            para['size'] =  total_size / para['numjobs']

        for para in parameter_combs:
            obj = Experimenter( Parameters(**para) )
            obj.main()


    # test_seq()
    test_rand()


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





