from Makefile import *


def pattern_on_fs():
    class Experimenter(object):
        def __init__(self, para):
            self.conf = ssdbox.dftldes.Config()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

        def setup_environment(self):
            self.conf['device_path'] = self.para.device_path
            self.conf['dev_size_mb'] = 256
            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['workload_class'] = 'PatternSuite'

            self.conf['linux_ncq_depth'] = self.para.linux_ncq_depth

            set_vm_default()
            set_vm("dirty_bytes", self.para.dirty_bytes)

        def setup_workload(self):
            self.conf['workload_conf_key'] = 'PatternSuite'
            self.conf['PatternSuite'] = {}

        def setup_flash(self):
            pass

        def setup_ftl(self):
            self.conf['enable_blktrace'] = False
            self.conf['enable_simulation'] = False

        def run(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_workload()
            self.setup_flash()
            self.setup_ftl()
            self.run()

    def test_rand():
        Parameters = collections.namedtuple("Parameters",
            "filesystem, numjobs, bs, iodepth, expname, size, rw, direct, "\
            "dirty_bytes, fsync, device_path, linux_ncq_depth")

        expname = get_expname()
        para_dict = {
                'device_path'    : ['/dev/loop0'],
                'numjobs'        : [16],
                'bs'             : [16*KB],
                'iodepth'        : [1],
                'filesystem'     : ['ext4'],
                'expname'        : [expname],
                'rw'             : ['write'],
                'direct'         : [0],
                'dirty_bytes'    : [4*MB],
                'fsync'          : [0],
                'linux_ncq_depth': [31]
                }

        parameter_combs = ParameterCombinations(para_dict)
        total_size = 128*MB
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





