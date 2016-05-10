from Makefile import *

from workflow import run_workflow
from utilities.utils import get_expname


def pattern_on_fs():
    class Experimenter(object):
        def __init__(self, para):
            self.conf = ssdbox.dftldes.Config()
            self.para = para
            self.conf['exp_parameters'] = self.para._asdict()

            self.patternconf = {
                    'zone_size': 1*MB,
                    'chunk_size': 512*KB,
                    'traffic_size': 8*MB,
                    }


        def setup_environment(self):
            self.conf['device_path'] = self.para.device_path
            self.conf['dev_size_mb'] = 256
            self.conf['filesystem'] = self.para.filesystem
            self.conf["n_online_cpus"] = 'all'

            self.conf['linux_ncq_depth'] = self.para.linux_ncq_depth

            set_vm_default()
            set_vm("dirty_bytes", self.para.dirty_bytes)

        def setup_file_prep(self):
            if self.para.patternclass in ['SRandomReadNoPrep', 'SSequentialReadNoPrep']:
                self.conf['age_workload_class'] = 'PatternSuite'
                self.conf['age_config'] = {'patternname': 'SSequentialWrite',
                    'parameters': self.patternconf}

                self.conf['aging_config_key'] = 'age_config'
                print "Aging workload is patternstuie"
            else:
                print "Aging workload is NOOP"

        def setup_workload(self):
            # they already have prep
            assert self.para.patternclass not in ["SRandomRead", "SSequentialRead"]

            self.conf['workload_class'] = 'PatternSuite'
            self.conf['workload_config'] = {'patternname': self.para.patternclass,
                'parameters': self.patternconf}
            self.conf['workload_conf_key'] = 'workload_config'

        def setup_flash(self):
            self.conf['SSDFramework']['ncq_depth'] = 4

            self.conf['flash_config']['page_size'] = 2048
            self.conf['flash_config']['n_pages_per_block'] = 64
            self.conf['flash_config']['n_blocks_per_plane'] = 2
            self.conf['flash_config']['n_planes_per_chip'] = 1
            self.conf['flash_config']['n_chips_per_package'] = 1
            self.conf['flash_config']['n_packages_per_channel'] = 1
            self.conf['flash_config']['n_channels_per_dev'] = 4

            self.conf['do_not_check_gc_setting'] = True
            self.conf.GC_high_threshold_ratio = 0.96
            self.conf.GC_low_threshold_ratio = 0

        def setup_ftl(self):
            self.conf['enable_blktrace'] = True
            self.conf['enable_simulation'] = True

            self.conf['simulator_class'] = 'SimulatorDESNew'
            self.conf['ftl_type'] = 'dftldes'

            logicsize_mb = self.conf['dev_size_mb']
            entries_need = int(logicsize_mb * 2**20 * 1.00 / self.conf['flash_config']['page_size'])
            self.conf.mapping_cache_bytes = int(entries_need * self.conf['cache_entry_bytes']) # 8 bytes (64bits) needed in mem
            self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

        def run(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))
            runtime_update(self.conf)

            run_workflow(self.conf)

        def main(self):
            self.setup_environment()
            self.setup_file_prep()
            self.setup_workload()
            self.setup_flash()
            self.setup_ftl()
            self.run()

    def test_rand():
        Parameters = collections.namedtuple("Parameters",
            "patternclass, filesystem, expname, dirty_bytes, device_path, linux_ncq_depth")

        expname = get_expname()
        para_dict = {
                # 'patternclass'   : ['SRandomWrite'],
                'patternclass'   : ['SRandomReadNoPrep'],
                'device_path'    : ['/dev/loop0'],
                'filesystem'     : ['ext4'],
                'expname'        : [expname],
                'dirty_bytes'    : [4*MB],
                'linux_ncq_depth': [31]
                }
        parameter_combs = ParameterCombinations(para_dict)

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





