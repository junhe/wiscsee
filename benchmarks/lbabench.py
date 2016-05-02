from Makefile import *

def simplest():
    class Experiment(object):
        def __init__(self, para):
            self.para = para
        def setup_config(self):
            self.conf = ssdbox.dftldes.Config()
            self.conf['SSDFramework']['ncq_depth'] = 1

            self.conf['flash_config']['n_pages_per_block'] = 2
            self.conf['flash_config']['n_blocks_per_plane'] = 2
            self.conf['flash_config']['n_planes_per_chip'] = 1
            self.conf['flash_config']['n_chips_per_package'] = 1
            self.conf['flash_config']['n_packages_per_channel'] = 1
            self.conf['flash_config']['n_channels_per_dev'] = 4

        def setup_environment(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))

            self.conf['enable_blktrace'] = True
            self.conf['enable_simulation'] = True

        def setup_workload(self):
            w = OP_WRITE
            r = OP_READ
            d = OP_DISCARD

            self.conf["workload_src"] = LBAGENERATOR
            self.conf["lba_workload_class"] = "ExtentTestWorkloadFLEX2"
            self.conf["lba_workload_configs"]["ExtentTestWorkloadFLEX2"] = {
                    "events": [
                        (d, 1, 3),
                        (w, 1, 3),
                        (r, 1, 3)
                        ]}
            self.conf["age_workload_class"] = "NoOp"

        def setup_ftl(self):
            self.conf['ftl_type'] = 'dftldes'
            self.conf['simulator_class'] = 'SimulatorDESNew'

            logicsize_mb = 2
            self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page \
                    * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
            self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

        def my_run(self):
            runtime_update(self.conf)
            workflow(self.conf)

        def main(self):
            self.setup_config()
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.my_run()

    Parameters = collections.namedtuple("Parameters",
            "expname")
    expname = get_expname()

    exp = Experiment( Parameters(expname = expname) )
    exp.main()

def qdepth_pattern():
    class Experiment(object):
        def __init__(self, para):
            self.para = para

        def setup_config(self):
            self.conf = ssdbox.dftldes.Config()
            self.conf['SSDFramework']['ncq_depth'] = self.para.ncq_depth

            self.conf['flash_config']['n_pages_per_block'] = 64
            self.conf['flash_config']['n_blocks_per_plane'] = 2
            self.conf['flash_config']['n_planes_per_chip'] = 1
            self.conf['flash_config']['n_chips_per_package'] = 1
            self.conf['flash_config']['n_packages_per_channel'] = 1
            self.conf['flash_config']['n_channels_per_dev'] = 8

            self.conf['exp_parameters'] = self.para._asdict()

            self.conf['do_not_check_gc_setting'] = True
            self.conf.GC_high_threshold_ratio = 0.01
            self.conf.GC_low_threshold_ratio = 0

        def setup_environment(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))

            self.conf['enable_blktrace'] = True
            self.conf['enable_simulation'] = True

        def setup_workload(self):
            self.conf["workload_src"] = LBAGENERATOR
            self.conf["lba_workload_class"] = "TestWorkloadFLEX3"

            traffic = self.para.traffic
            chunk_size = self.para.chunk_size
            page_size = self.conf['flash_config']['page_size']
            self.conf["lba_workload_configs"]["TestWorkloadFLEX3"] = {
                    "op_count": traffic/chunk_size,
                    "extent_size": chunk_size/page_size ,
                    "ops": [OP_WRITE], 'mode': self.para.pattern}
            print self.conf['lba_workload_configs']['TestWorkloadFLEX3']
            self.conf["age_workload_class"] = "NoOp"

        def setup_ftl(self):
            self.conf['ftl_type'] = 'dftldes'
            self.conf['simulator_class'] = 'SimulatorDESNew'
            self.conf['stripe_size'] = self.para.stripe_size

            logicsize_mb = self.para.logicsize_mb
            self.conf.cache_mapped_data_bytes = self.para.cache_mapped_data_bytes
            self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.00))

        def my_run(self):
            runtime_update(self.conf)
            workflow(self.conf)

        def main(self):
            self.setup_config()
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.my_run()

    def gen_parameters():
        expname = get_expname()
        # para_dict = {
                # 'expname'        : [expname],
                # 'ncq_depth'      : [1, 8],
                # 'chunk_size'     : [2*KB, 16*KB],
                # 'traffic'        : [8*MB],
                # 'logicsize_mb'     : [128],
                # 'cache_mapped_data_bytes' :[16*MB, 128*MB],
                # 'pattern'        : ['sequential', 'random'],
                # 'stripe_size'    : [1, 'infinity']
                # }
        para_dict = {
                'expname'        : [expname],
                'ncq_depth'      : [8],
                'chunk_size'     : [2*KB],
                'traffic'        : [1*MB], # 1 4 7
                'logicsize_mb'     : [128],
                'cache_mapped_data_bytes' :[128*MB],
                'pattern'        : ['sequential', 'random'],
                'stripe_size'    : [1, 'infinity']
                }

        parameter_combs = ParameterCombinations(para_dict)

        return parameter_combs

    for i, para in enumerate(gen_parameters()):
        print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', i
        # if i != 17:
            # continue
        Parameters = collections.namedtuple("Parameters",
                ','.join(para.keys()))
        exp = Experiment( Parameters(**para) )
        exp.main()

def simple_gc():
    class Experiment(object):
        def __init__(self, para):
            self.para = para
        def setup_config(self):
            self.conf = ssdbox.dftldes.Config()
            self.conf['SSDFramework']['ncq_depth'] = 2

            self.conf['flash_config']['n_pages_per_block'] = 2
            self.conf['flash_config']['n_blocks_per_plane'] = 2
            self.conf['flash_config']['n_planes_per_chip'] = 1
            self.conf['flash_config']['n_chips_per_package'] = 1
            self.conf['flash_config']['n_packages_per_channel'] = 1
            self.conf['flash_config']['n_channels_per_dev'] = 2

            self.conf['do_not_check_gc_setting'] = True
            self.conf.GC_high_threshold_ratio = 0.95
            self.conf.GC_low_threshold_ratio = 0

            self.conf['stripe_size'] = 'infinity'

        def setup_environment(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))

            self.conf['enable_blktrace'] = True
            self.conf['enable_simulation'] = True

        def setup_workload(self):
            w = OP_WRITE
            r = OP_READ
            d = OP_DISCARD

            n = self.conf.n_pages_per_block
            self.conf["workload_src"] = LBAGENERATOR
            self.conf["lba_workload_class"] = "ExtentTestWorkloadFLEX2"
            self.conf["lba_workload_configs"]["ExtentTestWorkloadFLEX2"] = {
                    "events": [
                        (w, 0, n), # to channel1
                        (w, n, n), # to channel0
                        (w, 0, 1),  # to channel1, the previous block in channel1 become 'used'
                        (w, n, 1),
                        (OP_CLEAN, 0, 0)
                        ]}
            self.conf["age_workload_class"] = "NoOp"

        def setup_ftl(self):
            self.conf['ftl_type'] = 'dftldes'
            self.conf['simulator_class'] = 'SimulatorDESNew'

            logicsize_mb = 2
            self.conf.mapping_cache_bytes = self.conf.n_mapping_entries_per_page \
                    * self.conf['cache_entry_bytes'] # 8 bytes (64bits) needed in mem
            self.conf.set_flash_num_blocks_by_bytes(int(logicsize_mb * 2**20 * 1.28))

        def my_run(self):
            runtime_update(self.conf)
            workflow(self.conf)

        def main(self):
            self.setup_config()
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.my_run()

    Parameters = collections.namedtuple("Parameters",
            "expname")
    expname = get_expname()

    exp = Experiment( Parameters(expname = expname) )
    exp.main()


def patterns_bench():
    class Experiment(object):
        def __init__(self, para):
            self.para = para

        def setup_config(self):
            self.conf = ssdbox.dftldes.Config()
            self.conf['SSDFramework']['ncq_depth'] = self.para.ncq_depth

            self.conf['flash_config']['page_size'] = 2048
            self.conf['flash_config']['n_pages_per_block'] = 64
            self.conf['flash_config']['n_blocks_per_plane'] = 2
            self.conf['flash_config']['n_planes_per_chip'] = 1
            self.conf['flash_config']['n_chips_per_package'] = 1
            self.conf['flash_config']['n_packages_per_channel'] = 1
            self.conf['flash_config']['n_channels_per_dev'] = 4

            self.conf['exp_parameters'] = self.para._asdict()

            self.conf['do_not_check_gc_setting'] = True
            self.conf.GC_high_threshold_ratio = 0.96
            self.conf.GC_low_threshold_ratio = 0

        def setup_environment(self):
            set_exp_metadata(self.conf, save_data = True,
                    expname = self.para.expname,
                    subexpname = chain_items_as_filename(self.para))

            self.conf['enable_blktrace'] = True
            self.conf['enable_simulation'] = True

        def setup_workload(self):
            self.conf["workload_src"] = LBAGENERATOR
            self.conf["lba_workload_class"] = "PatternAdapter"
            self.conf["lba_workload_configs"]["PatternAdapter"] = {}
            self.conf["lba_workload_configs"]["PatternAdapter"]["class"] = self.para.patternclass
            self.conf["age_workload_class"] = "NoOp"

        def setup_ftl(self):
            self.conf['ftl_type'] = 'dftldes'
            self.conf['simulator_class'] = 'SimulatorDESNew'
            self.conf['stripe_size'] = self.para.stripe_size

            self.conf.cache_mapped_data_bytes = self.para.cache_mapped_data_bytes
            self.conf.set_flash_num_blocks_by_bytes(self.para.flashbytes)

        def my_run(self):
            runtime_update(self.conf)
            workflow(self.conf)

        def main(self):
            self.setup_config()
            self.setup_environment()
            self.setup_workload()
            self.setup_ftl()
            self.my_run()

    def gen_parameters():
        expname = get_expname()
        para_dict = {
                'expname'        : [expname],
                'ncq_depth'      : [4],
                # 'patternclass'   : ['SRandomRead'],
                'patternclass'   : ['SHotNCold',
                    'SRandomWrite', 'SRandomRead',
                    'SSequentialWrite', 'SSequentialRead',
                    'SSnake', 'SFadingSnake', 'SStrided'],
                'cache_mapped_data_bytes' :[128*MB],
                'flashbytes'     : [128*MB],
                'stripe_size'    : [1, 'infinity']
                }

        parameter_combs = ParameterCombinations(para_dict)

        return parameter_combs

    for i, para in enumerate(gen_parameters()):
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        exp = Experiment( Parameters(**para) )
        exp.main()



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


