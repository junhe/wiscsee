import collections

from config import LBAGENERATOR
from utilities import utils
from experimenter import *


def wearleveling_bench():
    class LocalExperimenter(Experimenter):
        def setup_workload(self):
            flashbytes = self.para.lbabytes
            zone_size = flashbytes / 8

            self.conf["workload_src"] = LBAGENERATOR
            self.conf["lba_workload_class"] = "AccessesWithDist"
            self.conf["age_workload_class"] = "NoOp"

            self.conf['AccessesWithDist'] = {
                    'lba_access_dist' : self.para.access_distribution,
                    'chunk_size'      : self.para.chunk_size,
                    'traffic_size'    : self.para.traffic_size,
                    'space_size'      : self.para.space_size,
                    'skew_factor'     : self.para.skew_factor,
                    'zipf_alpha'      : self.para.zipf_alpha,
                    }

    class ParaDict(object):
        def __init__(self):
            expname = utils.get_expname()
            lbabytes = 128*MB
            para_dict = get_shared_para_dict(expname, lbabytes)

            # for DFTL
            dftl_update = {
                    'ftl'              : ['dftldes'],
                    'n_channels_per_dev': [1],

                    'enable_simulation': [True],
                    'over_provisioning': [1.5], # 1.28 is a good number
                    'gc_high_ratio'    : [0.9],
                    'gc_low_ratio'     : [0.8],
                    'not_check_gc_setting': [False],
                    'cache_mapped_data_bytes' :[int(1*lbabytes)],
                    'segment_bytes'    : [lbabytes],
                    'snapshot_interval': [1*SEC],
                    'write_gc_log'     : [False],
                    'stripe_size'      : [1],
                    'do_wear_leveling' : [False, True],
                    'wear_leveling_check_interval': [1*SEC],
                    'wear_leveling_factor': [1],
                    'wear_leveling_diff' : [10],
                    'snapshot_valid_ratios': [False],
                    'snapshot_erasure_count_dist': [True],

                    'chunk_size'       : [64*KB],
                    'traffic_size'     : [32*1000*MB],
                    'space_size'       : [int(lbabytes / 2)],

                    'access_distribution' : ['uniform', 'zipf'],
                    'skew_factor'      : [10],
                    'zipf_alpha'       : [1],
                    }

            # for NKFTL2
            nkftl_update = {
                    'ftl'              : ['nkftl2'],
                    'enable_simulation': [True],
                    'over_provisioning': [1.5], # 1.28 is a good number
                    'gc_high_ratio'    : [0.9],
                    'gc_low_ratio'     : [0.8],
                    'not_check_gc_setting': [False],
                    'cache_mapped_data_bytes' :[None],
                    'segment_bytes'    : [lbabytes],
                    'snapshot_interval': [1*SEC],
                    'write_gc_log'     : [False],
                    'stripe_size'      : [64],

                    'do_wear_leveling' : [True],
                    'wear_leveling_check_interval': [1*SEC],
                    'wear_leveling_factor': [1],
                    'wear_leveling_diff' : [10],
                    'snapshot_valid_ratios': [False],
                    'snapshot_erasure_count_dist': [True],

                    'chunk_size'       : [64*KB],
                    'traffic_size'     : [300*MB],
                    'space_size'       : [int(lbabytes / 2)],

                    'access_distribution' : ['uniform'],
                    'skew_factor'      : [10],
                    'zipf_alpha'       : [1],
                    }

            # para_dict.update( dftl_update )
            para_dict.update( nkftl_update )

            self.check_config(para_dict)

            self.parameter_combs = ParameterCombinations(para_dict)

        def check_config(self, para_dict):
            if 'nkftl2' in para_dict['ftl']:
                for size in para_dict['stripe_size']:
                    for n_pages_per_block in para_dict['n_pages_per_block']:
                        assert size >= n_pages_per_block

        def __iter__(self):
            return iter(self.parameter_combs)

    def main():
        print 'here'
        for para in ParaDict():
            print para
            Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
            obj = LocalExperimenter( Parameters(**para) )
            obj.main()

    main()



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





