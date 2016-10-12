from Makefile import *
import csv
import os
import glob
import time

from utilities import utils
from experimenter import *
from expconfs import ParameterPool
import filesim

import prepare4pyreuse
from pyreuse.sysutils.straceParser import parse_and_write_dirty_table


class ParaDictIterMixin(object):
    def iterate_blocksize_segsize_fs(self):
        para = self.parameter_combs[0]
        lbabytes = para['lbabytes']
        updatedicts = [
            # {'segment_bytes': 2*MB, 'n_pages_per_block': 128*KB/(2*KB)},
            # {'segment_bytes': 16*MB,        'n_pages_per_block': 128*KB/(2*KB)},
            # {'segment_bytes': lbabytes * 2, 'n_pages_per_block': 128*KB/(2*KB)},

            # {'segment_bytes': 16*MB,        'n_pages_per_block': 1*MB/(2*KB)},
            # {'segment_bytes': 128*MB,        'n_pages_per_block': 1*MB/(2*KB)},
            {'segment_bytes': lbabytes * 2, 'n_pages_per_block': 1*MB/(2*KB)},
            ]
        new_update_dics = []
        for d in updatedicts:
            for fs in ['ext4', 'f2fs', 'xfs', 'btrfs']:
            # for fs in ['btrfs', 'xfs']:
                new_d = copy.copy(d)
                new_d['filesystem'] = fs

                new_update_dics.append(new_d)

        for update_dict in new_update_dics:
            tmp_para = copy.deepcopy(para)
            tmp_para.update(update_dict)
            yield tmp_para

    def iterate_blocksize_for_alignment(self):
        local_paras = []
        for parameters in self.parameter_combs:
            for block_size in self.block_sizes:
                para = copy.deepcopy(parameters)
                para['n_pages_per_block'] = block_size / (2*KB)
                para['stripe_size'] = para['n_pages_per_block']
                para['segment_bytes'] = block_size

                local_paras.append(para)

        for para in local_paras:
            yield para


testname_dict = {
    'rocksdb_reqscale': [
        'rocksdb_reqscale_r_seq',
        'rocksdb_reqscale_r_rand',
        'rocksdb_reqscale_r_mix',
        'rocksdb_reqscale_w_seq',
        'rocksdb_reqscale_w_rand',
        'rocksdb_reqscale_w_mix'
        ],

    'leveldb_reqscale': [
        'leveldb_reqscale_r_seq',
        'leveldb_reqscale_r_rand',
        'leveldb_reqscale_r_mix',
        'leveldb_reqscale_w_seq',
        'leveldb_reqscale_w_rand',
        'leveldb_reqscale_w_mix'
        ],

    'rocksNlevelwrite_reqscale': [
        'rocksdb_reqscale_w_seq',
        'rocksdb_reqscale_w_rand',
        'rocksdb_reqscale_w_mix',

        'leveldb_reqscale_w_seq',
        'leveldb_reqscale_w_rand',
        'leveldb_reqscale_w_mix',
        ],

    'sqlitewal_reqscale': [
        'sqliteWAL_reqscale_r_seq',
        'sqliteWAL_reqscale_r_rand',
        'sqliteWAL_reqscale_r_mix',
        'sqliteWAL_reqscale_w_seq',
        'sqliteWAL_reqscale_w_rand',
        'sqliteWAL_reqscale_w_mix'
        ],

    'sqliterb_reqscale': [
        'sqliteRB_reqscale_r_seq',
        'sqliteRB_reqscale_r_rand',
        'sqliteRB_reqscale_r_mix',
        'sqliteRB_reqscale_w_seq',
        'sqliteRB_reqscale_w_rand',
        'sqliteRB_reqscale_w_mix'
        ],

    'varmail_reqscale': [
        'varmail_reqscale_r_small',
        'varmail_reqscale_r_large',
        'varmail_reqscale_r_mix',
        'varmail_reqscale_w_small',
        'varmail_reqscale_w_large',
        'varmail_reqscale_w_mix'
        ],

    ########### wear level ##############

    'rocksdb_wearlevel':
        [
        'rocksdb_wearlevel_w_seq',
        'rocksdb_wearlevel_w_rand',
        'rocksdb_wearlevel_w_mix',
        ],

    'leveldb_wearlevel':
        [
        'leveldb_wearlevel_w_seq',
        'leveldb_wearlevel_w_rand',
        'leveldb_wearlevel_w_mix',
        ],

    'sqlitewal_wearlevel':
        [
        'sqliteWAL_wearlevel_w_seq',
        'sqliteWAL_wearlevel_w_rand',
        'sqliteWAL_wearlevel_w_mix',
        ],

    'sqliterb_wearlevel':
        [
        'sqliteRB_wearlevel_w_seq',
        'sqliteRB_wearlevel_w_rand',
        'sqliteRB_wearlevel_w_mix',
        ],

    'varmail_wearlevel':
        [
        'varmail_wearlevel_w_small',
        'varmail_wearlevel_w_large',
        'varmail_wearlevel_w_mix',
        ],

    'tmp':
        [
        # 'leveldb_reqscale_r_seq',
        # 'leveldb_reqscale_r_rand',
        # 'leveldb_reqscale_r_mix',
        # 'leveldb_reqscale_w_seq',
        'leveldb_reqscale_w_rand',
        # 'leveldb_reqscale_w_mix'
        ],

}


def appmixbench_for_rw(testsetname, expname):
    if testsetname == "" or expname == "":
        print 'testsetname or expname missing'
        print 'Usage: make appmix4rw testsetname=rocksdb_reqscale expname=myexp001'
        exit(1)

    para_pool = ParameterPool(
            expname = expname,
            testname = testname_dict[testsetname],
            filesystem = ['ext4', 'f2fs', 'xfs']
            # filesystem = ['ext4']
            )

    for para in para_pool:
        run_on_real_dev(para)


def run_on_real_dev(para):
    Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
    obj = RealDevExperimenter( Parameters(**para) )
    obj.main()


def simulate_from_event_files(app=None, rule=None, expname=None):
    if app == "" or rule == "" or expname == "":
        print 'app/rule is not set'
        print 'Usage: make simevents app=rocksdb rule=alignment expname=rocksdb-alginment-xxj3j'
        exit(1)

    appmap = {
            # value is a exp_rel_path
            'rocksdb': 'rocksdb-reqscale',
            'leveldb': 'leveldb-reqscale-001',
            'sqlitewal': 'sqlitewal-reqscale-240000-inserts-3',
            'sqliterb': 'sqliterb-reqscale-240000-insertions-4',
            'varmail': 'varmail-reqscale-002',
            'tmp': 'rocks_and_level_write_noOOS2/subexp-3959790978413626819-f2fs-10-10-16-41-16--4009885425202000064',
            }

    table =\
        {
         'group0': ['rocks_and_level_write_noOOS2/subexp--1769718657183269759-ext4-10-10-16-40-18--5327014900870736072',
                    'rocks_and_level_write_noOOS2/subexp-3959790978413626819-f2fs-10-10-16-41-16--4009885425202000064',
                    'rocks_and_level_write_noOOS2/subexp--5400495843365659613-f2fs-10-10-16-50-41-1036897982225647212'],
         'group1': ['rocks_and_level_write_noOOS2/subexp--2651559459791749708-ext4-10-10-16-37-11--2476577234383557317',
                    'rocks_and_level_write_noOOS2/subexp--4012079044899239942-f2fs-10-10-16-47-24-8962436495046765337',
                    'rocks_and_level_write_noOOS2/subexp-6734567197784924450-f2fs-10-10-16-38-08-5525072682590038137'],
         'group2': ['rocks_and_level_write_noOOS2/subexp-3061564522848141715-xfs-10-10-16-48-19--5395887198175941667',
                    'rocks_and_level_write_noOOS2/subexp--407794861899277989-ext4-10-10-16-34-23--1388338513924544090',
                    'rocks_and_level_write_noOOS2/subexp-731938296861647804-ext4-10-10-16-46-26--7861811878071624305'],
         'group3': ['rocks_and_level_write_noOOS2/subexp-3094175081237092577-ext4-10-10-16-49-42-7697824717987838947',
                    'rocks_and_level_write_noOOS2/subexp--4321129376877859415-f2fs-10-10-16-44-25--7204748025581881532',
                    'rocks_and_level_write_noOOS2/subexp--7650075805024570086-xfs-10-10-16-45-18--9114967625115597626'],
         'group4': ['rocks_and_level_write_noOOS2/subexp-3348595430537727579-xfs-10-10-16-39-02-2209328006332006116',
                    'rocks_and_level_write_noOOS2/subexp-4770662763680469818-xfs-10-10-16-36-11--4521167812851410244',
                    'rocks_and_level_write_noOOS2/subexp--8663889719348096684-xfs-10-10-16-51-37--3241077762353620515'],
         'group5': ['rocks_and_level_write_noOOS2/subexp-3633859889505807604-xfs-10-10-16-42-11--796626381758564691',
                    'rocks_and_level_write_noOOS2/subexp--4837249144845266743-f2fs-10-10-16-35-18--8464917546135255015',
                    'rocks_and_level_write_noOOS2/subexp-8721179042090565947-ext4-10-10-16-43-29--8110094886926018731'],

         'sqliterb-align-0': ['sqliterb-reqscale-240000-insertions-4/subexp--7571909590259533821-ext4-10-08-00-05-29-5223497787234525006',
                              'sqliterb-reqscale-240000-insertions-4/subexp--3141442527781592876-f2fs-10-07-23-45-07--6856869384304485438'],
         'sqliterb-align-1': ['sqliterb-reqscale-240000-insertions-4/subexp-2742809270733970649-xfs-10-07-23-59-30--8341394983503659935',
                              'sqliterb-reqscale-240000-insertions-4/subexp-8553389467829355426-f2fs-10-08-00-03-23-1880758771844807441'],
         'sqliterb-align-2': ['sqliterb-reqscale-240000-insertions-4/subexp-2758345266501184620-f2fs-10-08-00-13-21-866783223289054574',
                              'sqliterb-reqscale-240000-insertions-4/subexp-2336942718368821990-ext4-10-08-00-10-42-9127803783073608824'],
         'sqliterb-align-3': ['sqliterb-reqscale-240000-insertions-4/subexp--1811502557637329453-xfs-10-08-00-04-23--7480859084318509711',
                              'sqliterb-reqscale-240000-insertions-4/subexp--2208768073357703318-ext4-10-07-23-44-02--3698939624785112052'],
         'sqliterb-align-4': ['sqliterb-reqscale-240000-insertions-4/subexp--611519970204123274-ext4-10-07-23-54-06-3954495646325573485',
                              'sqliterb-reqscale-240000-insertions-4/subexp--2997539756754670987-ext4-10-07-23-47-09-3798978770958489688'],
         'sqliterb-align-5': ['sqliterb-reqscale-240000-insertions-4/subexp-4908908674413587590-xfs-10-08-00-08-54--2361949898767176570',
                              'sqliterb-reqscale-240000-insertions-4/subexp-7524125378098933777-f2fs-10-08-00-07-24-5305576362074031946'],
         'sqliterb-align-6': ['sqliterb-reqscale-240000-insertions-4/subexp-8302396001383885851-f2fs-10-07-23-49-33--1967830900465990693',
                              'sqliterb-reqscale-240000-insertions-4/subexp--3556361485728753380-ext4-10-08-00-02-14-442952194550896543'],
         'sqliterb-align-7': ['sqliterb-reqscale-240000-insertions-4/subexp-3609841292231416489-xfs-10-08-00-15-22--5707918999480787436',
                              'sqliterb-reqscale-240000-insertions-4/subexp-6162233044671063036-f2fs-10-07-23-56-58--432793293883955356'],
         'sqliterb-align-8': ['sqliterb-reqscale-240000-insertions-4/subexp-1961487334150063805-xfs-10-07-23-46-02-3881305204640570514',
                              'sqliterb-reqscale-240000-insertions-4/subexp--8986550952199358176-xfs-10-07-23-51-44-3510605567209788381'],
         }

    if app in table.keys():
        trace_expnames = table[app]
    else:
        trace_expnames = [ appmap[app] ]

    print trace_expnames
    time.sleep(1)

    # rule = 'locality'
    # rule = 'localitysmall'
    # rule = 'alignment'
    # rule = 'grouping'

    for para in filesim.ParaDict(expname, trace_expnames, rule):
        execute_simulation(para)


def execute_simulation(para):
    """
    INPUT: para is a dictionary generated by filesim.ParaDict

    This function is only for simulating blktrace events as LBA workload
    """
    Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
    obj = filesim.LocalExperimenter( Parameters(**para) )
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





