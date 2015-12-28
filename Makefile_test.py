import unittest

from Makefile import *


class Experiment(object):
    def __init__(self):
        # Get default setting
        self.conf = config.Config()

    def setup_environment(self):
        raise NotImplementedError

    def setup_workload(self):
        raise NotImplementedError

    def setup_ftl(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def main(self):
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.run()

class DftlExp(Experiment):
    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        self.conf["workload_src"] = LBAGENERATOR
        self.conf["age_workload_class"] = "NoOp"

    def setup_ftl(self):
        self.conf['ftl_type'] = 'dftl2'
        self.conf["enable_e2e_test"] = True

        devsize_mb = 1024
        entries_need = int(devsize_mb * 2**20 * 0.03 / self.conf['flash_page_size'])
        self.conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
        self.conf['interface_level'] =  'page'
        self.conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))

    def run(self):
        runtime_update(self.conf)
        workflow(self.conf)


class DftlTest(unittest.TestCase):
    def test_001(self):
        exp = DftlExp()
        exp.main()

    def off_test_basic(self):
        confdic = get_default_config()
        conf = config.Config(confdic)
        conf['ftl_type'] = 'dftl2'

        metadata_dic = choose_exp_metadata(conf, interactive = False)
        conf.update(metadata_dic)

        def run(fs, nfiles, divider, filesize, chunksize):
            conf["age_workload_class"] = "NoOp"

            devsize_mb = 1024 / divider
            entries_need = int(devsize_mb * 2**20 * 0.03 / conf['flash_page_size'])
            conf['dftl']['max_cmt_bytes'] = int(entries_need * 8) # 8 bytes (64bits) needed in mem
            conf['interface_level'] =  'page'
            conf.set_flash_num_blocks_by_bytes(int(devsize_mb * 2**20 * 1.28))
            conf['loop_dev_size_mb'] = devsize_mb
            conf['filesystem'] =  fs

            conf['enable_blktrace'] = True
            conf['enable_simulation'] = True

            # Setup workload
            # filesize = 256 + 128) * MB
            # chunksize = 16 * KB
            bytes_to_write = filesize * 8
            conf["workload_class"] = "Synthetic"
            conf["workload_conf"] = {
                    # "generating_func": "self.generate_hotcold_workload",
                    # "generating_func": "self.generate_sequential_workload",
                    # "generating_func": "self.generate_backward_workload",
                    # "generating_func": "self.generate_random_workload",
                    "generating_func": "self.generate_parallel_random_writes",
                    # "generating_func": gen_func,
                    # "chunk_count": 100*2**20/(8*1024),
                    "chunk_count": filesize / chunksize,
                    "chunk_size" : chunksize,
                    "iterations" : int(bytes_to_write/filesize),
                    "filename"   : "test.file",
                    "n_col"      : 5,   # only for hotcold workload
                    "nfiles"     : nfiles   # for parallel random writes
                }

            runtime_update(conf)

            workflow(conf)


        for fs in ('ext4', 'f2fs'):
            for nfiles in (2, 1):
                for filesize in (32 * MB, 128 * 3 * MB):
                    for chunksize in (1024 * KB, 512 * KB):
                        if nfiles == 1:
                            run(fs = fs, nfiles = nfiles, divider = 2,
                                filesize = filesize, chunksize = chunksize)
                        else:
                            run(fs = fs, nfiles = nfiles, divider = 1,
                                filesize = filesize, chunksize = chunksize)
                        exit(0)

        self.assertTrue(True)



def main():
    unittest.main()

if __name__ == '__main__':
    main()

