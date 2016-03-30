import unittest
import pprint

from Makefile import *


class TestManager(unittest.TestCase):
    def init(self):
        # Get default setting
        self.conf = FtlSim.dftldes.Config()

        Parameters = collections.namedtuple("Parameters",
            "numjobs, bs, iodepth, expname, size")
        self.para = Parameters(
                        numjobs = 1,
                        bs = 4*KB,
                        iodepth = 1,
                        expname = 'testblocktrace',
                        size = 1*MB)
        self.conf['exp_parameters'] = self.para._asdict()

    def setup_environment(self):
        self.conf['device_path'] = "/dev/loop0"
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
        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = False
        self.conf['simulator_class'] = 'SimulatorDES'
        self.conf['ftl_type'] = 'dftldes'

    def run_fio(self):
        workload = WlRunner.workload.FIONEW(self.conf,
                workload_conf_key = self.conf['workload_conf_key'])
        workload.run()

    def my_run(self):
        set_exp_metadata(self.conf, save_data = True,
                expname = self.para.expname,
                subexpname = chain_items_as_filename(self.para))
        runtime_update(self.conf)

        # self.run_fio()
        workflow(self.conf)

    def test_main(self):
        self.init()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestBlocktraceResult(unittest.TestCase):
    def test_main(self):
        self.conf = FtlSim.dftldes.Config()
        blkresult = WlRunner.blocktrace.BlktraceResult(self.conf,
                './testdata/blkparse-output.txt',
                '/tmp/blkparse-output.txt.parsed')
        blkresult.create_event_file()

        print blkresult.get_duration()
        print blkresult.count_sectors('read')
        print blkresult.count_sectors('write')
        print blkresult.get_bandwidth_mb('read')
        print blkresult.get_bandwidth_mb('write')

def main():
    unittest.main()

if __name__ == '__main__':
    main()


