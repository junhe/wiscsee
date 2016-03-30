import unittest
from Makefile import *


class TestConf(unittest.TestCase):
    def test_conf(self):
        jobconf = WlRunner.fio.JobConfig()

        jobconf.append_section("global", {'size': 100, 'fsync':12})
        jobconf.append_section("job1",
                {'size': 100, 'fsync':12, 'rw': 'write'})
        self.assertEqual(jobconf.get('job1', 'fsync'), 12)
        self.assertEqual(jobconf.get('global', 'size'), 100)


class TestFio(unittest.TestCase):
    def setup_environment(self):
        self.conf = ssdbox.dftldes.Config()

    def setup_workload(self):
        self.fio_job_conf = WlRunner.fio.JobConfig()
        self.fio_job_conf.append_section("global", {'size': '8kb'})
        self.fio_job_conf.append_section("job1",
                {'rw': 'write', 'filename': '/tmp/tmp.fio.file'})

        self.conf['fio_job_conf'] = {
            'ini': self.fio_job_conf.as_ordered_dict(),
            'runner': {}
            }

    def setup_ftl(self):
        pass

    def my_run(self):
        set_exp_metadata(self.conf, save_data = False,
                expname = 'testexp',
                subexpname = 'testsubexp')
        runtime_update(self.conf)

        workload = WlRunner.workload.FIONEW(self.conf,
                workload_conf_key = 'fio_job_conf')
        workload.run()

    def test_main(self):
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()


def main():
    unittest.main()

if __name__ == '__main__':
    main()

