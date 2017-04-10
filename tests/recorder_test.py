import config
import unittest

import wiscsim
from utilities.utils import *

class TestFTLwithDFTL(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)

        recorder = wiscsim.recorder.Recorder(
                output_target = wiscsim.recorder.FILE_TARGET,
                output_directory = "/tmp"
                )
        recorder.enable()

        recorder.add_to_general_accumulater("counter_set_1", "counter1", 3)
        recorder.add_to_general_accumulater("counter_set_1", "counter1", 4)
        self.assertEqual(
                recorder.general_accumulator["counter_set_1"]["counter1"], 7)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()

class TestCountMe(unittest.TestCase):
    def setup_config(self):
        self.conf = config.ConfigNCQFTL()

    def setup_environment(self):
        metadata_dic = choose_exp_metadata(self.conf, interactive = False)
        self.conf.update(metadata_dic)

        self.conf['enable_blktrace'] = True
        self.conf['enable_simulation'] = True

    def setup_workload(self):
        pass

    def setup_ftl(self):
        pass

    def my_run(self):
        runtime_update(self.conf)

        recorder = wiscsim.recorder.Recorder(
                output_target = wiscsim.recorder.FILE_TARGET,
                output_directory = '/tmp'
                )
        recorder.enable()

        recorder.count_me("counter_name_1", "item1")
        recorder.count_me("counter_name_1", "item1")
        self.assertEqual(recorder.get_count_me("counter_name_1", "item1"), 2)

    def test_main(self):
        self.setup_config()
        self.setup_environment()
        self.setup_workload()
        self.setup_ftl()
        self.my_run()



def main():
    unittest.main()

if __name__ == '__main__':
    main()





