import os


from config import WLRUNNER, LBAGENERATOR, LBAMULTIPROC
from commons import *
from utilities.utils import *
import wiscsim
from wiscsim.simulator import create_simulator
import workrunner


def run_workflow(conf):
    wf = Workflow(conf)
    wf.run()


class Workflow(object):
    def __init__(self, conf):
        self.conf = conf

    def run(self):
        self._save_conf()
        event_iter = self._run_workload()
        self._run_simulator(event_iter)

    def run_simulator(self, event_iter):
        self._save_conf()
        return self._run_simulator(event_iter)

    def run_workload(self):
        self._save_conf()
        return self._run_workload()

    def _save_conf(self):
        confpath = os.path.join(self.conf['result_dir'], 'config.json')
        prepare_dir_for_path(confpath)
        self.conf.dump_to_file(confpath)

    def _run_workload(self):
        workload_src = self.conf['workload_src']
        if workload_src == WLRUNNER:
            runner = workrunner.wlrunner.WorkloadRunner(self.conf)
            event_iter = runner.run()
        elif workload_src == LBAGENERATOR:
            classname = self.conf['lba_workload_class']
            cls = eval("workrunner.lbaworkloadgenerator.{}".format(classname))
            lbagen = cls(self.conf)
            event_iter = lbagen
        elif workload_src == LBAMULTIPROC:
            classname = self.conf['lba_workload_class']
            cls = "workrunner.lbaworkloadgenerator.{}".format(classname)
            lbagen = cls(self.conf)
            event_iter = lbagen.get_iter_list()
        else:
            raise RuntimeError("{} is not a valid workload source"\
                .format(workload_src))

        return event_iter

    def _run_simulator(self, event_iter):
        if self.conf['enable_simulation'] is not True:
            return

        simulator = create_simulator(self.conf['simulator_class'], self.conf,
                event_iter )
        simulator.run()


