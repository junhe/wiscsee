import os

from utilities.utils import *

def flamegraph_wrap(perf_path, cmd, result_dir, flamegraph_dir):
    """
    cmd will be executed by perf and the statistics will be saved to result_dir

    flamegraph_dir = '/users/jhe/flamegraph'
    """

    stackcollapse_perf = os.path.join(flamegraph_dir, "stackcollapse-perf.pl")
    flamegraph_path = os.path.join(flamegraph_dir, "flamegraph.pl")

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    with cd(result_dir):
        shcmd("{perf_path} record -F 99 -a -g -o myperfrecord -- {cmd}".format(
            perf_path = perf_path, cmd = cmd))
        shcmd("{perf_path} script -i myperfrecord > out.perf".format(
            perf_path = perf_path))
        shcmd("{cmd} out.perf > out.folded".format(cmd = stackcollapse_perf))
        shcmd("{cmd} out.folded > kernel.svg".format(cmd = flamegraph_path))


