Release Notes
-------------------------

Demo
- Run workload and get traces, without simulation (DONE)
- Run workload, get trace, and simulate on dftl (DONE)
- Run workload, get trace, and simulate on nkftl (DONE)
- Generate LBA workload and simulate (DONE)
- Specify trace and simulate on dftl       10min (7:33-7:52) 20min (DONE)
- Specify trace and simulate on nkftl      10min (DONE, just use the one above)
- Specify trace and study request scale    20min (8:08-
- Specify trace and study locality         10min
- Specify trace and study alignment        10min
- Specify trace and study grouping         10min
- Specify trace and study data lifetime    10min


TODO:

- Remove useless workloads
- Remove useless setup codes


Doraemon Manual
-------------------------

Makefile.py is no longer used. I usually start running experiments
by Makefile.

Most of the experiments can be run with `make xxxx`. The implementation
of the experiment setup is in ./benchmarks/appbench.py. Right now there 
are only a few functions in appbench.py, so it won't be too hard to run.

Breifly, `appmixbench_for_rw()` is for running on real device, for example, 
doing request scale test on SSDs. `execute_simulation` is for running
traces on SSD simulator.  

You can run simulation on multiple nodes by celery. Use `run-celery-on-nodes.py`
to run celery. Use `./taskpusher-integration.py` to push tasks to workers.

The traces needed for simulation are in `datahose/localreults` on the MAC.

The current master branch has the latest code that is used for EuroSys submission.


