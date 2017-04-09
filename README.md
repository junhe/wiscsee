Release Notes
-------------------------

Demo
- Run workload and get traces, without simulation (DONE)
- Run workload, get trace, and simulate on dftl (DONE)
- Run workload, get trace, and simulate on nkftl (DONE)
- Generate LBA workload and simulate (DONE)
- Specify trace and simulate on dftl
- Specify trace and simulate on nkftl
- Specify trace and study request scale
- Specify trace and study locality
- Specify trace and study alignment
- Specify trace and study grouping
- Specify trace and study data lifetime


- Request Scale
    - Output the table with timestamp, queue depth, ...
- Locality
    - Report the miss ratio
- Aligned Sequentiality
    - Report the unaligned ratio
- Grouping by Death Time
    - Report the snapshots of valid ratios
- Uniform Data Lifetime
    - The raw block trace should be enough

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


