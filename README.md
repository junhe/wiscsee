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

# Set aging workload

`benchmarks/experimenter.py` `RealDevExperimenter` sets `self.conf['aging_workload_class'], self.conf['aging_workload_config']`, which are later used to run the aging workload. 

To pass configuration to the configuration above, you need to set `self.para.age_workload_class` and `self.aging_appconfs`.


# Set regular workload 

The current best way is to embed the new workload to class `AppMix` 
in `workrunner/workload.py`.



# Do this before you do everything


```
cd ./utilities/
setup.env.sh
```

This will install all the prerequisites (many of which you do not
need for this tutorial).


# Instruction for running on real device

To get the results for request scale and data lifetime, you need to run
experiments on real device. Here are the instructions for getting 
request scale and data lifetime of an application (Linux dd).

1. Add a 'process' class to `./workrunner/appprocess.py`

    This class is where you use subprocess to run `dd`.

2. Hook `LinuxDdProc` with `class AppMix` located in `./workrunner/workload.py`. 

    `AppMix` is wrapper class for 'process' class. `AppMix` allows you to run multiple processes together. 
    In this step, you should tell `AppMix` how to create `LinuxDdProc` instances

3. Add configurations for `AppMix` in `./benchmarks/expconfs.py` 

    The purpose of this step is to add configurations which will be
    passed to `AppMix` later. After `AppMix` receives the configuration,
    it will pass parts of it to `LinuxDdProc`

4. Also in `./benchmarks/expconfs.py`, add a function 
`linuxdd_reqscale_w_seq()` to class `ParameterPool`.

    This function put the `AppMix` configuration to a bigger 
    configuration dictionary.

5. Enable invoking the experiment from command line.

    Put the following to `testname_dict` in `./benchmarks/appbench.py`.

```
    'linuxdd':
        [
            'linuxdd_reqscale_w_seq'
        ],
            
```

6. Change the device on which you want to run

    In `./benchmarks/experimenter.py`, change the following line to 
    change the device.

    ```
            'device_path'    : ['/dev/sdb1'],
    ```

7. Run the experiment!

    ```
    $ make appmix4rw testsetname=linuxdd expname=linuxdd-exp-001
    ```

    After running, you results will be in `/tmp/results/linuxdd-exp-001`.




# Run simulation on traces

Our simulations need traces collected from experiments that were run on 
real devices. Now, let's assume that you have successfully run Linux dd
on real device, and the experiment results are at 
`/tmp/results/linuxdd-exp-001`. 

1. Put experiment folder name of the traces to `appmap` in `benchmarks/appbench.py`.

```
appmap = {
        'linuxdd-for-sim': 'linuxdd-exp-001',
        }
```

'linuxdd-exp-001' will be interpreted as '/tmp/results/sqlserver-all-patterns' internally. WiscSee will look for traces there. 

2. Run the simulation

```
$ make simevents app=linuxdd-for-sim rule=grouping expname=new-exp-for-grouping
```

Choices for rule include 

- locality
- localitysmall: tiny coverage
- alignment
- grouping

After running, the results are in `/tmp/results/new-exp-for-grouping`


# Plot results

In this section, we will use `analyze-scale.r` to plot results of request scale.

1. Install R

    Our plotting scripts are written in R, so you need R.

2. Install required R packages 
    
    They include:

    ```
    ggplot2
    plyr
    dplyr
    reshape2
    gridExtra
    jsonlite
    digest

    ```

3. Set script path in the R script

    Suppose the plotting repository is at `/Users/junhe/workdir/analysis-script/`
    
    In `doraemon/analyze-scale.r`, change `WORKDIRECTORY` and
    to be

    ```
    WORKDIRECTORY= "/Users/junhe/workdir/analysis-script/"

    ```
    

3. Download results from your experiment machine.

    Something like

    ```
    scp -r jhe@cloudlab.com:/tmp/results/linuxdd-exp-001 /Users/junhe/downloads/
    ```

4. Change the lookup path for experiment results

    In `doraemon/header.r`, change function `get_exp_path()` to be

    ```
get_exp_path <- function(expdir)
{
    p = paste("/Users/junhe/downloads/", expdir, sep='')
    return(p)
}
    ```
    
5. Let the plotting script know the result folder name.

    
    Change the invokation of `batch_plot()` in `main()` of 
    analyze-scaling.r to 

    ```
    batch_plot(c(
                 'linuxdd-exp-001'
                 ))

    ```

6. Plot!

    Copy the `sme()` function (shown below) to R console.
    
    ```
# copy the following so you can do sme()
sme <- function()
{
    WORKDIRECTORY= "/Users/junhe/workdir/analysis-script/"
    THISFILE     = 'doraemon/analyze-scaling.r'
    setwd(WORKDIRECTORY)
    source(THISFILE)
}
    ```

    Then run `sme()` by typing in 

    ```
    sme()
    ```
    And then hit ENTER.
