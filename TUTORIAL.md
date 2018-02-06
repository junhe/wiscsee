After walking through this tutorial, you will learn the following. 

- How to run your application with WiscSee and get results for all the rules
- Where the results are located
- What are in the results and how to interpret the results
- How to run a preparation workload before your workload

In this tutorial, let's assume that the application we study is the Linux `dd`
command. We also pretend that `/dev/loop0` is an SSD. We will use `dd` to write
to a file system mounted on `/dev/loop0`. The traces of the write workload 
will be fed to our SSD simulator (WiscSim).

Basically, WiscSee will do the following.

- Make a file system on a disk partition
    - Block traces will be collected
- Mount a file system on the partition
    - Block traces will be collected
- Run a preparation workload (a.k.a. aging workload, which is optional)
    - Block traces will be collected
- Run a workload of interest (Linux `dd` command for this tutorial)
    - Block traces will be collected
- Feed traces to WiscSim (the SSD simulator)

Now let us see how to get results for each of the rules.

# Request Scale and Uniform Data Lifetime (Produce Traces)

We study Request Scale by NCQ depths and request sizes. We study Uniform
Data Lifetime by write counts on logical addresses. To get these results, 
you will need:

1. Specify your application

Open `workrunner/workload.py`, add the following code

```
class LinuxDdWrite(Workload):
    def __init__(self, confobj, workload_conf_key = None):
        super(LinuxDdWrite, self).__init__(confobj, workload_conf_key)

    def run(self):
        mnt = self.conf["fs_mount_point"]
        cmd = "dd if=/dev/zero of={}/datafile bs=64k count=128".format(mnt)
        print cmd
        subprocess.call(cmd, shell=True)
        subprocess.call("sync")

    def stop(self):
        pass
```

In the next step, we will tell WiscSee to use this class.


2. Setup the Experiment

Open `tests/test_demo.py`, add the following test class. (We implement experiments as tests for convenience.)

```
class TestLinuxDdReqscaleAndDataLifetime(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "LinuxDdWrite"

        para = experiment.get_shared_nolist_para_dict(expname="linux-dd-exp",
                                                      lbabytes=1024*MB)
        para.update(
            {
                'device_path': "/dev/loop0",
                'ftl' : 'ftlcounter',
                'enable_simulation': True,
                'dump_ext4_after_workload': True,
                'only_get_traffic': False,
                'trace_issue_and_complete': True,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()
```

`self.conf['workload_class'] = "LinuxDdWrite"` tells WiscSee to use class `LinuxDdWrite`, which we just added in the previous step, to run the application. 

`experiment.get_shared_nolist_para_dict()` returns a pre-set configuration,
which you can override. This function accepts two parameters, `expname` is the
name of the experiment, which will be used to name the result directory 
(`/tmp/results/linux-dd-exp`). `lbabytes` is the size of the LBA space. A 
reasonable size is 1024 MB. Note that if the LBA size is too small, some
file systems may not be able to run. For example, F2FS requires at least 256 MB.

`device_path` is the partition that WiscSee will format
and mount a file system on. 
**To get realistic results, you should use a real SSD, which gives
you realistic queue depths.**
`ftl` specifies the simulator that WiscSee will
feed the block traces to. The FTL used here, `ftlcounter`, analyzes the
traces and produces NCQ depths, request sizes and logical address write counts.
(`ftlcounter` does not simulate an SSD.) 

You may check the source code of `get_shared_nolist_para_dict()` for more options.  For example, you can change the file system by overriding `filesystem`:

```
        para.update(
            {
                'filesystem': "f2fs", # or "ext4", "xfs"
            })
```

3. Run the Experiment

```
./run_testclass.sh tests.test_demo.TestLinuxDdReqscaleAndDataLifetime
```

4. Check the results 

WiscSee puts results to `/tmp/results/$expname/$subexpname` (for example, `/tmp/results/linux-dd-exp/subexp-8265515130178797917-ext4-02-06-15-24-26-497134079608212519`).

The NCQ depths and request sizes are in a file called `ncq_depth_timeline.txt`.
Here is a sample of the file.

```
pre_depth;post_depth;offset;action;timestamp;operation;pid;size
0;1;299008;D;0.000000000;OP_READ;16030;4096
1;0;299008;C;0.000036622;OP_READ;15922;4096
0;1;331776;D;0.000074849;OP_READ;16030;4096
1;0;331776;C;0.000079772;OP_READ;15922;4096
```

The file has multiple columns separated by `;`. Each row represents an I/O
request.  It should be easy for you to parse the file and analyze its data.
`pre_depth` is the NCQ depth before the request of that row is issued.
`post_depth` is the NCQ depth after the request is issued. 
`offset` (unit: byte)  is the logical offset of the request.
`size` (unit: byte) is the size of the request.
`action` indicates if a request is being issued (`D`) or completed (`C`).
The other columns are self-explanatory.

You can identify a request by its offset. For example, from the sample 
we can see that the READ request to offset `299008` is issued at the
first row and then completed at the second row.

The write counts of each logical addresses are in a file called `lpn.count`.
Here is a sample of the file.

```
lpn read write discard
0 2 4 0
1 1 4 0
2 1 2 0
```

The file has multiple columns separated by spaces. Each row represents
a logical page (usually 2 KB or 4 KB in WiscSee). For example,
this sample shows that logical page number `1` has been read 1 time, 
written 4 times, and discarded 0 times.


Just in case you are interested, here is a list of what each file in
the result directory is for.

```
accumulator_table.txt                   value of various counters set in the simulator
app_duration.txt                        duration of running application (wall clock time)
blkparse-output-mkfs.txt                raw trace of mkfs from blktrace
blkparse-output.txt                     raw trace of running the  application on file system from blktrace
blkparse-events-for-ftlsim-mkfs.txt     refined trace
blkparse-events-for-ftlsim.txt          refined trace 
config.json                             the configuration of the experiment
dumpe2fs.out                            dumpe2fs results of ext4  
recorder.json                           various statistics, such as valid ratio distributions, number of flash writes, ...
recorder.log                            no longer used
```

# Locality

Because you have already got the traces of your application from the
previous part of this tutorial, you do not need to generate traces for 
studying locality. You can reuse the traces in `/tmp/results/linux-dd-exp`.

To get hit/miss ratios of the translation cache, you will need to do the following.

1. Setup the Experiment 

Open `tests/test_demo.py`, add the following test class.  

```
class TestLinuxDdLocality(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(expname="linux-dd-locality", 
                                            trace_expnames=['linux-dd-exp'],
                                            rule="locality"):
            experiment.execute_simulation(para)
```

`expname` is name of the current experiment for locality. The data of 
this experiment will go to `/tmp/results/linux-dd-locality` in this example. 
`trace_expnames` is the a list of experiment names that WiscSee will use 
to find traces. For example, WiscSee will look for traces in
`/tmp/results/linux-dd-exp`. `rule` is the rule we study. If `locality` is the
rule, WiscSee will run simulations with several different cache coverages. 
You can change the coverage ratios in class `LocalityParaIter` in 
`config_helper/rule_parameter.py`. Do not set the coverage too low, as  
SSDs need some coverage to operate.

2. Run the Experiment

```
./run_testclass.sh tests.test_demo.TestLinuxDdLocality
```

3. Get the Results

The results are in a file named `recorder.json`. You should be able
to parse the file with any json parser. The miss and hit counts of
the translation cache are in `['general_accumulator']['Mapping_Cache']`.
Here is a sample.

```
        "Mapping_Cache": {
            "miss": 9, 
            "hit": 4137
        }, 
```

The sample indicates that there were 9 misses and 4137 hits. So 
the miss ratio is `9 / (9 + 4137) = 0.002`.

# Aligned Sequentiality

Again, we will feed the traces produced by the first part of this tutorial 
to the simulator.

To get results for Aligned Sequentiality, you will need to do the following.

1. Setup the Experiment 

Open `tests/test_demo.py`, add the following test class.  

```
class TestLinuxDdAlignment(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(expname="linux-dd-alignment", 
                                            trace_expnames=['linux-dd-exp'],
                                            rule="alignment"):
            experiment.execute_simulation(para)
```

WiscSee will feed the traces to two simulations with different block sizes
(128 KB and 2 MB)

2. Run the experiment

```
./run_testclass.sh tests.test_demo.TestLinuxDdAlignment
```

3. Get the Results

The results are also in `recorder.json`. The counts of operations for
full merges and partial merges are in `['general_accumulator']['FULL.MERGE']`
and `['general_accumulator']['PARTIAL.MERGE']`, respectively.
You may not have a particular type of merge presented in the file because
it is never triggered.
Note that the write count and the read count of a merge operation are always
the same because what has been read will eventually be written. 
The unit of the count is "one page". Since the default page size in WiscSee 
is 2 KB, the total bytes moved during merges are
`['FULL.MERGE']['physical_write'] * 2KB + ['PARTIAL.MERGE']['physical_write'] * 2KB`.

To calculate the valid ratio, you also need the size of logical space used,
which is the total size of your files. You can find that out by `ls` or `du`
command.

# Grouping by Death Time

Once again, we will feed the traces produced by the first part of this tutorial 
to the simulator.

To get the data for plotting zombie curves, do the following.

1. Setup the Experiment 

Open `tests/test_demo.py`, add the following test class.  

```
class TestLinuxDdGrouping(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(expname="linux-dd-grouping", 
                                            trace_expnames=['linux-dd-exp'],
                                            rule="grouping"):
            experiment.execute_simulation(para)
```

This will configure the SSD simulator to have infinite over-provisioning so
that you can study how well your workload conform to Grouping by Death Time.

2. Run the experiment

```
./run_testclass.sh tests.test_demo.TestLinuxDdGrouping
```

3. Get the Results

The results are also in `recorder.json`. Here is a sample.

```
    "ftl_func_valid_ratios": [
        {
            "1.00": 128
        }, 
        {
            "1.00": 128, 
            "0.97": 8, 
            "0.98": 8
        }, 
        {
            "0.95": 16, 
            "1.00": 144
        }, 
        ...
    ]
```

Each `{...}` is a snapshot of the valid ratio counts. For example, `"0.97": 8`
indicates that there are `8` flash blocks with a valid ratio `0.97`. You 
probably should check multiple snapshots to make sure the SSD has entered
a stable state. Otherwise, you are just studying some non-representative 
transient state.

Using the data in `ftl_func_valid_ratios`, you can create an animation of how
the valid ratios change over time, which enjoy a lot :)

# Run a Preparation Workload Before Your Final Workload

Sometimes you may want to run a preparation workload before your final workload. 
For example, in order to get the request scale of a read workload, 
you must run a write workload to fill your files (so you have something to read). This write workload is 
the preparation workload (we also call it an aging workload in WiscSee).
The read workload is the final workload.

This section of the tutorial introduces how to run `dd` to write a file 
(the preparation workload) and then run `dd` to read the file (the final
workload). WiscSee will produce results only for `dd` read, since that is what we
care about.

1. Create the Preparation Workload

We have done this at the beginning of this tutorial. Class `LinuxDdWrite` in `workload.py` will be our preparation workload.

2. Create the Final Workload

Open `workrunner/workload.py`, add the following code

```
class LinuxDdRead(Workload):
    def __init__(self, confobj, workload_conf_key = None):
        super(LinuxDdRead, self).__init__(confobj, workload_conf_key)

    def run(self):
        mnt = self.conf["fs_mount_point"]
        cmd = "dd if={}/datafile of=/dev/zero bs=64k count=128".format(mnt)
        print cmd
        subprocess.call(cmd, shell=True)
        subprocess.call("sync")

    def stop(self):
        pass
```

Note that `LinuxDdRead` will read the file we created in `LinuxDdWrite`.

3. Setup the Experiment


```
class TestLinuxDdReadReqscaleAndDataLifetime(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['age_workload_class'] = "LinuxDdWrite"
                self.conf['workload_class'] = "LinuxDdRead"

        para = experiment.get_shared_nolist_para_dict(
                        expname="linux-dd-with-preparation",
                        lbabytes=1024*MB)
        para.update(
            {
                'device_path': "/dev/loop0",
                'ftl' : 'ftlcounter',
                'enable_simulation': True,
                'dump_ext4_after_workload': True,
                'only_get_traffic': False,
                'trace_issue_and_complete': True,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()
```

`self.conf['age_workload_class'] = "LinuxDdWrite"` tells WiscSee to use `LinuxDdWrite` as the preparation workload. `self.conf['workload_class'] = "LinuxDdRead"` tells WiscSee to use `LinuxDdRead` as the final workload.

4. Run the Experiment

```
./run_testclass.sh tests.test_demo.TestLinuxDdReadReqscaleAndDataLifetime
```

5. Get the Results

The results are in `/tmp/results/linux-dd-with-preparation`. Just as before, you can
reuse the results of this experiment to study other rules.

