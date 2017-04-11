WiscSee is an I/O workload analyzer that helps you understand your application
performance on SSDs. WiscSee comes with a fully functioning trace-driven SSD simulator,
WiscSim, which supports enhanced versions of multiple well-known FTLs, NCQ, multiple
channels, garbage collections, wear-leveling, page allocation policies and more.
WiscSim is implemented as a Discrete-Event Simulator.

WiscSee runs your application, collects its block I/O trace, and later feeds the trace
to WiscSim.

WiscSee contains several demos to help you get started quickly. The demos show

- How to run your application in WiscSee
- How to specify the file system you use
- How to trace the I/O of your application
- How to trace an application and simulate the I/O workload on an SSD simulator
- How to simulate synthetic workloads (operations on LBA) on an SSD simulator
- How to evaluate whether your workloads conform/violate the rules of the
  unwritten contract of SSDs (see "The Unwritten Contract of Solid State Drives"
  in EuroSys'17)


# Download and Setup

#### Option 1: VM Image

We made a VirtualBox VM Image that has the complete environment ready (Ubuntu
16.04 + WiscSee). You do not need to do any configuration. It is the easiest
option in terms of setting up. It is garanteed to run.

In order to use this option, you need to have VirtualBox
(https://www.virtualbox.org/) installed. 

1. Download VirtualBox Image from the following address: 

```
http://pages.cs.wisc.edu/~jhe/wiscsee-vm.tar.gz
```

The SHA245 sum of the file is:

```
80c5f586d525e0fa54266984065b2b727f71c45de8940aafd7247d49db8e0070
```

2. Untar the downloaded file

3. Open the VM image with VirtualBox. 

This VM image may also work with other VM manager.

4. Login to the guest OS

```
Username: wsee
Password: abcabc
```

5. Run WiscSee tests

```
cd /home/wsee/workdir/wiscsee
make test_all
```

The root password is:

```
abcabc
```

6. Run DEMOs

```
make run_demo
```

#### Option 2: Git clone

1. Setup

```
make setup
```

2. Run tests

```
make test_all
```

# Run Demo

```
make run_demo
```

The code of the demos is in `tests/test_demo.py`.

# Tutorial: study your application on an SSD simulator

In this short tutorial, let's assume that the application we study is the Linux `dd`
command. We also pretend that `/dev/loop0` is an SSD. We will use `dd` to write
to a file system mounted on `/dev/loop0`. We simulate this workload on an SSD
simulator.

#### 1. Specify your application 

Open `workrunner/workload.py`, add the following code

```
class LinuxDD(Workload):
    def __init__(self, confobj, workload_conf_key = None):
        super(LinuxDD, self).__init__(confobj, workload_conf_key)

    def run(self):
        mnt = self.conf["fs_mount_point"]
        cmd = "dd if=/dev/zero of={}/datafile bs=64k count=128".format(mnt)
        print cmd
        subprocess.call(cmd, shell=True)
        subprocess.call("sync")

    def stop(self):
        pass
```

In the next step we will tell WiscSee to use this class.

#### 2. Setup Experiment

Open `tests/test_demo.py`, add the following code

```
class Test_TraceAndSimulateLinuxDD(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "LinuxDD"

        para = experiment.get_shared_nolist_para_dict("test_exp_LinuxDD", 16*MB)
        para['device_path'] = "/dev/loop0" 
        para['filesystem'] = "ext4"
        para['ftl'] = "dftldes"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()
```

We implement the experiment as a test for convenience of this tutorial.

`self.conf['workload_class'] = "LinuxDD"` tells WiscSee to use class `LinuxDD`
to run the application. 

You may check `./config_helper/experiment.py` and `config.py` for more options
of experiments.


#### 3. Run

```
./run_testclass.sh tests.test_demo.Test_TraceAndSimulateLinuxDD
```

#### 4. Check Results

WiscSee puts results to `/tmp/results/`. In my case, the results of this
experiment is in
`/tmp/results/test_exp_LinuxDD/subexp--3884625007297461212-ext4-04-10-11-48-16-3552120672700940123`.
In the directory, you will see the following files.

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




