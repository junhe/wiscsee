WiscSee is an I/O workload analyzer, which helps you to understand your workload
performance on SSDs. WiscSee comes with a fully functioning trace-driven SSD simulator,
WiscSim, which supports enhanced versions of multiple well-known FTLs, NCQ, multiple
channels, garbage collections, wear-leveling, page allocation policies and more.
WiscSim is implemented as a Discrete-Event Simulator.

WiscSee runs your workload, collects its block trace, and later feeds the trace
to WiscSim.

WiscSee contains several demos to help you get started quickly. The demos show

- How to only trace your workload
- How trace a workload and simulate the workload on an SSD
- How simulate synthetic workloads on an SSD
- How to evaluate whether your workloads conform/violate the rules of the
  unwritten contract of SSDs (see "The Unwritten Contract of Solid State Drives"
  in EuroSys'17)

# Download and Setup

## Option 1: VM Image

## Option 2: Docker Image

## Option 3: Git clone

1. Setup

```
make setup
```

2. Run tests

```
make test_all
```



