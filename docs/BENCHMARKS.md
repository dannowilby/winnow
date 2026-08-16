
# Hadoop Streaming vs Winnow Benchmarks

The benchmarks are run on AWS and provisioned with terraform. Both tests are run with the following
specs:
- 172 map jobs
- 20 reduce jobs
- 6 x t3.large + 45GiB root block storage per instance

**Why word frequency?** While the mapper is light, it produces large intermediate files, which are then
processed by heavier reducers. This maximizes the effects of data locality, the
main difference between Hadoop Streaming and Winnow.

**The data used in this benchmark.** The input data is ~100,000 text files downloaded from Project Gutenberg's
mirror, totaling 23.6 Gb. This data has been merged into a singular file, so
that MapReduce doesn't thrash jobs. Without this pre-run optimization just the
map stage alone would have taken approximately ~126 hours to complete.

**Future benchmarks for these systems.** Due to a limited amount of resources, I
was only able to test the "straight-shot" workload of Winnow and Hadoop
Streaming. In the future, benchmarks regarding failure recovery would shed light
on how each failure model compares.

## Hadoop

Hadoop version 3.5.0

1 namenode
5 datanodes

load_data.sh (insertion into hadoop): 211s after data consolidation
run_test.sh (running hadoop-streaming.jar): 2452s = 40m52s 
verify_data.sh: matches baseline

## Winnow

Winnow version 1.0.0

6 instances

load_data.sh (generating key file): 0s
run_test.sh (running winnow): 3114 = 51m54s
verify_data.sh: matches baseline
