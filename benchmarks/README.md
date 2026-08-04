
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
that MapReduce doesn't thrash jobs. Without this pre-run optimization just the map stage will take approximately ~126 hours to complete.

## Hadoop

Hadoop version 3.5.0

1 namenode
5 datanodes

load_data.sh (insertion into hadoop): 1228s, 211s after data consolidation
run_test.sh (running hadoop-streaming.jar): 2452s = 40m52s 
verify_data.sh: matches baseline

## Winnow

Coming soon.

---

## Running the benchmarks

### Running the baseline

The baseline is the workload completed on a single machine using bare metal
execution. Change the `data_src` in `main.py` to point at your data. Then run
`run_baseline.sh`. This will create a file called `baseline.txt` with the
results. It will create a separate file `benchmark_run_test.sh` that contains
the run time of the program in seconds.

### Running the Hadoop Streaming benchmark

1. Create a temporary ssh-key: `ssh-keygen -t ed25519 -f /tmp/eic_key -N ""`

2. Build the AMI using `packer build .` in the `/hadoop-image/` directory. This
will push a new AMI to your AWS account, which will be used in provisioning.

3. Provision the AWS resources with `terraform apply` in the `/infra/` folder.
After `terraform apply` complete, it will output the control plane's id and
connect endpoint's id.

4. Use the control plane's id and connect endpoint's id to connect to the
   machine by running `connect-ec2.sh`.

Now that you are in the machine, you should download the data you want to
benchmark on, then run the following scripts: `load_data.sh`, `run_test.sh`, and
`verify_results.sh`.

### Running the Winnow benchmark

Coming soon.