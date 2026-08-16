
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