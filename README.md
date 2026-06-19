# Winnow
> "to separate grain from chaff"

A Rust implementation of Google's MapReduce with WASM-powered user programs.

## Quick start

## Getting started
1. Build or download the server binary and deploy it on your cluster's members,
   passing the port to serve on as a flag `--port 3000` or specified in the
   config file. It is optional to provide all the cluster members at this point.

2. Build your map reduce components. The components folder is already set up
   with all the necessary dependencies to compile, and running `just
   build-components` will spit out the built binaries for you.

3. Either pass the cluster members to the server in the config, or when
   interacting with the CLI. Specify the input file containing the keys of your
   data, and the various locations of the map reduce components. Specify M and
   R, the number of map and reduce jobs.

4. Hit enter. The CLI will start the job and show you a live view of its progress.

## Status
The system is functional, but is currently undergoing hardening for better fault tolerance. Expect many changes to the CLI!
