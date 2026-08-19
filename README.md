
<div align="center">
   <img width="454" height="98" alt="winnow-cli" src="https://github.com/user-attachments/assets/63a8be23-eff3-4cbe-8554-b6a2c38941bb" />
   <div id="user-content-toc">
      <ul align="center" style="list-style: none;">
       <summary>
         <h1>Winnow</h1>
       </summary>
      </ul>
   </div>
   <p align="center">MapReduce jobs as WASM components</p>
   <div align="center">
   <img src="https://codecov.io/github/dannowilby/winnow/graph/badge.svg" />
   <img src="https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square" />
   </div>
</div>

---

This was built as a successor to [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html). Hadoop Streaming is an implementation of MapReduce with where the user programs are executables called on the task-running machine, and the data is passed in through standard input. This can work well, but consider the following scenario:
> You have a 30-line Python map function: 500 nodes each need version-matched Python plus every imported library, the tab-delimited stdin/stdout breaks the first time a value contains a tab or a non-UTF-8 byte, and the per-task subprocess forks

Winnow solves all three issues by using WASM components to define the MapReduce job. Write your program once with a typed interface and run on all your machines with no extra environment configurations.

| | Hadoop Streaming | This engine |
| --- | --- | --- |
| Deploying a job| interpreter/binary installed + version-matched per node | one .wasm, compiled once, runs anywhere |
| How data crosses | tab-delimited text over stdin/stdout | typed values across a declared interface |
| Per task | fork a subprocess | in-process call |
| Cross-arch/OS| native code rebuilt per target | portable artifact, no rebuild |

## Quick start

1. Clone this repo
```bash
git clone https://github.com/dannowilby/winnow.git
cd winnow
```

2. Start a local cluster
```bash
just run-local-cluster
```
This will run three of winnow's MapReduce nodes along with an instance for Grafana, Loki, Tempo, and Prometheus. 
> In addition to starting the instances, the command will also build release versions of the example user program contained in `components/`. If you want to rebuild them at any point, recompile them with `just build-components`.

3. Define a job
```bash
cp example.job.json job.json
cp example.keys.txt keys.txt
```
More information is contained in the [AUTHORING_COMPONENTS](./docs/AUTHORING_COMPONENTS.md) guide.

4. Use the CLI to run the job
```
just cli run-job
```
Once the job finishes, a json file with the job's name will be written containing information about where each final partition's data is. 

5. (Optional) Download the reduce output
```
just cli download <partition name>
```

## Installation

The `winnow` binary is a deployable artifact that will automatically run a singular Winnow server. The binary requires a `cluster.json` file to run, supplying telemetry configuration and cluster membership. An example has been provided in `example.cluster.json`.

## Features

**Fault-tolerance**

A heartbeat mechanism is used to detect failures. When a leader detects a machine has failed, the map job for the index split is rescheduled on a random, live machine. The reduce jobs are similarly rescheduled, using an exponential backoff to wait until the new map job has finished producing data.

**Observability**

OpenTelemetry metrics, traces, and logs are configured. A trace can be followed through from the start of a job with the CLI, to each individual data query in the reduce stage.

**Async support (non-WASM)**

Unfortunately, WASM-level async (asynchronous calls within a WASM component) proved too immature to successfully work with Python's WASM component compiler. As such, multiple requests are handled asynchronously, but not the UDFs themselves.

**Live status updates**

Like Hadoop Streaming, Winnow provides a progress bar of the map and reduce phases of the program's completion.


## Benchmarks

Benchmarks have been performed and discussed in the [BENCHMARKS](./docs/BENCHMARKS.md) document. The benchmarks focusing on comparing the wall-clock times of Hadoop Streaming and Winnow.

## Limitations

WASM is often cited as a strong use case for secure environments. The WASM components are run in a non-secure setting here: they can make network requests, allocate memory, and write to the file system. This assumes that you trust the jobs you are running, and only come from within your own organization.

## AI usage

Claude Code was used to create and debug tests, create the local cluster docker compose configuration, refine the machine images used for benchmarking, and fix small bugs that only surfaced in benchmarks.
