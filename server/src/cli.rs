use std::{
    collections::HashMap,
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use mapreduce::{
    cluster::{ClusterList, Host},
    promote::PromoteRequest,
    query::{QueryRequest, QueryResponse},
    server::context,
    storage::OutputData,
    transport::TcpConnector,
};
use serde::Deserialize;
use tarpc::{client::RpcError, tokio_util::sync::CancellationToken};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    RpcError(#[from] RpcError),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

/// The job description read from `job.json`. The `*_src` fields are paths to the
/// compiled WASM programs that the job runs.
#[derive(Deserialize)]
struct JobConfig {
    id: String,
    read_src: String,
    map_src: String,
    reduce_src: String,
    partition_src: String,
    m: u32,
    r: u32,
    leader: Host,
}

/// winnow — a CLI for running map-reduce jobs against the cluster.
#[derive(Parser)]
#[command(name = "winnow")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a map-reduce job on the cluster.
    RunJob,
    /// Download the output for a given partition.
    Download {
        /// The partition whose output should be downloaded.
        partition: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let cli = Cli::parse();
    let config: JobConfig = serde_json::from_slice(&fs::read("./job.json")?)?;

    println!();
    println!("Winnow CLI");

    match cli.command {
        Command::RunJob => start_job(&config).await?,
        Command::Download { partition } => download(&config, &partition).await?,
    }

    Ok(())
}

/// Download and print the output for a single partition.
async fn download(config: &JobConfig, partition: &str) -> Result<(), CliError> {
    println!("Downloading partition: {}", partition);
    // TODO: wire up — query the host holding `partition`, download its reduce
    // output, then hand the bytes to `deserialize_and_print_output`.

    let partitions: HashMap<String, Host> =
        serde_json::from_slice(&fs::read(format!("output-{}.json", config.id))?)?;

    let Some(host) = partitions.get(partition) else {
        println!("Partition not found in output: {}", partition);
        return Ok(());
    };

    let cluster = ClusterList::new(
        vec![(host.domain.clone(), host.port)],
        0,
        Arc::new(TcpConnector),
    )
    .connect()
    .await;

    let QueryResponse::Data(data) = cluster
        .get_loopback()
        .client
        .as_ref()
        .unwrap()
        .query(
            context(),
            QueryRequest::DownloadReduceOutput(partition.to_owned()),
        )
        .await?
        .map_err(CliError::Other)?
    else {
        return Err(CliError::Other("Error querying host.".to_owned()));
    };

    let contents_path = format!("reduce-output-{}", partition);
    fs::write(&contents_path, &data)?;

    println!("Output binary written to {}", contents_path);

    Ok(())
}

/// Temp deserializer for testing
#[allow(unused)]
fn deserialize_and_print_output(r: Vec<u8>) {
    let output_data: OutputData = rmp_serde::from_slice(&r).expect("should have some actual data");
    let o: i32 = rmp_serde::from_slice(&output_data.1).expect("hm");
    println!("{}: {}", output_data.0, o);
}

async fn start_job(config: &JobConfig) -> Result<(), CliError> {
    let cluster = ClusterList::new(
        vec![(config.leader.domain.clone(), config.leader.port)],
        0,
        Arc::new(TcpConnector),
    )
    .connect()
    .await;

    println!("\"{}\"", config.id);

    // One key per line; blank lines are ignored.
    let keys = fs::read_to_string("./keys.txt")?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(String::from)
        .collect::<Vec<String>>();

    let promote_request = PromoteRequest {
        read_src: fs::read(&config.read_src)?,
        map_src: fs::read(&config.map_src)?,
        reduce_src: fs::read(&config.reduce_src)?,
        partition_src: fs::read(&config.partition_src)?,
        m: config.m,
        r: config.r,
        keys,
    };

    let ctx = context();

    let n = Instant::now();

    let token = CancellationToken::new();

    let request_handle = tokio::spawn({
        let token = token.clone();
        let t = cluster.get_loopback().client.clone();
        async move {
            let request = t.as_ref().unwrap().promote(ctx, promote_request).await;
            token.cancel();
            request
        }
    });

    // The overall display: a "primed" status line at the top, followed by a
    // progress bar for the map phase and one for the reduce phase. The
    // `MultiProgress` keeps the three lines rendering together without one
    // clobbering another.
    let multi = MultiProgress::new();

    let status_bar = multi.add(ProgressBar::new_spinner());
    status_bar.set_style(
        ProgressStyle::with_template("{spinner:.yellow} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    status_bar.set_message("Priming cluster…");
    status_bar.enable_steady_tick(Duration::from_millis(100));

    let bar_style = ProgressStyle::with_template(
        "{prefix:>8} [{elapsed_precise}] {bar:40.cyan/blue} {pos:>5}/{len:5} {msg}",
    )
    .unwrap()
    .progress_chars("=>-");

    let map_bar = multi.add(ProgressBar::new(0));
    map_bar.set_style(bar_style.clone());
    map_bar.set_prefix("Map");

    let reduce_bar = multi.add(ProgressBar::new(0));
    reduce_bar.set_style(bar_style);
    reduce_bar.set_prefix("Reduce");

    let progress_handle = tokio::spawn({
        let token = token.clone();
        let t = cluster.get_loopback().client.clone();

        // Pulls the latest progress from the server and reflects it onto the
        // status line and the two progress bars. The bars are cheap (Arc-backed)
        // clones, so the originals stay available for the finishing touches.
        let refresh = {
            let status_bar = status_bar.clone();
            let map_bar = map_bar.clone();
            let reduce_bar = reduce_bar.clone();
            move |progress: &mapreduce::job_lookup::Progress| {
                if progress.primed {
                    status_bar.set_message("Primed");
                }

                if progress.total_map_jobs != 0 {
                    map_bar.set_length(progress.total_map_jobs as u64);
                    map_bar.set_position(progress.completed_map_jobs as u64);
                }

                if progress.total_reduce_jobs != 0 {
                    reduce_bar.set_length(progress.total_reduce_jobs as u64);
                    reduce_bar.set_position(progress.completed_reduce_jobs as u64);
                }
            }
        };

        async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        if let Ok(QueryResponse::Progress(progress)) = t
                            .as_ref()
                            .unwrap()
                            .query(context(), QueryRequest::JobProgress)
                            .await
                            .unwrap()
                        {
                            refresh(&progress);
                        }
                        status_bar.finish_with_message("Primed");
                        map_bar.finish();
                        reduce_bar.finish();
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let QueryResponse::Progress(progress) = t
                            .as_ref()
                            .unwrap()
                            .query(context(), QueryRequest::JobProgress)
                            .await.unwrap()
                            .map_err(CliError::Other).unwrap()
                        else {
                            continue;
                        };

                        refresh(&progress);
                    }
                }
            }
        }
    });

    let (response, _progress_err) = tokio::join!(request_handle, progress_handle);

    println!("Finished in {}ms", n.elapsed().as_millis());

    let data = response.unwrap()?;
    let contents_path = format!("output-{}.json", config.id);
    let contents = serde_json::to_string(&data)?;
    fs::write(&contents_path, contents)?;

    println!("Partition locations written to {}", contents_path);

    Ok(())
}
