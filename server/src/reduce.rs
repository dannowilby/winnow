use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{self, BufReader, Read, Write},
    iter::Peekable,
    marker::PhantomData,
    time::Duration,
};

use thiserror::Error;
use tokio::time::sleep;

use ext_sort::{
    BinaryHeapMerger, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder, RmpExternalChunk,
};
use serde::{Deserialize, Serialize};

use crate::{
    cluster::Host,
    query::{IntermediateData, OutputData, QueryRequest, QueryResponse},
    server::{MapReduceServer, context},
    wasm::{WasmEnv, handle::reduce::ReduceFn},
};

#[derive(Debug, Deserialize, Serialize)]
pub struct ReduceRequest {
    pub partition: String,
    pub indices: Vec<usize>,

    pub leader: Host,
}

#[derive(Error, Debug)]
pub enum ReduceError {
    #[error("{0}")]
    ConnectionError(String),
    #[error(transparent)]
    FileError(#[from] std::io::Error),
    #[error(transparent)]
    EncodeError(#[from] rmp_serde::encode::Error),
    #[error(transparent)]
    WasmError(#[from] wasmtime::error::Error),
}

pub async fn handle_reduce<W: WasmEnv>(
    server: MapReduceServer<W>,
    rr: ReduceRequest,
) -> Result<(), ReduceError> {
    println!("Running reduce job for {}", &rr.partition);

    // if we're going to fail, at least fail before downloading everything
    let programs = server.programs.read().await;
    let mut reducer = server
        .wasm_env
        .load_reduce_binary(&programs.reduce_src)
        .await?;

    let file_path = format!("data/{}-sorted", &rr.partition);

    let mut download_queue = VecDeque::from(rr.indices);
    let mut leader = server
        .cluster
        .read()
        .await
        .get_unchecked(rr.leader.clone())
        .clone();

    while let Some(index) = download_queue.pop_front() {
        // we need a functioning leader
        let mut retry = 0;
        let max_retries = 3;
        while let None = leader.client
            && retry < max_retries
        {
            retry = retry + 1;
            server.cluster.write().await.reconnect().await;

            leader = server
                .cluster
                .read()
                .await
                .get_unchecked(rr.leader.clone())
                .clone();

            sleep(Duration::from_secs(2_u64.pow(retry))).await;
        }

        // being unable to communicate with the leader is unrecoverable
        if leader.client.is_none() {
            return Err(ReduceError::ConnectionError(format!(
                "leader ({:?}) failed to connect",
                leader.host
            )));
        }

        let Ok(QueryResponse::Host(data_host)) = leader
            .client
            .as_ref()
            .unwrap()
            .query(context(), QueryRequest::IndexLocation(index))
            .await
            .unwrap()
        else {
            println!("[WARNING] Downloading {} failed to query host", index);
            download_queue.push_back(index);
            continue;
        };

        // get and verify the connection, requeue the index if it looks like
        // things have failed
        let mut target_connection = server
            .cluster
            .read()
            .await
            .get_unchecked(data_host.clone())
            .clone();
        if target_connection.client.is_none() {
            server.cluster.write().await.reconnect().await;
            target_connection = server.cluster.read().await.get_unchecked(data_host).clone();

            if target_connection.client.is_none() {
                download_queue.push_back(index);
                continue;
            }
        }

        let result = target_connection
            .client
            .as_ref()
            .unwrap()
            .query(
                context(),
                QueryRequest::Download(format!("data/{}on{}", &rr.partition, index)),
            )
            .await;

        let Ok(Ok(QueryResponse::Data(data))) = result else {
            println!("[WARNING]: Downloading {} failed to download data", index);
            download_queue.push_back(index);
            continue;
        };

        // write the data if successful (if not successful, requeue the index)
        let mut f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)?;
        f.write(&data)?;
    }

    // Sort the data and create an iterator over it
    let mut sorted = external_sort(&file_path)?;

    // create our final output file
    let output_file_path = format!("data/{}-output", rr.partition);
    let mut output_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(output_file_path)?;

    // iterate over sorted kv pairs and fold with the reducer
    let mut acc = Vec::<u8>::new();

    let mut key: String = "".to_owned();
    while let Some(Ok(item)) = sorted.next() {
        // println!("item: {}", rmp_serde::from_slice::<i32>(&item.value).expect("l"));
        key = item.key.clone();
        acc = reducer.reduce(&item.key, &item.value, &acc).await?;

        // println!("[Reducer] acc is now: {}", rmp_serde::from_slice::<i32>(&acc).expect("j"));

        if let Some(Ok(next_item)) = sorted.peek() {
            if next_item.key != item.key {
                let output = OutputData(item.key, acc);
                let b = rmp_serde::to_vec(&output)?;
                output_file.write(&b)?;

                acc = Vec::new();
            }
        }
    }

    let output = OutputData(key, acc);
    let b = rmp_serde::to_vec(&output)?;
    output_file.write(&b)?;

    Ok(())
}

fn external_sort(
    file_path: &str,
) -> Result<
    Peekable<
        BinaryHeapMerger<
            IntermediateData,
            rmp_serde::decode::Error,
            fn(&IntermediateData, &IntermediateData) -> std::cmp::Ordering,
            RmpExternalChunk<IntermediateData>,
        >,
    >,
    std::io::Error,
> {
    let sort_file = fs::File::open(&file_path)?;
    let file_size = sort_file.metadata()?.len();
    let reader = BufReader::new(sort_file).take(file_size);
    let iter: RmpIter<IntermediateData> = RmpIter {
        reader,
        _marker: PhantomData,
    };

    let sorter: ExternalSorter<IntermediateData, rmp_serde::decode::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(std::path::Path::new("./data"))
            .with_buffer(LimitedBufferBuilder::new(100_000, false))
            .build()
            .expect("Could not build sorter");

    Ok(sorter
        .sort_by(
            iter,
            cmp_intermediate as fn(&IntermediateData, &IntermediateData) -> std::cmp::Ordering,
        )
        .expect("Could not sort data")
        .peekable())
}

struct RmpIter<T> {
    reader: io::Take<BufReader<fs::File>>,
    _marker: PhantomData<T>,
}

impl<T: serde::de::DeserializeOwned> Iterator for RmpIter<T> {
    type Item = Result<T, rmp_serde::decode::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.limit() == 0 {
            return None;
        }
        Some(rmp_serde::decode::from_read(&mut self.reader))
    }
}

fn cmp_intermediate(a: &IntermediateData, b: &IntermediateData) -> std::cmp::Ordering {
    a.key.cmp(&b.key)
}
