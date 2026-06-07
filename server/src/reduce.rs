use std::{collections::VecDeque, time::Duration};

use thiserror::Error;
use tokio::time::sleep;

use serde::{Deserialize, Serialize};

use crate::{
    cluster::Host,
    query::{QueryRequest, QueryResponse},
    server::{MapReduceServer, context},
    storage::{OutputData, StorageError},
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
    #[error(transparent)]
    StorageError(#[from] StorageError),
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
                QueryRequest::DownloadMapOutput(index, rr.partition.clone()),
            )
            .await;

        let Ok(Ok(QueryResponse::Data(data))) = result else {
            println!("[WARNING]: Downloading {} failed to download data", index);
            download_queue.push_back(index);
            continue;
        };

        server
            .storage
            .append_reduce_in(rr.partition.clone(), data)?;
    }

    // Sort the data and create an iterator over it
    let mut sorted = server
        .storage
        .get_reduce_external_sort_iter(rr.partition.clone())?;

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
                server
                    .storage
                    .append_reduce_out(rr.partition.clone(), output)?;

                acc = Vec::new();
            }
        }
    }

    let output = OutputData(key, acc);
    server
        .storage
        .append_reduce_out(rr.partition.clone(), output)?;

    Ok(())
}
