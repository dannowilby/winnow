use std::{collections::VecDeque, sync::Arc, time::Duration};

use tarpc::context;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;

use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::{
    cluster::Host,
    query::{QueryRequest, QueryResponse},
    server::{MapReduceServer, context, set_parent},
    storage::{OutputData, StorageError, advance_reduce_sorted},
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

#[tracing::instrument(name = "Reduce", skip_all)]
pub async fn handle_reduce<W: WasmEnv>(
    server: MapReduceServer<W>,
    ctx: context::Context,
    rr: ReduceRequest,
) -> Result<(), ReduceError> {
    set_parent(&tracing::Span::current(), &ctx);

    info!("Running reduce job for {}", rr.partition);

    let partition_lock = server
        .reduce_locks
        .entry(rr.partition.clone())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();
    let _partition_guard = partition_lock.lock().await;

    server.storage.clear_reduce_in(&rr.partition).await?;
    server.storage.clear_reduce_out(&rr.partition).await?;

    let _permit = server
        .wasm_slots
        .acquire()
        .await
        .expect("wasm_slots semaphore should not be closed");

    // if we're going to fail, at least fail before downloading everything
    let programs = server.programs.read().await;
    let mut reducer = server
        .wasm_env
        .load_reduce_binary(&programs.reduce_src)
        .await?;

    let mut download_queue = VecDeque::from(rr.indices);

    let mut total_downloaded_bytes: u64 = 0;
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
            retry += 1;
            server.cluster.write().await.reconnect().await;

            leader = server
                .cluster
                .read()
                .await
                .get_unchecked(rr.leader.clone())
                .clone();

            if leader.client.is_none() {
                sleep(Duration::from_secs(6_u64.pow(retry))).await;
            }
        }

        // being unable to communicate with the leader is unrecoverable
        if leader.client.is_none() {
            return Err(ReduceError::ConnectionError(format!(
                "leader ({:?}) failed to connect",
                leader.host
            )));
        }

        // check the status of the job, we don't want to download incomplete
        // data (important for fault tolerance)
        let Ok(QueryResponse::Status(true)) = leader
            .client
            .as_ref()
            .unwrap()
            .query(context(), QueryRequest::IsMapJobComplete(index))
            .await
            .unwrap()
        else {
            warn!("{} has not completed yet", index);
            download_queue.push_back(index);
            sleep(Duration::from_secs(6_u64.pow(retry))).await;
            continue;
        };

        // Find where to download from
        let index_location_result = leader
            .client
            .as_ref()
            .unwrap()
            .query(context(), QueryRequest::IndexLocation(index))
            .await
            .unwrap();

        let data_host = match index_location_result {
            Ok(QueryResponse::Host(data_host)) => data_host,
            other => {
                warn!("Downloading {} failed to query host: {:?}", index, other);
                download_queue.push_back(index);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
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

        let data = match result {
            Ok(Ok(QueryResponse::Data(data))) => data,
            other => {
                warn!(
                    "Downloading {} from {:?} failed to download data: {:?}",
                    index, target_connection.host, other
                );
                download_queue.push_back(index);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let data_len = data.len() as u64;

        info!(
            "{}-{}, {:?} downloaded data from {:?}, {} bytes",
            rr.partition, index, leader.host, target_connection.host, data_len
        );

        total_downloaded_bytes += data_len;

        server
            .storage
            .append_reduce_in(rr.partition.clone(), data)
            .await?;

        let actual_len_after_append = server.storage.reduce_in_len(&rr.partition).await?;
        if actual_len_after_append != total_downloaded_bytes {
            error!(
                partition = %rr.partition,
                index,
                this_append_bytes = data_len,
                running_total = total_downloaded_bytes,
                actual_file_bytes = actual_len_after_append,
                "sor-* diverged from expected size right after appending this index"
            );
        }
    }

    server.storage.sync_reduce_in(&rr.partition).await?;

    let actual_len = server.storage.reduce_in_len(&rr.partition).await?;
    if actual_len != total_downloaded_bytes {
        error!(
            partition = %rr.partition,
            downloaded_bytes = total_downloaded_bytes,
            actual_file_bytes = actual_len,
            "sor-* size mismatch: appended byte count doesn't match what's on disk"
        );
    } else {
        info!(
            partition = %rr.partition,
            bytes = actual_len,
            "sor-* size matches downloaded bytes"
        );
    }

    // Sort the data and create an iterator over it
    let mut sorted = server
        .storage
        .get_reduce_external_sort_iter(rr.partition.clone())
        .await?;

    // iterate over sorted kv pairs and fold with the reducer
    let mut acc = Vec::<u8>::new();

    let mut key: String = "".to_owned();
    loop {
        let (next_sorted, item, peek_key_differs) = advance_reduce_sorted(sorted).await;
        sorted = next_sorted;

        let Some(Ok(item)) = item else {
            break;
        };

        key = item.key.clone();
        acc = reducer.reduce(&item.key, &item.value, &acc).await?;

        if peek_key_differs {
            let output = OutputData(item.key, acc);
            server
                .storage
                .append_reduce_out(rr.partition.clone(), output)
                .await?;

            acc = Vec::new();
        }
    }

    let output = OutputData(key, acc);
    server
        .storage
        .append_reduce_out(rr.partition.clone(), output)
        .await?;

    server.storage.sync_reduce_out(&rr.partition).await?;

    Ok(())
}
