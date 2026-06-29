use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tarpc::context;
use thiserror::Error;
use tracing::info_span;

use crate::{
    server::{MapReduceServer, set_parent},
    storage::{IntermediateData, StorageError},
    wasm::{
        WasmEnv,
        handle::{map::MapFn, partition::PartitionFn, read::ReadFn},
    },
};

#[derive(Debug, Deserialize, Serialize)]
pub struct MapRequest {
    pub index: usize,

    pub key_range: Vec<String>,
    pub r: u32,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MapResponse {
    pub index: usize,
    pub seen_partitions: Vec<String>,
}

#[derive(Error, Debug)]
pub enum MapError {
    #[error(transparent)]
    FileError(#[from] std::io::Error),
    #[error(transparent)]
    EncodeError(#[from] rmp_serde::encode::Error),
    #[error(transparent)]
    WasmError(#[from] wasmtime::error::Error),
    #[error(transparent)]
    StorageError(#[from] StorageError),
}

pub async fn handle_map<W: WasmEnv>(
    server: MapReduceServer<W>,
    ctx: context::Context,
    mp: MapRequest,
) -> Result<MapResponse, MapError> {
    let span = info_span!("Map");
    set_parent(&span, &ctx);
    tracing::info!(trace = %ctx.trace_id(), "received map");

    // We have to create the environment in the thread that builds and
    // executes the wasm code. wasmtime constructs do not mostly implement `Send`
    let programs = server.programs.read().await;

    let mut mapper = server.wasm_env.load_map_binary(&programs.map_src).await?;
    let mut reader = server.wasm_env.load_read_binary(&programs.read_src).await?;
    let mut partitioner = server
        .wasm_env
        .load_partition_binary(&programs.partition_src)
        .await?;

    let mut seen_partitions = HashSet::<String>::new();

    for key in mp.key_range {
        let value = reader.read(&key).await?;
        let kvs = mapper.map(&key, &value).await?;

        for (out_key, value) in kvs {
            let partition = partitioner.partition(&out_key, mp.r).await?;

            server.storage.append_map_out(
                mp.index,
                partition.clone(),
                IntermediateData {
                    key: out_key,
                    value,
                },
            )?;

            seen_partitions.insert(partition);
        }
    }

    let seen_partitions = seen_partitions.into_iter().collect::<Vec<String>>();

    Ok(MapResponse {
        index: mp.index,
        seen_partitions,
    })
}
