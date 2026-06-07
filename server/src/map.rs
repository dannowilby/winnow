use std::{collections::HashSet, fs::OpenOptions, io::Write};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    query::IntermediateData,
    server::MapReduceServer,
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
}

pub async fn handle_map<W: WasmEnv>(
    server: MapReduceServer<W>,
    mp: MapRequest,
) -> Result<MapResponse, MapError> {
    std::fs::create_dir_all("./data")?;

    // We have to create the environment in the thread that builds and
    // executes the wasm code. wasmtime constructs do not mostly implement `Send`
    let programs = server.programs.read().await;

    let mut mapper = server.wasm_env.load_map_binary(&programs.map_src)?;
    let mut reader = server.wasm_env.load_read_binary(&programs.read_src)?;
    let mut partitioner = server
        .wasm_env
        .load_partition_binary(&programs.partition_src)?;

    let mut seen_partitions = HashSet::<String>::new();

    for key in mp.key_range {
        let value = reader.read(&key)?;
        let kvs = mapper.map(&key, &value)?;

        for (out_key, value) in kvs {
            let partition = partitioner.partition(&out_key, mp.r)?;

            save_data(
                mp.index,
                &partition,
                IntermediateData {
                    key: out_key,
                    value,
                },
            )?;

            seen_partitions.insert(partition);
        }
    }

    Ok(MapResponse {
        index: mp.index,
        seen_partitions: seen_partitions.into_iter().collect::<Vec<String>>(),
    })
}

fn save_data(
    index: usize,
    partition: &str,
    intermediate_data: IntermediateData,
) -> Result<(), MapError> {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(format!("data/{}on{}", partition, index))?;

    let b = rmp_serde::to_vec(&intermediate_data)?;

    file.write(&b)?;

    Ok(())
}

#[cfg(test)]
mod tests {

    #[test]
    fn deduplicates_partitions() {}

    #[test]
    fn generates_correct_response() {}

    #[test]
    fn writes_correct_data() {}
}
