use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    cluster::ClusterConn,
    wasm::{WasmEnv, handle::read::ReadFn},
};

#[derive(Debug, Deserialize, Serialize)]
pub struct MapRequest {
    pub index: usize,

    pub key_range: Vec<String>,
    pub r: u32,

    pub read_src: Vec<u8>,
    pub map_src: Vec<u8>,
    pub partition_src: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MapResponse {
    pub seen_partitions: Vec<String>,
}

pub async fn handle_map<W: WasmEnv>(
    _cluster: Arc<ClusterConn>,
    wasm_env: W,
    mp: MapRequest,
) -> MapResponse {
    // We have to create the environment in the thread that builds and
    // executes the wasm code. wasmtime constructs do not mostly implement `Send`
    use crate::wasm::handle::map::MapFn;
    let mut mapper = wasm_env.load_map_binary(&mp.map_src).unwrap();
    let mut reader = wasm_env.load_read_binary(&mp.read_src).unwrap();

    let mut seen_partitions = Vec::<String>::new();

    for key in mp.key_range {
        let value = reader
            .read(&key)
            .expect("Should be able to read the key...");
        let mut partitions = mapper
            .map(&key, &value)
            .expect("Should be able to map the key/value pair");

        seen_partitions.append(&mut partitions);
    }

    MapResponse { seen_partitions }
}
