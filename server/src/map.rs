use std::{fs::OpenOptions, io::Write, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    cluster::ClusterConn, download::IntermediateData, wasm::{WasmEnv, handle::{partition::{self, PartitionFn}, read::ReadFn}}
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
    wasm_env: W,
    mp: MapRequest,
) -> MapResponse {

    if !std::fs::exists("./data").expect("should be able to check the directory") {
        std::fs::create_dir("./data").expect("should be able to create dir");
    }

    // We have to create the environment in the thread that builds and
    // executes the wasm code. wasmtime constructs do not mostly implement `Send`
    use crate::wasm::handle::map::MapFn;
    let mut mapper = wasm_env.load_map_binary(&mp.map_src).unwrap();
    let mut reader = wasm_env.load_read_binary(&mp.read_src).unwrap();
    let mut partitioner = wasm_env.load_partition_binary(&mp.partition_src).unwrap();

    let mut seen_partitions = Vec::<String>::new();

    for key in mp.key_range {
        let value = reader
            .read(&key)
            .expect("Should be able to read the key...");
        let mut kvs = mapper
            .map(&key, &value)
            .expect("Should be able to map the key/value pair");

        for (out_key, value) in kvs {
            let partition = partitioner.partition(&out_key, mp.r).expect("should be able to partition");

            save_data(&partition, IntermediateData { key: out_key, value });

            seen_partitions.push(partition);
        }

    }

    println!("Partitions being sent back from the mapper: {:?}", seen_partitions);

    MapResponse { seen_partitions }
}

fn save_data(partition: &str, intermediate_data: IntermediateData) {
    let mut file = OpenOptions::new().append(true).create(true).open(format!("data/{}", partition)).expect("could not create partition file");

    let b = rmp_serde::to_vec(&intermediate_data).expect("could not serialize output");

    let _ = file.write(&b);
}
