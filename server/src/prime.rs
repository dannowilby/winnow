use serde::{Deserialize, Serialize};

use crate::{server::MapReduceServer, wasm::WasmEnv};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PrimeRequest {
    pub read_src: Vec<u8>,
    pub map_src: Vec<u8>,
    pub reduce_src: Vec<u8>,
    pub partition_src: Vec<u8>,
}

/// The wasm programs primed via the [prime](crate::prime::handle_prime)
/// endpoint, stored globally on the server for use by the map and reduce
/// endpoints.
#[derive(Clone, Debug, Default)]
pub struct Programs {
    pub read_src: Vec<u8>,
    pub map_src: Vec<u8>,
    pub reduce_src: Vec<u8>,
    pub partition_src: Vec<u8>,
}

/// Resets the machine for a fresh map-reduce run: wipes the data folder, clears
/// the global job-lookup state, and stores the supplied programs globally so the
/// map and reduce endpoints can use them.
pub async fn handle_prime<W: WasmEnv>(
    server: MapReduceServer<W>,
    pr: PrimeRequest,
) -> Result<(), std::io::Error> {
    // wipe any stale intermediate/output data, then recreate the folder so it
    // exists even on nodes that get a reduce job without ever running a map job
    if std::fs::exists("./data")? {
        std::fs::remove_dir_all("./data")?;
    }
    std::fs::create_dir_all("./data")?;

    // clear the global job-lookup state
    server.job_lookup.write().await.clear();

    // store the programs globally for the map and reduce endpoints
    let mut programs = server.programs.write().await;
    programs.read_src = pr.read_src;
    programs.map_src = pr.map_src;
    programs.reduce_src = pr.reduce_src;
    programs.partition_src = pr.partition_src;

    Ok(())
}
