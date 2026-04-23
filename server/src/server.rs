use std::rc::Rc;

use tarpc::context;
use wasmtime::{Engine, Store};

use crate::{
    cluster::ClusterConfig,
    map::{MapRequest, perform_map}, wasm::{DefaultWasmEnv, WasmEnv, handles::Map},
};

#[tarpc::service]
pub trait MapReduceService {
    /// Returns true if healthy, false otherwise.
    async fn heartbeat() -> bool;

    async fn map(mp: MapRequest) -> ();

    async fn reduce() -> ();

    async fn promote() -> ();

    /// `notify` provides an endpoints for finishing map/reduce tasks to send their parsed data to
    async fn notify() -> ();
}

#[derive(Clone)]
pub struct MapReduceServer<W: WasmEnv> {
    cluster: ClusterConfig,
    wasm_env: W
}

impl<W: WasmEnv> MapReduceServer<W> {
    pub fn new() -> Self {
        let wasm_env = W::new().unwrap();
        Self {
            cluster: ClusterConfig {
                instances: Vec::new(),
            },
            wasm_env
        }
    }
}

impl<W: WasmEnv> MapReduceService for MapReduceServer<W> {
    async fn heartbeat(self, _: context::Context) -> bool {
        // If we can still accept heartbeat requests, that means we're healthy
        true
    }

    async fn map(mut self, _: context::Context, mp: MapRequest) -> () {
        
        //let _ = perform_map(mp);
        let map_binary = mp.map_src.bytes().collect::<Vec<u8>>();

        // We have to create the environment in the thread that builds and
        // executes the wasm code. wasmtime constructs do not mostly implement `Send`

        let mut mapper = self.wasm_env.load_map_binary(map_binary.as_slice()).unwrap();
        let r = mapper.map("key 1", &[0x00]);
        match r {
            Ok(_) => { println!("Ran successfully"); }
            Err(e) => { println!("Error encountered: {}", e); }
        }
    }

    async fn reduce(self, _: context::Context) -> () {

        // get the locations of all the data
        // get the reduce function
        // get a sort function

        // download the data
        // sort the data

        // iterate over the data with kv pairs and calculate the final result

        // write the final result
        // send a message to master indicating that the reduce task has finished
    }

    async fn promote(self, _: context::Context) -> () {}

    async fn notify(self, _: context::Context) -> () {}
}
