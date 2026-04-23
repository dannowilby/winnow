mod context;
pub mod handles;

use crate::wasm::{context::{ExtensionData, HostAPI}, handles::{Map, MapHandle, Partition, PartitionHandle, Read, ReadHandle, Reduce, ReduceHandle}};
use wasmtime_wasi::{ResourceTable, WasiCtxBuilder};
use wasmtime::{Config, Engine, Store, component::{Component, Linker}};

mod mapper {
    wasmtime::component::bindgen!({
        world: "mapper",
        path: "../wit/world.wit",
        anyhow: true
    });
}

mod reducer {
    wasmtime::component::bindgen!({
        world: "reducer",
        path: "../wit/world.wit",
        anyhow: true
    });
}

mod partitioner {
    wasmtime::component::bindgen!({
        world: "partitioner",
        path: "../wit/world.wit",
        anyhow: true
    });
}

mod reader {
    wasmtime::component::bindgen!({
        world: "reader",
        path: "../wit/world.wit",
        anyhow: true
    });
}

use mapper::Mapper;
use reducer::Reducer;
use partitioner::Partitioner;
use reader::Reader;


pub trait WasmEnv : Clone {
    fn new() -> Result<Self, Box<dyn std::error::Error>>;

    fn load_partition_binary(&mut self, binary: &[u8]) -> Result<impl Partition, wasmtime::error::Error>;
    fn load_map_binary(&mut self, binary: &[u8]) -> Result<impl Map, wasmtime::error::Error>;
    fn load_reduce_binary(&mut self, binary: &[u8]) -> Result<impl Reduce, wasmtime::error::Error>;
    fn load_read_binary(&mut self, binary: &[u8]) -> Result<impl Read, wasmtime::error::Error>;
}

#[derive(Clone)]
pub struct DefaultWasmEnv {
    engine: Engine,
}

impl WasmEnv for DefaultWasmEnv {

    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let engine = Engine::new(&Config::default())?;
        
        Ok(Self {
            engine
        })
    }

    fn load_map_binary(&mut self, binary: &[u8]) -> Result<impl Map, wasmtime::error::Error> {

        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let mapper = Mapper::instantiate(&mut store, &component, &linker)?;

        Ok(MapHandle {
            store,
            mapper
        })
    }

    fn load_reduce_binary(&mut self, binary: &[u8]) -> Result<impl Reduce, wasmtime::error::Error> {
        
        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let reducer = Reducer::instantiate(&mut store, &component, &linker)?;

        Ok(ReduceHandle {
            store,
            reducer
        })
    }
    
    fn load_partition_binary(&mut self, binary: &[u8]) -> Result<impl Partition, wasmtime::error::Error> {
        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let partitioner = Partitioner::instantiate(&mut store, &component, &linker)?;

        Ok(PartitionHandle {
            store,
            partitioner
        })
    }
    
    fn load_read_binary(&mut self, binary: &[u8]) -> Result<impl Read, wasmtime::error::Error> {
        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let reader = Reader::instantiate(&mut store, &component, &linker)?;

        Ok(ReadHandle {
            store,
            reader
        })
    }
}

/// Create the store, component, and linker in order to instantiate the
/// component. This code does not vary between different components.
fn pre_instantiate_component(engine: &Engine, binary: &[u8]) -> Result<(Store<HostAPI>, Component, Linker<HostAPI>), wasmtime::error::Error> {
    let component = Component::new(&engine, binary)?;

    let mut linker: Linker<HostAPI> = Linker::new(&engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
    mapper::mapreduce::typeimpls::logging::add_to_linker::<HostAPI, ExtensionData>(
        &mut linker,
        |state: &mut HostAPI| state,
    )?;

    let wasi_ctx = WasiCtxBuilder::new()
    .inherit_stdio()
    .inherit_env()
    .inherit_stderr()
    .build();

    let store = Store::new(&engine, HostAPI {
        wasi_ctx,
        resource_table: ResourceTable::new()
    });

    Ok((store, component, linker))
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;

    use postcard::to_allocvec;
    use serde::{Deserialize, Serialize};

    use crate::wasm::handles::Map;

    #[derive(Deserialize, Serialize)]
    struct Temp(u32);

    #[test]
    fn run_wasm_file() -> Result<(), Box<dyn std::error::Error>> {
        let mut wasm_env = DefaultWasmEnv::new()?;

        let reader_bytes = std::fs::read("../target/wasm32-wasip2/release/read.wasm").unwrap();
        println!("read.wasm size: {}", reader_bytes.len());

        let mapper_bytes = std::fs::read("../target/wasm32-wasip2/release/map.wasm").unwrap();
        println!("map.wasm size: {}", mapper_bytes.len());

        let key = "test-key";
        
        let mut value: Vec<u8>;
        {
            let mut reader = wasm_env.load_read_binary(&reader_bytes)?;
            value = reader.read(key)?;
        }

        let mut mapper = wasm_env.load_map_binary(&mapper_bytes)?;
        mapper.map(key, &value)?;

        Ok(())
    }
}