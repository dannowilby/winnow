mod context;
pub mod handle;

use std::future::Future;

use crate::wasm::{
    context::{ExtensionData, HostAPI},
    handle::{
        map::{MapFn, MapHandle},
        partition::{PartitionFn, PartitionHandle},
        read::{ReadFn, ReadHandle},
        reduce::{ReduceFn, ReduceHandle},
    },
};
use wasmtime::{
    Config, Engine, Store,
    component::{Component, Linker},
};
use wasmtime_wasi::{DirPerms, FilePerms, ResourceTable, WasiCtxBuilder};

mod mapper {
    wasmtime::component::bindgen!({
        world: "mapper",
        path: "../wit/world.wit",
        anyhow: true,
        exports: { default: async | ignore_wit },
        require_store_data_send: true,
    });
}

mod reducer {
    wasmtime::component::bindgen!({
        world: "reducer",
        path: "../wit/world.wit",
        anyhow: true,
        exports: { default: async | ignore_wit },
        require_store_data_send: true,
    });
}

mod partitioner {
    wasmtime::component::bindgen!({
        world: "partitioner",
        path: "../wit/world.wit",
        anyhow: true,
        exports: { default: async | ignore_wit },
        require_store_data_send: true,
    });
}

mod reader {
    wasmtime::component::bindgen!({
        world: "reader",
        path: "../wit/world.wit",
        anyhow: true,
        exports: { default: async | ignore_wit },
        require_store_data_send: true,
    });
}

use mapper::Mapper;
use partitioner::Partitioner;
use reader::Reader;
use reducer::Reducer;

pub trait WasmEnv: Clone + Send + Sync + 'static {
    fn new() -> Result<Self, Box<dyn std::error::Error>>;

    fn load_partition_binary(
        &self,
        binary: &[u8],
    ) -> impl Future<Output = Result<impl PartitionFn, wasmtime::error::Error>> + Send;
    fn load_map_binary(
        &self,
        binary: &[u8],
    ) -> impl Future<Output = Result<impl MapFn, wasmtime::error::Error>> + Send;
    fn load_reduce_binary(
        &self,
        binary: &[u8],
    ) -> impl Future<Output = Result<impl ReduceFn, wasmtime::error::Error>> + Send;
    fn load_read_binary(
        &self,
        binary: &[u8],
    ) -> impl Future<Output = Result<impl ReadFn, wasmtime::error::Error>> + Send;
}

#[derive(Clone)]
pub struct DefaultWasmEnv {
    engine: Engine,
}

impl WasmEnv for DefaultWasmEnv {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config::new();
        let engine = Engine::new(&config)?;

        Ok(Self { engine })
    }

    async fn load_map_binary(&self, binary: &[u8]) -> Result<impl MapFn, wasmtime::error::Error> {
        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let mapper = Mapper::instantiate_async(&mut store, &component, &linker).await?;

        Ok(MapHandle { store, mapper })
    }

    async fn load_reduce_binary(
        &self,
        binary: &[u8],
    ) -> Result<impl ReduceFn, wasmtime::error::Error> {
        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let reducer = Reducer::instantiate_async(&mut store, &component, &linker).await?;

        Ok(ReduceHandle { store, reducer })
    }

    async fn load_partition_binary(
        &self,
        binary: &[u8],
    ) -> Result<impl PartitionFn, wasmtime::error::Error> {
        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let partitioner = Partitioner::instantiate_async(&mut store, &component, &linker).await?;

        Ok(PartitionHandle { store, partitioner })
    }

    async fn load_read_binary(&self, binary: &[u8]) -> Result<impl ReadFn, wasmtime::error::Error> {
        let (mut store, component, linker) = pre_instantiate_component(&self.engine, binary)?;

        let reader = Reader::instantiate_async(&mut store, &component, &linker).await?;

        Ok(ReadHandle { store, reader })
    }
}

/// Create the store, component, and linker in order to instantiate the
/// component. This code does not vary between different components.
fn pre_instantiate_component(
    engine: &Engine,
    binary: &[u8],
) -> Result<(Store<HostAPI>, Component, Linker<HostAPI>), wasmtime::error::Error> {
    let component = Component::new(engine, binary)?;

    let mut linker: Linker<HostAPI> = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    // Currently we just use the mapper's defined imports to link all of the
    // separate binaries. This works because the mapper's imports contain all of
    // the imports of the other worlds.
    //
    // In the future, if we can't get a world with a set of all the potential
    // imports, then this will have to be done individually for the different
    // binaries/components. For now, this is good enough.
    Mapper::add_to_linker::<HostAPI, ExtensionData>(&mut linker, |state: &mut HostAPI| state)?;

    // The preopen below requires this directory to already exist on disk;
    // nothing else in the startup path is guaranteed to have created it (e.g.
    // handlers invoked directly, bypassing `prime`/`Storage::reset`).
    std::fs::create_dir_all("sources")?;

    let wasi_ctx = WasiCtxBuilder::new()
        .inherit_stdio()
        .inherit_env()
        .inherit_stderr()
        .preopened_dir("sources", ".", DirPerms::READ, FilePerms::READ)?
        .build();

    let store = Store::new(
        engine,
        HostAPI {
            wasi_ctx,
            resource_table: ResourceTable::new(),
        },
    );

    Ok((store, component, linker))
}
