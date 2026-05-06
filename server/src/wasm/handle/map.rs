use wasmtime::Store;

use crate::wasm::{context::HostAPI, mapper::Mapper};

pub trait MapFn {
    fn map(&mut self, key: &str, value: &[u8]) -> Result<Vec<String>, wasmtime::error::Error>;
}

pub struct MapHandle {
    pub store: Store<HostAPI>,
    pub mapper: Mapper,
}

impl MapFn for MapHandle {
    fn map(&mut self, key: &str, value: &[u8]) -> Result<Vec<String>, wasmtime::error::Error> {
        self.mapper.call_map_fn(&mut self.store, key, value)
    }
}
