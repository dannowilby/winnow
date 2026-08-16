use std::future::Future;

use wasmtime::Store;

use crate::wasm::{context::HostAPI, mapper::Mapper};

pub trait MapFn {
    fn map(
        &mut self,
        key: &str,
        value: &[u8],
    ) -> impl Future<Output = Result<Vec<(String, Vec<u8>)>, wasmtime::error::Error>> + Send;
}

pub struct MapHandle {
    pub store: Store<HostAPI>,
    pub mapper: Mapper,
}

impl MapFn for MapHandle {
    async fn map(
        &mut self,
        key: &str,
        value: &[u8],
    ) -> Result<Vec<(String, Vec<u8>)>, wasmtime::error::Error> {
        self.mapper.call_map_fn(&mut self.store, key, value).await
    }
}
