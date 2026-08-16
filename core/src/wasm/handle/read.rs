use std::future::Future;

use wasmtime::Store;

use crate::wasm::{context::HostAPI, reader::Reader};

pub trait ReadFn {
    fn read(
        &mut self,
        key: &str,
    ) -> impl Future<Output = Result<Vec<u8>, wasmtime::error::Error>> + Send;
}

pub struct ReadHandle {
    pub store: Store<HostAPI>,
    pub reader: Reader,
}

impl ReadFn for ReadHandle {
    async fn read(&mut self, key: &str) -> Result<Vec<u8>, wasmtime::error::Error> {
        self.reader.call_read_fn(&mut self.store, key).await
    }
}
