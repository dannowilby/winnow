use wasmtime::Store;

use crate::wasm::{context::HostAPI, reader::Reader};

pub trait ReadFn {
    fn read(&mut self, key: &str) -> Result<Vec<u8>, wasmtime::error::Error>;
}

pub struct ReadHandle {
    pub store: Store<HostAPI>,
    pub reader: Reader,
}

impl ReadFn for ReadHandle {
    fn read(&mut self, key: &str) -> Result<Vec<u8>, wasmtime::error::Error> {
        self.reader.call_read_fn(&mut self.store, key)
    }
}
