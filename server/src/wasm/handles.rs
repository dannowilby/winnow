//! Handles abstract the actual loaded wasm functions/components. These can
//! be easily mocked to test different failure types.

use wasmtime::Store;

use crate::wasm::{context::HostAPI, mapper::Mapper, partitioner::Partitioner, reader::Reader, reducer::Reducer};

pub trait Partition {
    fn partition(&mut self, key: &str, r: u32) -> Result<String, wasmtime::error::Error>;
}

pub trait Map {
    fn map(&mut self, key: &str, value: &[u8]) -> Result<(), wasmtime::error::Error>;
}

pub trait Reduce {
    fn reduce(&mut self, key: &str, value: &[u8], acc: &[u8]) -> Result<Vec<u8>, wasmtime::error::Error>;
}

pub trait Read {
    fn read(&mut self, key: &str) -> Result<Vec<u8>, wasmtime::error::Error>;
}

pub struct PartitionHandle {
    pub store: Store<HostAPI>,
    pub partitioner: Partitioner
}

impl Partition for PartitionHandle {
        fn partition(&mut self, key: &str, r: u32) -> Result<String, wasmtime::error::Error> {
            (&self.partitioner).call_partition_fn(&mut self.store, key, r)
        }
}

pub struct MapHandle {
    pub store: Store<HostAPI>,
    pub mapper: Mapper
}

impl Map for MapHandle {
    fn map(&mut self, key: &str, value: &[u8]) -> Result<(), wasmtime::error::Error> {
        (&self.mapper).call_map_fn(&mut self.store, key, value)
    }
}

pub struct ReduceHandle {
    pub store: Store<HostAPI>,
    pub reducer: Reducer
}

impl Reduce for ReduceHandle {
    fn reduce(&mut self, key: &str, value: &[u8], acc: &[u8]) -> Result<Vec<u8>, wasmtime::error::Error> {
        (&self.reducer).call_reduce_fn(&mut self.store, key, value, acc)
    }
}

pub struct ReadHandle {
    pub store: Store<HostAPI>,
    pub reader: Reader
}

impl Read for ReadHandle {
    fn read(&mut self, key: &str) -> Result<Vec<u8>, wasmtime::error::Error> {
        (&self.reader).call_read_fn(&mut self.store, key)
    }
}