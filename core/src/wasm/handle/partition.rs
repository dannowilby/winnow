use crate::wasm::{context::HostAPI, partitioner::Partitioner};
use std::future::Future;
use wasmtime::Store;

pub trait PartitionFn {
    fn partition(
        &mut self,
        key: &str,
        r: u32,
    ) -> impl Future<Output = Result<String, wasmtime::error::Error>> + Send;
}

pub struct PartitionHandle {
    pub store: Store<HostAPI>,
    pub partitioner: Partitioner,
}

impl PartitionFn for PartitionHandle {
    async fn partition(&mut self, key: &str, r: u32) -> Result<String, wasmtime::error::Error> {
        self.partitioner
            .call_partition_fn(&mut self.store, key, r)
            .await
    }
}
