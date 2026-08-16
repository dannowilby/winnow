use std::future::Future;

use wasmtime::Store;

use crate::wasm::{context::HostAPI, reducer::Reducer};

pub trait ReduceFn {
    fn reduce(
        &mut self,
        key: &str,
        value: &[u8],
        acc: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, wasmtime::error::Error>> + Send;
}

pub struct ReduceHandle {
    pub store: Store<HostAPI>,
    pub reducer: Reducer,
}

impl ReduceFn for ReduceHandle {
    async fn reduce(
        &mut self,
        key: &str,
        value: &[u8],
        acc: &[u8],
    ) -> Result<Vec<u8>, wasmtime::error::Error> {
        self.reducer
            .call_reduce_fn(&mut self.store, key, value, acc)
            .await
    }
}
