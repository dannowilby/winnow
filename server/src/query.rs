use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{cluster::Host, server::MapReduceServer, storage::StorageError, wasm::WasmEnv};

#[derive(Debug, Deserialize, Serialize)]
pub struct DownloadRequest {
    pub location: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum QueryRequest {
    /// Returns a blob of data that can be decoded into a vec of [IntermediateData](crate::storage::IntermediateData)
    DownloadMapOutput(usize, String),
    /// Returns a blob of data that can be decoded into a vec of [OutputData](crate::storage::OutputData)
    DownloadReduceOutput(String),
    IndexLocation(usize),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum QueryResponse {
    Data(Vec<u8>),
    Host(Host),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
}

pub async fn handle_query<W: WasmEnv>(
    server: MapReduceServer<W>,
    q: QueryRequest,
) -> Result<QueryResponse, QueryError> {
    match q {
        QueryRequest::DownloadMapOutput(index, partition) => {
            let data = server.storage.get_map_out(index, partition)?;
            Ok(QueryResponse::Data(data))
        }
        QueryRequest::DownloadReduceOutput(partition) => {
            let data = server.storage.get_reduce_out(partition)?;
            Ok(QueryResponse::Data(data))
        }
        QueryRequest::IndexLocation(index) => Ok(QueryResponse::Host(
            server
                .job_lookup
                .read()
                .await
                .get_host_by_index(index)
                .clone(),
        )),
    }
}
