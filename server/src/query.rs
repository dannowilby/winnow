use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    cluster::Host, job_lookup::Progress, server::MapReduceServer, storage::StorageError,
    wasm::WasmEnv,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct DownloadRequest {
    pub location: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum QueryRequest {
    IsMapJobComplete(usize),
    /// Returns a blob of data that can be decoded into a vec of [IntermediateData](crate::storage::IntermediateData)
    DownloadMapOutput(usize, String),
    /// Returns a blob of data that can be decoded into a vec of [OutputData](crate::storage::OutputData)
    DownloadReduceOutput(String),
    IndexLocation(usize),

    JobProgress,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum QueryResponse {
    Data(Vec<u8>),
    Host(Host),
    Status(bool),
    Progress(Progress),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("no host found for map index {0}")]
    IndexNotFound(usize),
}

pub async fn handle_query<W: WasmEnv>(
    server: MapReduceServer<W>,
    q: QueryRequest,
) -> Result<QueryResponse, QueryError> {
    match q {
        QueryRequest::IsMapJobComplete(index) => Ok(QueryResponse::Status(
            server.job_lookup.read().await.is_map_job_complete(index),
        )),

        QueryRequest::JobProgress => Ok(QueryResponse::Progress(
            server.job_lookup.read().await.progress.clone(),
        )),

        QueryRequest::DownloadMapOutput(index, partition) => {
            // TODO: check that the job has completed first
            let data = server.storage.get_map_out(index, partition)?;
            Ok(QueryResponse::Data(data))
        }
        QueryRequest::DownloadReduceOutput(partition) => {
            let data = server.storage.get_reduce_out(partition)?;
            Ok(QueryResponse::Data(data))
        }
        QueryRequest::IndexLocation(index) => {
            let host = server
                .job_lookup
                .read()
                .await
                .try_get_host_by_index(index)
                .cloned()
                .ok_or(QueryError::IndexNotFound(index))?;
            Ok(QueryResponse::Host(host))
        }
    }
}
