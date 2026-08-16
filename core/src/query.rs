use serde::{Deserialize, Serialize};
use tarpc::context;
use thiserror::Error;
use tracing::error;

use crate::{
    cluster::Host,
    job_lookup::Progress,
    server::{MapReduceServer, set_parent},
    storage::StorageError,
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

#[tracing::instrument(name = "Query", skip_all)]
pub async fn handle_query<W: WasmEnv>(
    server: MapReduceServer<W>,
    ctx: context::Context,
    q: QueryRequest,
) -> Result<QueryResponse, QueryError> {
    set_parent(&tracing::Span::current(), &ctx);

    match q {
        QueryRequest::IsMapJobComplete(index) => Ok(QueryResponse::Status(
            server.job_lookup.read().await.is_map_job_complete(index),
        )),

        QueryRequest::JobProgress => Ok(QueryResponse::Progress(
            server.job_lookup.read().await.progress.clone(),
        )),

        QueryRequest::DownloadMapOutput(index, partition) => {
            let data = server
                .storage
                .get_map_out(index, partition.clone())
                .await
                .inspect_err(
                    |e| error!(index, partition = %partition, error = %e, "DownloadMapOutput failed"),
                )?;
            Ok(QueryResponse::Data(data))
        }
        QueryRequest::DownloadReduceOutput(partition) => {
            let data = server
                .storage
                .get_reduce_out(partition.clone())
                .await
                .inspect_err(
                    |e| error!(partition = %partition, error = %e, "DownloadReduceOutput failed"),
                )?;
            Ok(QueryResponse::Data(data))
        }
        QueryRequest::IndexLocation(index) => {
            let host = server
                .job_lookup
                .read()
                .await
                .try_get_host_by_index(index)
                .cloned()
                .ok_or(QueryError::IndexNotFound(index))
                .inspect_err(|e| error!(index, error = %e, "IndexLocation failed"))?;
            Ok(QueryResponse::Host(host))
        }
    }
}
