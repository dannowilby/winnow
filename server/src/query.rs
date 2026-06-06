use serde::{Deserialize, Serialize};

use crate::{cluster::Host, map::MapJobMetadata, server::MapReduceServer, wasm::WasmEnv};

#[derive(Deserialize, Serialize)]
pub struct IntermediateData {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
pub struct OutputData(pub String, pub Vec<u8>);

#[derive(Debug, Deserialize, Serialize)]
pub struct DownloadRequest {
    pub location: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum QueryRequest {
    Download(String),
    IndexLocation(usize),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum QueryResponse {
    Data(Vec<u8>),
    Host(Host),
}

pub async fn handle_query<W: WasmEnv>(
    server: MapReduceServer<W>,
    q: QueryRequest,
) -> QueryResponse {
    match q {
        QueryRequest::Download(location) => {
            QueryResponse::Data(std::fs::read(format!("{}", location)).expect("No matched file!"))
        }
        QueryRequest::IndexLocation(index) => QueryResponse::Host(
            server
                .job_lookup
                .read()
                .await
                .get_host_by_index(index)
                .clone(),
        ),
    }
}
