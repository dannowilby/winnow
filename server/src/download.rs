use serde::{Deserialize, Serialize};


#[derive(Deserialize, Serialize)]
pub struct IntermediateData {
    pub key: String,
    pub value: Vec<u8>
}

#[derive(Deserialize, Serialize)]
pub struct OutputData(pub Vec<u8>);

#[derive(Debug, Deserialize, Serialize)]
pub struct DownloadRequest {
    pub location: String
}

pub async fn handle_download(dr: DownloadRequest) -> Vec<u8> {
    std::fs::read(format!("{}", dr.location)).expect("No matched file!")
}