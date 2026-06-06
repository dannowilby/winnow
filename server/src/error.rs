use tarpc::client::RpcError;

#[derive(Debug)]
pub enum CliError {
    IoError(std::io::Error),
    RpcError(RpcError),
    Other(String),
}

impl From<std::io::Error> for CliError {
    fn from(value: std::io::Error) -> Self {
        CliError::IoError(value)
    }
}

impl From<RpcError> for CliError {
    fn from(value: RpcError) -> Self {
        CliError::RpcError(value)
    }
}

impl From<&str> for CliError {
    fn from(value: &str) -> Self {
        CliError::Other(value.to_string())
    }
}
