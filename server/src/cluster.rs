#[derive(Clone)]
pub struct RemoteHost {
    pub host: String,
    pub port: u16,
}

#[derive(Clone)]
pub struct ClusterConfig {
    pub instances: Vec<RemoteHost>,
}
