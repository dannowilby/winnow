//! Transport seam between the cluster logic and the wire.

use futures::future::BoxFuture;
use tarpc::{client, tokio_serde::formats::Bincode};

use crate::cluster::Host;
use crate::server::MapReduceServiceClient;

/// Produces a connected client for a host, or `None` if the connection fails.
pub trait Connector: Send + Sync {
    fn connect(&self, host: &Host) -> BoxFuture<'_, Option<MapReduceServiceClient>>;
}

/// Production connector: dials each host over TCP.
#[derive(Debug, Clone, Copy, Default)]
pub struct TcpConnector;

impl Connector for TcpConnector {
    fn connect(&self, host: &Host) -> BoxFuture<'_, Option<MapReduceServiceClient>> {
        let host = host.clone();
        Box::pin(async move {
            let mut transport = tarpc::serde_transport::tcp::connect(
                format!("{}:{}", host.domain, host.port),
                Bincode::default,
            );
            transport.config_mut().max_frame_length(usize::MAX);

            transport.await.ok().map(|transport| {
                MapReduceServiceClient::new(client::Config::default(), transport).spawn()
            })
        })
    }
}
