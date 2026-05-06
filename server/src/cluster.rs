use crate::server::MapReduceServiceClient;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tarpc::{client, tokio_serde::formats::Json};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Host {
    pub domain: String,
    pub port: u16,
}

impl Host {
    pub fn key(&self) -> String {
        format_host(&self.domain, self.port)
    }
}

fn format_host(domain: &String, port: u16) -> String {
    format!("{}:{}", domain, port)
}

#[derive(Debug, Clone)]
pub struct ClusterList {
    pub members: Vec<Host>,
}

#[derive(Debug, Clone)]
pub struct Conn(pub Host, pub MapReduceServiceClient);

#[derive(Debug)]
pub struct ClusterConn {
    pub members: Vec<Conn>,
}

impl ClusterList {
    pub fn new(members: Vec<(String, u16)>) -> Self {
        Self {
            members: members
                .into_iter()
                .map(|(domain, port)| Host { domain, port })
                .collect(),
        }
    }

    /// For now, each cluster is assumed to be over TCP. Changing this method
    /// should not be hard. We also ignore any failed connections and try to
    /// continue on without that machine
    pub async fn connect(&self) -> ClusterConn {
        let conn_futs = self
            .members
            .clone()
            .into_iter()
            .map(|Host { domain, port }| async move {
                let mut transport = tarpc::serde_transport::tcp::connect(
                    format_host(&domain, port),
                    Json::default,
                );
                transport.config_mut().max_frame_length(usize::MAX);

                (domain, port, transport.await)
            });

        let mut instances = Vec::new();

        for (domain, port, transport_result) in join_all(conn_futs).await.into_iter() {
            match transport_result {
                Ok(transport) => {
                    instances.push(Conn(
                        Host { domain, port },
                        MapReduceServiceClient::new(client::Config::default(), transport).spawn(),
                    ));
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        ClusterConn { members: instances }
    }
}

impl ClusterConn {
    /// Helper method to make spreading initial jobs to machines easy.
    pub fn get_modulo<'a>(&'a self, index: usize) -> &'a Conn {
        let n = self.members.len();

        self.members.get(index % n).expect("Cluster internal lookup: index % n has failed to retrieve an instance! Seriously wrong!")
    }
}
