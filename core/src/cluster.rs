use std::sync::Arc;

use crate::server::MapReduceServiceClient;
use crate::transport::Connector;
use futures::future::join_all;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};

/// Represents the address of a machine (ipv6 + port)
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct Host {
    #[serde(alias = "host")]
    pub domain: String,
    pub port: u16,
}

/// Represents a TCP connection to another machine
#[derive(Debug, Clone)]
pub struct ActiveConnection {
    pub host: Host,
    pub client: Option<MapReduceServiceClient>,
}

#[derive(Clone)]
pub struct ClusterList {
    members: Vec<Host>,
    loopback: usize,
    connector: Arc<dyn Connector>,
}

pub struct Cluster {
    members: Vec<ActiveConnection>,
    loopback: usize,
    rng: StdRng,
    connector: Arc<dyn Connector>,
}

const SEED: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

impl ClusterList {
    pub fn new(
        members: Vec<(String, u16)>,
        loopback: usize,
        connector: Arc<dyn Connector>,
    ) -> Self {
        Self {
            members: members
                .into_iter()
                .map(|(domain, port)| Host { domain, port })
                .collect(),
            loopback,
            connector,
        }
    }

    /// Build a cluster list directly from already-parsed [Host]s, e.g. ones
    /// deserialized from a `cluster.json` file.
    pub fn from_hosts(members: Vec<Host>, loopback: usize, connector: Arc<dyn Connector>) -> Self {
        Self {
            members,
            loopback,
            connector,
        }
    }

    /// Connects to every member through the configured [Connector]. We ignore
    /// any failed connections and continue on without that machine.
    pub async fn connect(self) -> Cluster {
        let connector = self.connector;

        let conn_futs = self.members.into_iter().map(|host| {
            let connector = connector.clone();
            async move {
                let client = connector.connect(&host).await;
                ActiveConnection { host, client }
            }
        });

        Cluster {
            members: join_all(conn_futs).await,
            loopback: self.loopback,
            rng: StdRng::from_seed(SEED),
            connector,
        }
    }
}

impl Cluster {
    /// Get a random, non-failed host's connection.
    pub fn get_random(&mut self) -> &ActiveConnection {
        let active = self
            .members
            .iter()
            .filter(|connection| connection.client.is_some())
            .collect::<Vec<_>>();

        if active.is_empty() {
            panic!("No active members to get from!");
        }

        let index = self.rng.random_range(0..active.len());
        active.get(index).unwrap()
    }

    /// Gets the running machine's connection to itself. Useful for sending
    /// other machine's the leader's address.
    pub fn get_loopback(&self) -> &ActiveConnection {
        self.members.get(self.loopback).unwrap()
    }

    /// Gets the active connection for a host disregarding whether or not the
    /// host has failed.
    pub fn get_unchecked(&self, host: Host) -> &ActiveConnection {
        self.members
            .iter()
            .find(|connection| connection.host == host)
            .unwrap()
    }

    /// Similar to [get_unchecked](crate::cluster::Cluster::get_unchecked),
    /// except it returns a mutable reference.
    pub fn get_mut_unchecked(&mut self, host: Host) -> &mut ActiveConnection {
        self.members
            .iter_mut()
            .find(|connection| connection.host == host)
            .unwrap()
    }

    /// Marks a host as failed.
    pub fn signal_fail(&mut self, host: Host) {
        self.get_mut_unchecked(host).client = None;
    }

    /// Tries to reconnect to the failed connections through the configured
    /// [Connector].
    pub async fn reconnect(&mut self) {
        let connector = self.connector.clone();
        for ActiveConnection { host, client } in self.members.iter_mut() {
            // leave unfailed connection unchanged
            if client.is_some() {
                continue;
            }

            *client = connector.connect(host).await;
        }
    }

    /// Returns an iterator over all the members of the cluster, regardless if
    /// they've failed.
    pub fn iter<'a>(&'a self) -> ClusterIter<'a> {
        ClusterIter {
            members: &self.members,
            index: 0,
        }
    }
}

pub struct ClusterIter<'a> {
    members: &'a Vec<ActiveConnection>,
    index: usize,
}

impl<'a> Iterator for ClusterIter<'a> {
    type Item = &'a ActiveConnection;

    fn next(&mut self) -> Option<Self::Item> {
        let connection = self.members.get(self.index);
        self.index += 1;
        connection
    }
}
