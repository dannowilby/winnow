//! In-process transport for tests.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::{StreamExt, future::BoxFuture};
use tarpc::{
    client::Config,
    server::{BaseChannel, Channel},
};
use tokio::{
    sync::{RwLock, mpsc::unbounded_channel},
    task::JoinHandle,
};

use mapreduce::{
    cluster::Host,
    server::{MapReduceServer, MapReduceService, MapReduceServiceClient, context},
    transport::Connector,
    wasm::DefaultWasmEnv,
};

use crate::common::add_node;

/// A closure that mints a fresh client for a registered node. Each call sets up
/// a new in-memory channel and hands the server half to the node's serve loop.
type Dialer = Box<dyn Fn() -> Option<MapReduceServiceClient> + Send + Sync>;

/// A registered node: the dialer future connects use, plus every task its serve
/// loop has spawned (the accept loop and one task per connection) so [`kill`] can
/// abort them.
struct Node {
    dialer: Dialer,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

/// In-process connector for tests. Holds a registry mapping each [`Host`] to the
/// node serving it; connecting invokes that node's dialer to spin up a fresh
/// `tarpc::transport::channel` pair into its serve loop.
#[derive(Clone, Default)]
pub struct InMemoryNet {
    registry: Arc<RwLock<HashMap<Host, Node>>>,
}

impl InMemoryNet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wire `server` up as the node serving `host`: spawn its serve loop and
    /// register a dialer so connects to `host` mint a fresh in-memory channel
    /// into that loop. All spawned tasks are tracked so [`kill`](InMemoryNet::kill)
    /// can tear the node down.
    pub async fn serve(&self, host: Host, server: MapReduceServer<DefaultWasmEnv>) {
        let tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let (incoming_tx, mut incoming_rx) = unbounded_channel();

        // Accept loop: every dial mints a fresh channel into this server. Each
        // connection is its own tracked task so killing the node drops the
        // channel and closes the client's transport.
        let conn_tasks = tasks.clone();
        let accept = tokio::spawn(async move {
            while let Some(server_transport) = incoming_rx.recv().await {
                let server = server.clone();
                let channel = BaseChannel::with_defaults(server_transport);
                let conn = tokio::spawn(channel.execute(server.serve()).for_each(|rpc| async {
                    tokio::spawn(rpc);
                }));
                conn_tasks.lock().unwrap().push(conn);
            }
        });
        tasks.lock().unwrap().push(accept);

        let dialer: Dialer = Box::new(move || {
            let (client_t, server_t) = tarpc::transport::channel::unbounded();
            incoming_tx.send(server_t).ok()?;
            Some(MapReduceServiceClient::new(Config::default(), client_t).spawn())
        });

        self.registry
            .write()
            .await
            .insert(host, Node { dialer, tasks });
    }

    /// Kill the node serving `host`: abort its serve tasks (closing every live
    /// connection so existing clients' RPCs error out) and drop its dialer (so
    /// future `connect`/`reconnect` calls return `None`). The twin of a node
    /// that crashes and stays down.
    pub async fn kill(&self, host: &Host) {
        if let Some(node) = self.registry.write().await.remove(host) {
            for task in node.tasks.lock().unwrap().drain(..) {
                task.abort();
            }
        }
    }
}

impl Connector for InMemoryNet {
    fn connect(&self, host: &Host) -> BoxFuture<'_, Option<MapReduceServiceClient>> {
        let host = host.clone();
        Box::pin(async move {
            self.registry
                .read()
                .await
                .get(&host)
                .and_then(|node| (node.dialer)())
        })
    }
}

#[tokio::test]
async fn in_memory_connector_round_trips_rpc() {
    let net = Arc::new(InMemoryNet::new());
    let hosts = vec![Host {
        domain: "[::1]".to_owned(),
        port: 3000,
    }];

    let _node = add_node(net.clone(), hosts.clone(), 0).await;

    // A live host dials successfully and serves a real RPC.
    let client = net
        .connect(&hosts[0])
        .await
        .expect("connect to a registered node");
    assert!(
        client.heartbeat(context()).await.expect("heartbeat rpc"),
        "a healthy node should answer its heartbeat",
    );

    // Failure injection: once killed, the host behaves like a dead node.
    net.kill(&hosts[0]).await;
    assert!(
        net.connect(&hosts[0]).await.is_none(),
        "a killed node should be unreachable",
    );
}

#[tokio::test]
async fn multiple_connections() {
    let net = Arc::new(InMemoryNet::new());
    let hosts = vec![
        Host {
            domain: "[::1]".to_owned(),
            port: 3000,
        },
        Host {
            domain: "[::1]".to_owned(),
            port: 3001,
        },
    ];

    let _node = add_node(net.clone(), hosts.clone(), 0).await;
    let _node = add_node(net.clone(), hosts.clone(), 1).await;

    // A live host dials successfully and serves a real RPC.
    let client = net
        .connect(&hosts[0])
        .await
        .expect("connect to a registered node");
    assert!(
        client.heartbeat(context()).await.expect("heartbeat rpc"),
        "a healthy node should answer its heartbeat",
    );

    // Failure injection: once killed, the host behaves like a dead node.
    net.kill(&hosts[0]).await;
    assert!(
        net.connect(&hosts[0]).await.is_none(),
        "a killed node should be unreachable",
    );
    assert!(
        net.connect(&hosts[1]).await.is_some(),
        "a node should be reachable",
    );
}

#[tokio::test]
async fn kill_breaks_existing_client_connections() {
    let net = Arc::new(InMemoryNet::new());
    let hosts = vec![Host {
        domain: "[::1]".to_owned(),
        port: 3000,
    }];
    add_node(net.clone(), hosts.clone(), 0).await;

    // An already-connected client works...
    let client = net.connect(&hosts[0]).await.expect("connect");
    assert!(client.heartbeat(context()).await.expect("heartbeat ok"));

    // ...and after kill, that SAME client's RPCs error (transport closed),
    // which is what surfaces a MachineFailure in handle_promote.
    net.kill(&hosts[0]).await;
    assert!(
        client.heartbeat(context()).await.is_err(),
        "existing client should error once its node is killed",
    );
}
