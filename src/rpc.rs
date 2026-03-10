use std::sync::mpsc::Sender;

use crate::Event;

pub fn start_rpc_listener(_tx: Sender<Event>) {
    let _ = std::thread::spawn(move || {
        // Listen for RPC connections on a separate thread
        // rpc_tx.send(Event::RpcRequest(stream));
    });
}
