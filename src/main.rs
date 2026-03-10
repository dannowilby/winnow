mod rpc;
mod tui;

mod job;

mod map;
mod reduce;

use std::iter::Iterator;
use std::sync::mpsc::{self, Receiver, Sender};

use crate::job::run_job;
use crate::map::map;
use crate::reduce::reduce;
use crate::rpc::start_rpc_listener;
use crate::tui::start_stdin_listener;

pub enum Event {
    UserInput(String),
    RpcRequest(Task),
}

fn main() -> Result<(), String> {
    let (tx, rx): (Sender<Event>, Receiver<Event>) = mpsc::channel();

    start_rpc_listener(tx.clone());
    start_stdin_listener(tx.clone());

    tui::print_help();
    tui::print_prompt();

    'outer: for event in rx {
        match event {
            Event::UserInput(line) => {
                match line.trim() {
                    "job" => run_job(&line[3..]),
                    "help" => tui::print_help(),
                    "quit" | "exit" => {
                        break 'outer;
                    }
                    _ => {
                        println!("Unknown command: {line}");
                        tui::print_help();
                    }
                }
            }

            Event::RpcRequest(task) => match task.task_type {
                TaskType::Map => map(task),
                TaskType::Reduce => reduce(task),
            },
        }
    }

    tui::print_exit_message();

    Ok(())
}

#[allow(dead_code)]
struct NodeInfo {
    addr: String,
    port: u16,
}

#[allow(dead_code)]
enum TaskType {
    Map,
    Reduce,
}

#[allow(dead_code)]
enum TaskStatus {
    Pending,
    InProgress,
    Completed,
}

#[allow(dead_code)]
pub struct Task {
    task_type: TaskType,
    status: TaskStatus,
    leader: Option<NodeInfo>,
}

#[allow(dead_code)]
struct Job<K1, V1, K2, V2> {
    r: usize,
    m: usize,
    chunk_size: usize,
    map: fn(K1, V1) -> Vec<(K2, V2)>,
    reduce: fn(K2, Vec<V2>) -> (K2, V2),

    master: Option<NodeInfo>,
}

#[allow(dead_code)]
trait Reader {
    fn read(&self) -> impl Iterator<Item = (String, String)>;
}

#[allow(dead_code)]
struct MockReader;

#[allow(dead_code)]
impl Reader for MockReader {
    fn read(&self) -> impl Iterator<Item = (String, String)> {
        vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]
        .into_iter()
    }
}
