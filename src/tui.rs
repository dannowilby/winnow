use std::io::Write;
use std::sync::mpsc::Sender;

use crate::Event;

/**
  Starts a background thread that listens for user input from stdin and sends it to the main thread via the provided channel.
*/
pub fn start_stdin_listener(tx: Sender<Event>) {
    let _ = std::thread::spawn(move || {
        loop {
            match readline() {
                Ok(line) => {
                    let _ = tx.send(Event::UserInput(line));
                }
                Err(e) => {
                    eprintln!("Stdin error: {e}");
                    break;
                }
            }
        }
    });
}

pub fn print_help() {
    println!();
    println!("Rust MapReduce");
    println!();
    println!("Commands: job | help | quit/exit");
    println!("  job <folder>");
    println!("    Submits a new MapReduce job with input data from the specified folder.");
    println!("    Check the README for details on the expected folder structure and input format.");
    println!();
}

pub fn print_prompt() {
    print!("$ ");
    std::io::stdout().flush().ok();
}

pub fn print_exit_message() {
    println!();
    println!("Exiting Rust MapReduce. Goodbye!");
    println!();
}

fn readline() -> Result<String, String> {
    let mut buffer = String::new();
    std::io::stdin()
        .read_line(&mut buffer)
        .map_err(|e| e.to_string())?;
    Ok(buffer)
}
