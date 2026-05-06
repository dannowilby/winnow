use std::{
    fs,
    time::{Duration, Instant},
};

use mapreduce::{cluster::ClusterList, promote::PromoteRequest};
use tarpc::context;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterList::new(vec![("localhost".to_owned(), 3000)])
        .connect()
        .await;

    // Where you might find the wasm binaries:
    // let paths = fs::read_dir("./target/wasm32-wasip2/release").unwrap();
    // for path in paths {
    //     println!("Name: {}", path.unwrap().path().display())
    // }

    let promote_request = PromoteRequest {
        read_src: fs::read("./target/wasm32-wasip2/release/read.wasm")?,
        map_src: fs::read("./target/wasm32-wasip2/release/map.wasm")?,
        reduce_src: fs::read("./target/wasm32-wasip2/release/reduce.wasm")?,
        partition_src: fs::read("./target/wasm32-wasip2/release/partition.wasm")?,
        m: 2,
        r: 2,
        keys: vec!["Key 1".to_owned(), "Key 2".to_owned()],
    };

    let mut ctx = context::current();
    ctx.deadline = Instant::now() + Duration::from_secs(15);

    let n = Instant::now();
    let ack = cluster
        .get_modulo(0)
        .1
        .promote(ctx, promote_request)
        .await;
    
    match ack {
        Ok(_) => {
            println!("Job completed successfully!");
            println!();
            println!("Completed in: {}ms", n.elapsed().as_millis());
        }
        Err(e) => {
            println!("Job failed!");
            println!("Errored in: {}ms", n.elapsed().as_millis());
            eprintln!("{}",e);
        }
    }
    
    Ok(())
}
