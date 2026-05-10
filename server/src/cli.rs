use std::{
    fs,
    time::{Duration, Instant},
};

use mapreduce::{cluster::ClusterList, download::DownloadRequest, promote::PromoteRequest};
use serde::Deserialize;
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
        keys: vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
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

            let Ok(r) = cluster.get_modulo(0).1.download(context::current(), DownloadRequest {location: "data/odd-output".to_owned()}).await else {
                println!("Error with final output! 1");
                return Ok(());
            };

            println!("odd-sum: {:?}", deserialize_output(r));

            let Ok(r) = cluster.get_modulo(0).1.download(context::current(), DownloadRequest {location: "data/even-output".to_owned()}).await else {
                println!("Error with final output! 2");
                return Ok(());
            };

            println!("even-sum: {:?}", deserialize_output(r));
        }
        Err(e) => {
            println!("Job failed!");
            println!("Errored in: {}ms", n.elapsed().as_millis());
            eprintln!("{}",e);
        }
    }
    
    Ok(())
}

fn deserialize_output(r: Vec<u8>) -> Vec<i32> {

    let output_data: Vec<i32> = rmp_serde::from_slice(&r).expect("should have some actual data");
return output_data;
}