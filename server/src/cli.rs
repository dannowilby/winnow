use std::{fs, time::Instant};

use mapreduce::{
    cluster::ClusterList,
    error::CliError,
    promote::PromoteRequest,
    query::{OutputData, QueryRequest, QueryResponse},
    server::context,
};

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let cluster = ClusterList::new(
        vec![
            ("[::1]".to_owned(), 3000),
            ("[::1]".to_owned(), 3001),
            ("[::1]".to_owned(), 3002),
        ],
        0,
    )
    .connect()
    .await;
    println!("Here?");
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
        m: 5,
        r: 2,
        keys: vec!["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
            .iter()
            .map(|k| String::from(*k))
            .collect::<Vec<String>>(),
    };

    let ctx = context();

    let n = Instant::now();
    let ack = cluster
        .get_loopback()
        .client
        .as_ref()
        .unwrap()
        .promote(ctx, promote_request)
        .await?;

    println!("Took {}ms to complete!", n.elapsed().as_millis());

    for (partition, host) in ack {
        let download = cluster
            .get_unchecked(host)
            .client
            .as_ref()
            .unwrap()
            .query(
                context(),
                QueryRequest::Download(format!("data/{}-output", &partition)),
            )
            .await?;

        let QueryResponse::Data(d) = download else {
            return Err(CliError::from(
                "Error decoding successful query response, got some other QueryResponse variant",
            ));
            // panic!();
        };

        deserialize_and_print_output(d);
    }

    Ok(())
}

fn deserialize_and_print_output(r: Vec<u8>) {
    let output_data: OutputData = rmp_serde::from_slice(&r).expect("should have some actual data");
    let o: i32 = rmp_serde::from_slice(&output_data.1).expect("hm");
    println!("{}: {}", output_data.0, o);
}
