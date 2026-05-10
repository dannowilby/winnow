use std::{
    fs::{self, OpenOptions},
    io::{self, BufReader, Read, Write},
    marker::PhantomData,
    sync::Arc,
};

use ext_sort::{ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
use serde::{Deserialize, Serialize};
use tarpc::context;

use crate::{
    cluster::{ClusterConn, Host},
    download::{DownloadRequest, IntermediateData, OutputData},
    wasm::{WasmEnv, handle::reduce::ReduceFn},
};

#[derive(Debug, Deserialize, Serialize)]
pub struct ReduceRequest {
    pub partition: String,
    pub locations: Vec<Host>,

    pub reduce_src: Vec<u8>,
}

struct RmpIter<T> {
    reader: io::Take<BufReader<fs::File>>,
    _marker: PhantomData<T>,
}

impl<T: serde::de::DeserializeOwned> Iterator for RmpIter<T> {
    type Item = Result<T, rmp_serde::decode::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.limit() == 0 {
            return None;
        }
        Some(rmp_serde::decode::from_read(&mut self.reader))
    }
}

pub async fn handle_reduce<W: WasmEnv>(cluster: Arc<ClusterConn>, wasm_env: W, rr: ReduceRequest) {
    // if we're going to fail, at least fail before downloading everything
    let mut reducer = wasm_env
        .load_reduce_binary(&rr.reduce_src)
        .expect("Incorrect reduce binary received!");

    let file_path = format!("data/{}-sorted", &rr.partition);

    for host in rr.locations {
        let conn = cluster.get(host);
        println!("About to read from {} partition", rr.partition);
        let data = match conn
            .1
            .download(context::current(), DownloadRequest { location: format!("data/{}", rr.partition) })
            .await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{}", e);
                    panic!()
                }
            };

        let mut f = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_path)
            .expect("Could not access data");
        let _ = f.write(&data);
    }

    // external sort the data by key
    let sort_file = fs::File::open(&file_path).expect("Could not open data file");
    let file_size = sort_file.metadata().expect("Could not read file metadata").len();
    let reader = BufReader::new(sort_file).take(file_size);
    let iter: RmpIter<IntermediateData> = RmpIter { reader, _marker: PhantomData };

    let sorter: ExternalSorter<IntermediateData, rmp_serde::decode::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(std::path::Path::new("./data"))
            .with_buffer(LimitedBufferBuilder::new(100_000, false))
            .build()
            .expect("Could not build sorter");

    let mut sorted = sorter
        .sort_by(iter, |a, b| a.key.cmp(&b.key))
        .expect("Could not sort data").peekable();

    let output_file_path = format!("data/{}-output", rr.partition);
    let mut output_file = OpenOptions::new().append(true).create(true).open(output_file_path).expect("output has to go somehwere");

    // iterate over sorted kv pairs and fold with the reducer
    let mut acc = Vec::<u8>::new();
    
    while let Some(Ok(item)) = sorted.next() {

        // println!("item: {}", rmp_serde::from_slice::<i32>(&item.value).expect("l"));

        acc = tokio::task::block_in_place(|| { reducer
            .reduce(&item.key, &item.value, &acc)
            .expect("Reducer failed") });

        // println!("[Reducer] acc is now: {}", rmp_serde::from_slice::<i32>(&acc).expect("j"));

        if let Some(Ok(next_item)) = sorted.peek() {
            if next_item.key != item.key {
                let output = OutputData(acc);
                let b= rmp_serde::to_vec(&output).expect("should be able to encode output data");
                let _ = output_file.write(&b);

                acc = Vec::new();
            }
        }

    }
    // println!(
    //     "{}",
    //     rmp_serde::from_slice::<i32>(&acc).expect("h")
    // );
    let output = OutputData(acc);
    let b= rmp_serde::to_vec(&output).expect("should be able to encode output data");
    let _ = output_file.write(&b);

}
