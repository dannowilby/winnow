
use wit_bindgen::generate;

generate!("partitioner" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::{ log };

struct PartitionerComponent;

impl Guest for PartitionerComponent {
    fn partition_fn(key: String, r: u32) -> String {
        // let t = unsafe { std::mem::transmute::<&[u8], u32>(&value) };
        log(&key);
        String::from("test-partition")
    }
}

export!(PartitionerComponent);