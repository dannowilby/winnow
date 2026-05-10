use std::hash::{DefaultHasher, Hash, Hasher};

use wit_bindgen::generate;

generate!("partitioner" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct PartitionerComponent;

impl Guest for PartitionerComponent {
    fn partition_fn(key: String, r: u32) -> String {
        let mut dh = DefaultHasher::new();
        key.hash(&mut dh);
        let t = dh.finish() as u32;
        
        log(&key);
        
        key
    }
}

export!(PartitionerComponent);
