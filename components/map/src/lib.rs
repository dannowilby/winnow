use postcard::{from_bytes, to_allocvec};
use wit_bindgen::generate;

use common::Temp;

generate!("mapper" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct MapperComponent;

impl Guest for MapperComponent {
    fn map_fn(key: String, value: Vec<u8>) -> Vec<String> {
        let x: Temp = from_bytes(&value).unwrap();
        log(&format!("Hello there from inside mapper! {}, {}", key, x.0));
        emit(&key, &to_allocvec(&Temp(x.0 + 3)).unwrap());

        vec!["seen-partition-1".to_owned()]
    }
}

export!(MapperComponent);
