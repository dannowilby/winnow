
use postcard::from_bytes;
use wit_bindgen::generate;

use common::Temp;

generate!("mapper" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::{ log };

struct MapperComponent;

impl Guest for MapperComponent {
    fn map_fn(key: String, value: Vec<u8>) {
        let x: Temp = from_bytes(&value).unwrap();
        log(&format!("hello there! {}, {}", key, x.0));
    }
}

export!(MapperComponent);