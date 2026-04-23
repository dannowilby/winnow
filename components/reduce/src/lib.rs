
use wit_bindgen::generate;

generate!("reducer" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::{ log };

struct ReducerComponent;

impl Guest for ReducerComponent {
    fn reduce_fn(key: String, value: Vec<u8>, acc: Vec<u8>) -> Vec<u8> {
        // let t = unsafe { std::mem::transmute::<&[u8], u32>(&value) };
        log(&key);
        vec![]
    }
}

export!(ReducerComponent);