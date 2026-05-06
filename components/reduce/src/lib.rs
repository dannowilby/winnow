use wit_bindgen::generate;

generate!("reducer" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct ReducerComponent;

impl Guest for ReducerComponent {
    fn reduce_fn(key: String, _value: Vec<u8>, _acc: Vec<u8>) -> Vec<u8> {
        log(&key);
        vec![]
    }
}

export!(ReducerComponent);
