use wit_bindgen::generate;

generate!("reducer" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct ReducerComponent;

impl Guest for ReducerComponent {
    async fn reduce_fn(key: String, value: Vec<u8>, acc: Vec<u8>) -> Vec<u8> {
        let v: i32 = rmp_serde::from_slice(&value).expect("r1");

        let mut a = 0;
        if acc.len() > 0 {
            a = rmp_serde::from_slice(&acc).expect("r2");
        }

        rmp_serde::to_vec(&(v + a)).expect("r3")
    }
}

export!(ReducerComponent);
