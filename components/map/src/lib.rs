use wit_bindgen::generate;

generate!("mapper" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct MapperComponent;

impl Guest for MapperComponent {
    async fn map_fn(key: String, value: Vec<u8>) -> Vec<(String, Vec<u8>)> {
        let v: Vec<i32> = rmp_serde::from_slice(&value).expect("should be able to parse read data");
        let mut output = Vec::new();

        for x in v {
            if x % 2 == 0 {
                output.push(("even".to_owned(), rmp_serde::to_vec(&x).expect("s1")));
            } else {
                output.push(("odd".to_owned(), rmp_serde::to_vec(&x).expect("s2")));
            }
        }

        output
    }
}

export!(MapperComponent);
