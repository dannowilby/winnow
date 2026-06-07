#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

use wit_bindgen::generate;

generate!("reader" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct ReaderComponent;

impl Guest for ReaderComponent {
    async fn read_fn(key: String) -> Vec<u8> {
        let k = key.parse::<i32>().unwrap();

        let x: Vec<i32> = (((k - 1) * 10)..((k) * 10)).map(|x| x + 1).collect();

        rmp_serde::to_vec(&x).unwrap()
    }
}

export!(ReaderComponent);
