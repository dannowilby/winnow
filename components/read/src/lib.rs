#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

use wit_bindgen::generate;

generate!("reader" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct ReaderComponent;

impl Guest for ReaderComponent {
    fn read_fn(key: String) -> Vec<u8> {
        
        log(&format!("Mapping key: {}", &key));

        if &key == "1" {
            return rmp_serde::to_vec(&vec![1, 2, 3, 4, 5]).unwrap();
        }
        if &key == "2" {
            return rmp_serde::to_vec(&vec![6, 7, 8, 9, 10]).unwrap();
        }
        if &key == "3" {
            return rmp_serde::to_vec(&vec![11, 12, 13, 14, 15]).unwrap();
        }

        
        rmp_serde::to_vec(&Vec::<u8>::new()).unwrap()
    }
}

export!(ReaderComponent);
