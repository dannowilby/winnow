#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

use common::Temp;
use postcard::to_allocvec;
use wit_bindgen::generate;

generate!("reader" in "../../wit/world.wit");

use mapreduce::typeimpls::logging::log;

struct ReaderComponent;

impl Guest for ReaderComponent {
    fn read_fn(key: String) -> Vec<u8> {
        // let t = unsafe { std::mem::transmute::<&[u8], u32>(&value) };
        log(&format!("This is the reader speaking: {}!", &key));
        to_allocvec(&Temp(179)).unwrap()
    }
}

export!(ReaderComponent);
