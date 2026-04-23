use anyhow::Result;

#[derive(Debug)]
pub struct MapRequest {
    pub key_range: Vec<String>,
    pub r: u32,

    pub map_src: String,
    pub partition_src: String,
    pub reader_src: String,
}

pub fn perform_map(mp: MapRequest) -> Result<()> {
    // decode the request to get the key range
    // get R, the number of partitions here too
    // Ideally, the map and partition functions will be passed here as well

    for _k in mp.key_range {
        // read the data for k
        // run the map function on k,v
        // buffer/write the results
    }

    // send back the locations to the master
    Ok(())
}
