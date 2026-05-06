use serde::{Deserialize, Serialize};

use crate::cluster::Host;

#[derive(Debug, Deserialize, Serialize)]
pub struct ReduceRequest {
    pub partition: String,
    pub locations: Vec<Host>,

    pub reduce_src: Vec<u8>
}

pub async fn handle_reduce() {
    // get the locations of all the data
    // get the reduce function
    // get a sort function

    // download the data
    // sort the data

    // iterate over the data with kv pairs and calculate the final result

    // write the final result
    // send a message to master indicating that the reduce task has finished
}