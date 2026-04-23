
pub struct PromoteRequest {

}

pub fn run_promote() {
    // get the binary for the components
    // ie, reader, mapper, reducer (with key comparison), and partitioner
    // get the key file with all the input keys
    // get M and R

    // create a list of healthy machines based on the cluster
    // start a thread to monitor their health

    // split the input keys into M portions
    // create map job requests and send them 

    // wait until all map jobs have been fulfilled
    // now we should have all the locations of processed, partitioned data

    // create the reduce jobs sending over the binary sources and the locations
    // of the partition's data

    // if a worker fails, recompute any map jobs done on it

}