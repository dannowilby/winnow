use std::collections::HashMap;

use crate::{cluster::Host, map::MapResponse};

pub struct JobLookup {
    /// Maps input index to the host it was computed on
    mapping: HashMap<usize, Host>,

    /// Maps the partition to the map jobs it was located on
    pub partition: HashMap<String, Vec<usize>>,

    /// Maps a host to the partition
    pub reducing: HashMap<String, Host>,
}

impl JobLookup {
    pub fn new() -> Self {
        Self {
            mapping: HashMap::new(),
            partition: HashMap::new(),
            reducing: HashMap::new(),
        }
    }

    pub fn create_map_job(&mut self, index: usize, host: Host) {
        self.mapping.insert(index, host);
    }

    /// Records the partitions a finished map job produced on `host`.
    pub fn complete_map_job(&mut self, mr: MapResponse) {
        for partition in mr.seen_partitions {
            self.partition
                .entry(partition)
                .and_modify(|v| v.push(mr.index))
                .or_insert(vec![mr.index]);
        }
    }

    pub fn create_reduce_job(&mut self, partition: String, host: Host) {
        self.reducing.insert(partition, host);
    }

    /// A stub for now.
    pub fn complete_reduce_job(&mut self, _partition: String) {}

    pub fn get_host_by_index(&self, index: usize) -> &Host {
        self.mapping.get(&index).unwrap()
    }

    pub fn get_indices_for_partition(&self, partition: &String) -> &Vec<usize> {
        self.partition.get(partition).unwrap()
    }

    pub fn get_reduce_job_partitions(&self, target_host: Host) -> Vec<&String> {
        let partitions = self
            .reducing
            .iter()
            .filter(|(_, host)| &target_host == *host)
            .collect::<HashMap<&String, &Host>>();
        partitions.keys().map(|p| *p).collect::<Vec<&String>>()
    }

    pub fn get_map_job_indices(&self, target_host: Host) -> Vec<usize> {
        let host_indexes = self
            .mapping
            .iter()
            .filter(|(_, host)| &target_host == *host)
            .collect::<HashMap<&usize, &Host>>();
        host_indexes.keys().map(|i| **i).collect::<Vec<usize>>()
    }

    pub fn clear(&mut self) {
        self.mapping.clear();
        self.partition.clear();
        self.reducing.clear();
    }
}
