use std::collections::{HashMap, HashSet};

use crate::{cluster::Host, map::MapResponse};

#[derive(Default)]
pub struct JobLookup {
    /// Maps input index to the host it was computed on
    mapping: HashMap<usize, Host>,

    /// Maps the partition to the map jobs it was located on
    pub partition: HashMap<String, HashSet<usize>>,

    /// Maps a host to the partition
    pub reducing: HashMap<String, Host>,
}

impl JobLookup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_map_job(&mut self, index: usize, host: Host) {
        self.mapping.insert(index, host);
    }

    pub fn signal_map_job_failure(&mut self, index: usize) {
        // Only the index's *location* is lost on a host failure; it still feeds
        // the same partitions once recomputed. Leave `partition` untouched so a
        // reduce reassigned in the same failure pass keeps the full index set
        // (`complete_map_job` re-inserts idempotently). `create_map_job`
        // overwrites the mapping entry with the new host immediately after.
        self.mapping.remove(&index);
    }

    /// Records the partitions a finished map job produced on `host`.
    pub fn complete_map_job(&mut self, mr: MapResponse) {
        for partition in mr.seen_partitions {
            self.partition
                .entry(partition)
                .and_modify(|v| {
                    v.insert(mr.index);
                })
                .or_insert(HashSet::from([mr.index]));
        }
    }

    pub fn is_map_job_complete(&self, partition: &String, index: usize) -> bool {
        self.partition
            .get(partition)
            .is_some_and(|indices| indices.contains(&index))
    }

    pub fn create_reduce_job(&mut self, partition: String, host: Host) {
        self.reducing.insert(partition, host);
    }

    pub fn signal_reduce_job_failure(&mut self, partition: &String) {
        self.reducing.remove(partition);
    }

    /// A stub for now.
    pub fn complete_reduce_job(&mut self, _partition: String) {}

    pub fn get_host_by_index(&self, index: usize) -> &Host {
        self.mapping.get(&index).unwrap()
    }

    pub fn try_get_host_by_index(&self, index: usize) -> Option<&Host> {
        self.mapping.get(&index)
    }

    pub fn get_indices_for_partition(&self, partition: &String) -> &HashSet<usize> {
        self.partition.get(partition).unwrap()
    }

    pub fn get_reduce_job_partitions(&self, target_host: Host) -> Vec<&String> {
        let partitions = self
            .reducing
            .iter()
            .filter(|(_, host)| &target_host == *host)
            .collect::<HashMap<&String, &Host>>();
        partitions.keys().copied().collect::<Vec<&String>>()
    }

    pub fn get_map_job_indices(&self, target_host: Host) -> Vec<usize> {
        let host_indexes = self
            .mapping
            .iter()
            .filter(|(_, host)| &target_host == *host)
            .collect::<HashMap<&usize, &Host>>();
        host_indexes.keys().map(|i| **i).collect::<Vec<usize>>()
    }

    pub fn get_host_by_partition(&self, partition: &String) -> Option<&Host> {
        self.reducing.get(partition)
    }

    pub fn clear(&mut self) {
        self.mapping.clear();
        self.partition.clear();
        self.reducing.clear();
    }
}
