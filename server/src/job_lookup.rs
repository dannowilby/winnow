use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::{cluster::Host, map::MapResponse};

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Progress {
    pub total_map_jobs: usize,
    pub completed_map_jobs: usize,

    pub total_reduce_jobs: usize,
    pub completed_reduce_jobs: usize,

    pub primed: bool,
}

#[derive(Default)]
pub struct JobLookup {
    /// Maps input index to the host it was computed on
    mapping: HashMap<usize, Host>,

    /// Maps the partition to the map jobs it was located on
    pub partition: HashMap<String, HashSet<usize>>,

    /// Maps a host to the partition
    pub reducing: HashMap<String, Host>,

    /// Partitions whose reduce job has finished.
    completed_partitions: HashSet<String>,

    pub progress: Progress,
}

impl JobLookup {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn signal_primed(&mut self) {
        self.progress.primed = true;
    }

    pub fn create_map_job(&mut self, index: usize, host: Host) {
        self.mapping.insert(index, host);
        self.progress.total_map_jobs += 1;
    }

    pub fn signal_map_job_failure(&mut self, index: usize) {
        if self.is_map_job_complete(index) {
            self.progress.completed_map_jobs -= 1;
        }
        self.mapping.remove(&index);
        self.progress.total_map_jobs -= 1;
    }

    /// Records the partitions a finished map job produced on `host`.
    pub fn complete_map_job(&mut self, mr: MapResponse) {
        self.progress.completed_map_jobs += 1;
        for partition in mr.seen_partitions {
            self.partition
                .entry(partition)
                .and_modify(|v| {
                    v.insert(mr.index);
                })
                .or_insert(HashSet::from([mr.index]));
        }
    }

    pub fn is_map_job_complete(&self, index: usize) -> bool {
        let mut seen = false;
        self.partition.iter().for_each(|(_, v)| {
            if v.contains(&index) {
                seen = true;
            }
        });

        seen
    }

    pub fn create_reduce_job(&mut self, partition: String, host: Host) {
        self.reducing.insert(partition, host);
        self.progress.total_reduce_jobs += 1;
    }

    pub fn signal_reduce_job_failure(&mut self, partition: &String) {
        if self.is_reduce_job_complete(partition) {
            self.progress.completed_reduce_jobs -= 1;
            self.completed_partitions.remove(partition);
        }
        self.reducing.remove(partition);
        self.progress.total_reduce_jobs -= 1;
    }

    /// Records that `partition`'s reduce job has finished.
    pub fn complete_reduce_job(&mut self, partition: String) {
        self.progress.completed_reduce_jobs += 1;
        self.completed_partitions.insert(partition);
    }

    pub fn is_reduce_job_complete(&self, partition: &str) -> bool {
        self.completed_partitions.contains(partition)
    }

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
        *self = Self::default();
    }
}
