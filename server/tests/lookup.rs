//! Unit tests for [`JobLookup`], the leader's record of where map jobs ran,
//! which indices feed each partition, and where reduce jobs were placed. The
//! fault-tolerance behaviour worth pinning down is how a job *failure* mutates
//! this state -- in particular that losing a map job forgets only its location,
//! never the fact that it feeds a partition.

use std::collections::HashSet;

use mapreduce::{cluster::Host, job_lookup::JobLookup, map::MapResponse};

fn host(port: u16) -> Host {
    Host {
        domain: "[::1]".to_owned(),
        port,
    }
}

fn complete(index: usize, partitions: &[&str]) -> MapResponse {
    MapResponse {
        index,
        seen_partitions: partitions.iter().map(|p| (*p).to_owned()).collect(),
    }
}

#[test]
fn records_and_resolves_map_job_location() {
    let mut lookup = JobLookup::new();
    lookup.create_map_job(0, host(1));

    assert_eq!(lookup.try_get_host_by_index(0), Some(&host(1)));
    assert_eq!(lookup.get_host_by_index(0), &host(1));
}

#[test]
fn unknown_index_has_no_location() {
    let lookup = JobLookup::new();
    assert!(lookup.try_get_host_by_index(0).is_none());
}

#[test]
fn complete_map_job_accumulates_indices_per_partition() {
    let mut lookup = JobLookup::new();
    lookup.complete_map_job(complete(0, &["odd", "even"]));
    lookup.complete_map_job(complete(1, &["odd"]));

    assert!(lookup.is_map_job_complete(&"odd".to_owned(), 0));
    assert!(lookup.is_map_job_complete(&"odd".to_owned(), 1));
    assert!(lookup.is_map_job_complete(&"even".to_owned(), 0));
    // index 1 never produced "even"
    assert!(!lookup.is_map_job_complete(&"even".to_owned(), 1));

    assert_eq!(
        lookup.get_indices_for_partition(&"odd".to_owned()),
        &HashSet::from([0, 1])
    );
}

#[test]
fn is_map_job_complete_false_for_unknown_partition() {
    let lookup = JobLookup::new();
    assert!(!lookup.is_map_job_complete(&"odd".to_owned(), 0));
}

/// The key fault-tolerance invariant: a map job failure forgets where the index
/// ran, but leaves the partition membership intact, so a reduce reassigned in
/// the same failure pass still sees the full index set (and `complete_map_job`
/// re-inserts the location idempotently once recomputed).
#[test]
fn signal_map_job_failure_forgets_location_but_keeps_partition_membership() {
    let mut lookup = JobLookup::new();
    lookup.create_map_job(0, host(1));
    lookup.complete_map_job(complete(0, &["odd"]));

    lookup.signal_map_job_failure(0);

    // The location is gone...
    assert!(lookup.try_get_host_by_index(0).is_none());
    // ...but the partition still knows index 0 feeds it.
    assert!(lookup.is_map_job_complete(&"odd".to_owned(), 0));
    assert!(
        lookup
            .get_indices_for_partition(&"odd".to_owned())
            .contains(&0)
    );
}

#[test]
fn create_map_job_overwrites_location_on_reassignment() {
    let mut lookup = JobLookup::new();
    lookup.create_map_job(0, host(1));
    lookup.signal_map_job_failure(0);
    lookup.create_map_job(0, host(2));

    assert_eq!(lookup.try_get_host_by_index(0), Some(&host(2)));
}

#[test]
fn get_map_job_indices_filters_by_host() {
    let mut lookup = JobLookup::new();
    lookup.create_map_job(0, host(1));
    lookup.create_map_job(1, host(1));
    lookup.create_map_job(2, host(2));

    let mut on_host_1 = lookup.get_map_job_indices(host(1));
    on_host_1.sort();
    assert_eq!(on_host_1, vec![0, 1]);
    assert_eq!(lookup.get_map_job_indices(host(2)), vec![2]);
    assert!(lookup.get_map_job_indices(host(3)).is_empty());
}

#[test]
fn reduce_job_lifecycle() {
    let mut lookup = JobLookup::new();
    assert!(lookup.get_host_by_partition(&"odd".to_owned()).is_none());

    lookup.create_reduce_job("odd".to_owned(), host(1));
    assert_eq!(
        lookup.get_host_by_partition(&"odd".to_owned()),
        Some(&host(1))
    );

    lookup.signal_reduce_job_failure(&"odd".to_owned());
    assert!(lookup.get_host_by_partition(&"odd".to_owned()).is_none());
}

#[test]
fn get_reduce_job_partitions_filters_by_host() {
    let mut lookup = JobLookup::new();
    lookup.create_reduce_job("odd".to_owned(), host(1));
    lookup.create_reduce_job("even".to_owned(), host(2));

    assert_eq!(
        lookup.get_reduce_job_partitions(host(1)),
        vec![&"odd".to_owned()]
    );
    assert!(lookup.get_reduce_job_partitions(host(3)).is_empty());
}

#[test]
fn clear_empties_all_state() {
    let mut lookup = JobLookup::new();
    lookup.create_map_job(0, host(1));
    lookup.complete_map_job(complete(0, &["odd"]));
    lookup.create_reduce_job("odd".to_owned(), host(1));

    lookup.clear();

    assert!(lookup.try_get_host_by_index(0).is_none());
    assert!(!lookup.is_map_job_complete(&"odd".to_owned(), 0));
    assert!(lookup.get_host_by_partition(&"odd".to_owned()).is_none());
}
