//! End-to-end distributed fault-tolerance tests: drive `handle_promote` across a
//! multi-node in-memory cluster and kill nodes at different points to exercise
//! the leader's failure handling (and, in part E, the reduce worker's download
//! resilience).

mod common;

#[path = "distributed/promote.rs"]
mod promote;

#[path = "distributed/reduce.rs"]
mod reduce;
