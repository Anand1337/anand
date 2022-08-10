use near_metrics::{
    try_create_histogram_vec, try_create_int_counter_vec, HistogramVec, IntCounterVec,
};
use once_cell::sync::Lazy;

pub(crate) static DATABASE_OP_LATENCY_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_database_op_latency_by_op_and_column",
        "Database operations latency by operation and column.",
        &["op", "column"],
        Some(vec![0.00002, 0.0001, 0.0002, 0.0005, 0.0008, 0.001, 0.002, 0.004, 0.008, 0.1]),
    )
    .unwrap()
});

pub static CHUNK_CACHE_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_chunk_cache_hits", "Chunk cache hits", &["shard_id"]).unwrap()
});

pub static CHUNK_CACHE_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_chunk_cache_misses", "Chunk cache misses", &["shard_id"])
        .unwrap()
});

pub static SHARD_CACHE_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_shard_cache_hits", "Shard cache hits", &["shard_id"]).unwrap()
});

pub static SHARD_CACHE_MISSES: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_shard_cache_misses", "Shard cache misses", &["shard_id"])
        .unwrap()
});

pub static SHARD_CACHE_TOO_LARGE: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_shard_cache_too_large", "Shard cache too large", &["shard_id"])
        .unwrap()
});
