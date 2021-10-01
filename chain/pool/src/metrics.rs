use near_metrics::{
    try_create_histogram, try_create_int_counter, try_create_int_gauge, Histogram, IntCounter,
    IntGauge,
};

lazy_static! {
    pub static ref TRANSACTION_POOL_INSERTIONS : near_metrics::Result<IntCounter> =
        try_create_int_counter("near_transaction_pools_insertions_total", "Total number of transactions added to the pools tracked by this instance");
    pub static ref TRANSACTION_POOL_REMOVALS : near_metrics::Result<IntCounter> =
        try_create_int_counter("near_transaction_pools_removals_total", "Total number of transactions removed from the pools tracked by this instance");
}
