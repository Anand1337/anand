use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_primitives::borsh::maybestd::sync::Arc;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::Action::FunctionCall;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::{get, DBCol, Store};
use nearcore::NightshadeRuntime;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

fn timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

struct ProgressReporter {
    cnt: AtomicU64,
    // Timestamp to make relative measurements of block processing speed (in ms)
    ts: AtomicU64,
    all: u64,
    skipped: AtomicU64,
    // Fields below get cleared after each print.
    empty_blocks: AtomicU64,
    non_empty_blocks: AtomicU64,
}

impl ProgressReporter {
    pub fn inc_and_report_progress(&self) {
        let ProgressReporter { cnt, ts, all, skipped, empty_blocks, non_empty_blocks } = self;

        const PRINT_PER: u64 = 100;
        let prev = cnt.fetch_add(1, Ordering::Relaxed);
        if (prev + 1) % PRINT_PER == 0 {
            let prev_ts = ts.load(Ordering::Relaxed);
            let new_ts = timestamp_ms();
            let per_second = (PRINT_PER as f64 / (new_ts - prev_ts) as f64) as f64 * 1000.0;
            ts.store(new_ts, Ordering::Relaxed);
            let secs_remaining = (all - prev) as f64 / per_second;

            println!(
                "Processed {} blocks, {:.4} blocks per second ({} skipped), {:.2} secs remaining {} empty blocks",
                prev + 1,
                per_second,
                skipped.load(Ordering::Relaxed),
                secs_remaining,
                empty_blocks.load(Ordering::Relaxed),
            );
            empty_blocks.store(0, Ordering::Relaxed);
            non_empty_blocks.store(0, Ordering::Relaxed);
        }
    }
}

fn add_to_csv(csv_file_mutex: &Mutex<Option<&mut File>>, s: &str) {
    // println!("Adding to csv: {}", s);
    let mut csv_file = csv_file_mutex.lock().unwrap();
    if let Some(csv_file) = csv_file.as_mut() {
        write!(csv_file, "{}\n", s).unwrap();
    }
}

fn extract_history_from_block(
    height: BlockHeight,
    shard_id: ShardId,
    store: Store,
    genesis: &Genesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    progress_reporter: &ProgressReporter,
    verbose_output: bool,
    csv_file_mutex: &Arc<Mutex<Option<&mut File>>>,
    only_contracts: bool,
) {
    // normally save_trie_changes depends on whether the node is
    // archival, but here we don't care, and can just set it to false
    // since we're not writing anything to the store anyway
    let mut chain_store = ChainStore::new(store.clone(), genesis.config.genesis_height, false);
    let block_hash = match chain_store.get_block_hash_by_height(height) {
        Ok(block_hash) => block_hash,
        Err(_) => {
            // Skipping block because it's not available in ChainStore.
            progress_reporter.inc_and_report_progress();
            return;
        }
    };
    let block = chain_store.get_block(&block_hash).unwrap();
    for shard_id in 0..runtime_adapter.num_shards(block.header().epoch_id()).unwrap() {
        // println!("{} : {}", block.header().height(), shard_id);
        if *block.header().prev_hash() == CryptoHash::default() {
            if verbose_output {
                println!("Skipping the genesis block #{}.", height);
            }
        } else if block.chunks()[shard_id as usize].height_included() == height {
            let chunk_hash = block.chunks()[shard_id as usize].chunk_hash();
            let chunk = chain_store.get_chunk(&chunk_hash).unwrap_or_else(|error| {
                panic!(
                    "Can't get chunk on height: {} chunk_hash: {:?} error: {}",
                    height, chunk_hash, error
                );
            });
            for tx in chunk.transactions() {
                for action in &tx.transaction.actions {
                    match action {
                        FunctionCall(action) => {
                            if action.method_name == "ping"
                                || action.method_name == "create_staking_pool"
                            {
                                add_to_csv(
                                    &csv_file_mutex,
                                    &format!(
                                        "{},{},{},{},{}",
                                        block.header().height(),
                                        block.header().timestamp().timestamp(),
                                        tx.transaction.signer_id,
                                        tx.transaction.receiver_id,
                                        action.method_name
                                    ),
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    progress_reporter.inc_and_report_progress();
}

pub fn history(
    store: Store,
    genesis: &Genesis,
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    shard_id: ShardId,
    runtime: NightshadeRuntime,
    verbose_output: bool,
    csv_file: Option<&mut File>,
    only_contracts: bool,
    sequential: bool,
) {
    let parent_span = tracing::debug_span!(
        target: "state_viewer",
        "history",
        ?start_height,
        ?end_height,
        %shard_id,
        only_contracts,
        sequential)
    .entered();
    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(runtime);
    let chain_store = ChainStore::new(store.clone(), genesis.config.genesis_height, false);
    let end_height = end_height.unwrap_or_else(|| chain_store.head().unwrap().height);
    let start_height = start_height.unwrap_or_else(|| chain_store.tail().unwrap());

    println!(
        "Extracting history from chunks in the range {}..={} for shard_id {}",
        start_height, end_height, shard_id
    );

    let csv_file_mutex = Arc::new(Mutex::new(csv_file));
    add_to_csv(&csv_file_mutex, "Height,Timestamp,Signer,Recipient,FunctionName");

    let range = start_height..=end_height;
    let progress_reporter = ProgressReporter {
        cnt: AtomicU64::new(0),
        ts: AtomicU64::new(timestamp_ms()),
        all: end_height - start_height,
        skipped: AtomicU64::new(0),
        empty_blocks: AtomicU64::new(0),
        non_empty_blocks: AtomicU64::new(0),
    };
    let process_height = |height| {
        extract_history_from_block(
            height,
            shard_id,
            store.clone(),
            genesis,
            runtime_adapter.clone(),
            &progress_reporter,
            verbose_output,
            &csv_file_mutex,
            only_contracts,
        );
    };

    if sequential {
        range.into_iter().for_each(|height| {
            let _span = tracing::debug_span!(
                target: "state_viewer",
                parent: &parent_span,
                "process_block_in_order",
                height)
            .entered();
            process_height(height)
        });
    } else {
        range.into_par_iter().for_each(|height| {
            let _span = tracing::debug_span!(
                target: "mock_node",
                parent: &parent_span,
                "process_block_in_parallel",
                height)
            .entered();
            process_height(height)
        });
    }

    println!(
        "No differences found after applying chunks in the range {}..={} for shard_id {}",
        start_height, end_height, shard_id
    );
}
