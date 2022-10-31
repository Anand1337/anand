use crate::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use assert_matches::assert_matches;
use borsh::BorshSerialize;
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_chain_primitives::Error;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::state::ValueRef;
use near_primitives::types::{BlockHeight, NumShards, ShardId};
use near_store::flat_state::store_helper;
use near_store::migrations::BatchedStoreUpdate;
use near_store::{DBCol, FlatStateDelta, Trie, TrieTraversalItem};
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

const NUM_PARTS: u64 = 4_000;
const PART_STEP: u64 = 50;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationStatus {
    SavingDeltas,
    // contains hash of flat storage head to fetch and number of current part
    FetchingState((CryptoHash, u64)),
    // should contain current FS head?
    CatchingUp,
    // is it needed?
    Finished,
}

pub struct FlatStorageShardMigrator {
    pub status: MigrationStatus,
    pub shard_id: ShardId,
    pub finished_state_parts: Option<u64>,
    pub traverse_trie_sender: Sender<u64>,
    pub traverse_trie_receiver: Receiver<u64>,
    pub visited_items: u64,
}

impl FlatStorageShardMigrator {
    pub fn new(shard_id: ShardId) -> Self {
        let (traverse_trie_sender, traverse_trie_receiver) = unbounded();
        Self {
            status: MigrationStatus::SavingDeltas,
            shard_id,
            finished_state_parts: None,
            traverse_trie_sender,
            traverse_trie_receiver,
            visited_items: 0,
        }
    }
}

pub struct FlatStorageMigrator {
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub shard_migrator: Vec<Arc<Mutex<FlatStorageShardMigrator>>>,
    pub starting_height: BlockHeight,
    pub pool: rayon::ThreadPool,
}

impl FlatStorageMigrator {
    pub fn new(
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        num_shards: NumShards,
        starting_height: BlockHeight,
    ) -> Self {
        Self {
            runtime_adapter,
            shard_migrator: (0..num_shards)
                .map(|shard_id| Arc::new(Mutex::new(FlatStorageShardMigrator::new(shard_id))))
                .collect(),
            starting_height,
            pool: rayon::ThreadPoolBuilder::new().num_threads(PART_STEP as usize).build().unwrap(),
        }
    }

    pub fn get_status(&self, shard_id: ShardId) -> MigrationStatus {
        let guard = self.shard_migrator[shard_id as usize].lock().unwrap();
        guard.status.clone()
    }

    pub fn update_status(
        &self,
        shard_id: ShardId,
        final_head: Tip,
        chain_store: &ChainStore,
    ) -> Result<(), Error> {
        let mut guard = self.shard_migrator[shard_id as usize].lock().unwrap();

        match guard.status.clone() {
            MigrationStatus::SavingDeltas => {
                // migrate only shard 0
                if self.starting_height < final_head.height && shard_id == 0 {
                    // it means that we saved all deltas. spawn threads
                    guard.status = MigrationStatus::FetchingState((final_head.last_block_hash, 0));
                    guard.finished_state_parts = None;

                    // check deltas existence
                    for height in final_head.height + 1..=chain_store.head().unwrap().height {
                        for (_, hashes) in
                            chain_store.get_all_block_hashes_by_height(height)?.iter()
                        {
                            for hash in hashes {
                                info!(target: "chain", %shard_id, %height, %hash, "Checking delta existence");
                                assert_matches!(
                                    store_helper::get_delta(
                                        chain_store.store(),
                                        shard_id,
                                        hash.clone(),
                                    ),
                                    Ok(Some(_))
                                );
                            }
                        }
                    }
                }
            }
            MigrationStatus::FetchingState((block_hash, start_part_id)) => {
                match &guard.finished_state_parts {
                    None => {
                        info!(target: "chain", %shard_id, %block_hash, %start_part_id, "Spawning threads");
                        let start_part_id = start_part_id.clone();
                        let epoch_id = self.runtime_adapter.get_epoch_id(&block_hash)?;
                        let shard_uid =
                            self.runtime_adapter.shard_id_to_uid(shard_id, &epoch_id)?;
                        let state_root = chain_store
                            .get_chunk_extra(&block_hash, &shard_uid)?
                            .state_root()
                            .clone();
                        let trie = self.runtime_adapter.get_view_trie_for_shard(
                            shard_id,
                            &block_hash,
                            state_root,
                        )?;
                        let root_node = trie.retrieve_root_node().unwrap();
                        let memory_usage = root_node.memory_usage;
                        let part_progress = Arc::new(std::sync::atomic::AtomicU64::new(0));

                        for part_id in start_part_id..start_part_id + PART_STEP {
                            let path_begin =
                                trie.find_path_for_part_boundary(part_id, NUM_PARTS)?;
                            let path_end =
                                trie.find_path_for_part_boundary(part_id + 1, NUM_PARTS)?;

                            let trie_storage = trie
                                .storage
                                .as_caching_storage()
                                .expect("preload called without caching storage")
                                .clone();
                            let root = state_root.clone();
                            let store = self.runtime_adapter.store().clone();
                            let inner_part_progress = part_progress.clone();
                            let inner_sender = guard.traverse_trie_sender.clone();

                            self.pool.spawn(move || {
                                let path_prefix = match path_begin.last() {
                                    Some(16) => &path_begin[..path_begin.len() - 1],
                                    _ => &path_begin,
                                };
                                let hex_prefix: String = path_prefix
                                    .iter()
                                    .map(|&n| {
                                        char::from_digit(n as u32, 16)
                                            .expect("nibble should be <16")
                                    })
                                    .collect();
                                debug!(target: "store", "Preload state part from {hex_prefix}");
                                let trie = Trie::new(Box::new(trie_storage), root, None);
                                let mut trie_iter = trie.iter().unwrap();

                                let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
                                let mut n = 0;

                                for TrieTraversalItem { hash, key } in
                                    trie_iter.visit_nodes_interval(&path_begin, &path_end).unwrap()
                                {
                                    match key {
                                        None => {}
                                        Some(key) => {
                                            let value =
                                                trie.storage.retrieve_raw_bytes(&hash).unwrap();
                                            let value_ref = ValueRef::new(&value);
                                            #[cfg(feature = "protocol_feature_flat_state")]
                                            store_update
                                                .set_ser(DBCol::FlatState, &key, &value_ref)
                                                .expect("Failed to put value in FlatState");
                                            n += 1;
                                        }
                                    }
                                }
                                store_update.finish().unwrap();
                                inner_part_progress
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let part_progress =
                                    inner_part_progress.load(std::sync::atomic::Ordering::Relaxed);

                                // visited {nodes_count} nodes, \
                                // {threads_usage} threads used, \
                                // {first_record_to_display} is the first state record, \

                                debug!(target: "store",
                                    "Preload subtrie at {hex_prefix} done, \
                                    loaded {n:<8} state items, \
                                    parts proccessed: {part_progress} / {PART_STEP}"
                                );

                                inner_sender.send(n).unwrap();
                            })
                        }

                        guard.finished_state_parts = Some(0);
                    }
                    Some(x) if *x == PART_STEP => {
                        guard.finished_state_parts = None;
                        let new_start_part_id = start_part_id + PART_STEP;
                        guard.status = if new_start_part_id == NUM_PARTS {
                            info!(target: "chain", %shard_id, %block_hash, %new_start_part_id, "Finished fetching state");
                            let mut store_update = self.runtime_adapter.store().store_update();
                            store_helper::set_flat_head(&mut store_update, shard_id, &block_hash);
                            store_update.commit().unwrap();
                            MigrationStatus::CatchingUp
                        } else {
                            info!(target: "chain", %shard_id, %block_hash, %new_start_part_id, "Moving part id");
                            MigrationStatus::FetchingState((block_hash.clone(), new_start_part_id))
                        };
                    }
                    Some(_) => {
                        while let Ok(n) = guard.traverse_trie_receiver.try_recv() {
                            guard.finished_state_parts =
                                Some(guard.finished_state_parts.unwrap() + 1);
                            guard.visited_items += n;
                        }
                    }
                }
            }
            MigrationStatus::CatchingUp => {
                let store = self.runtime_adapter.store();
                let old_flat_head = store_helper::get_flat_head(store, shard_id).unwrap();
                let mut flat_head = old_flat_head.clone();
                let mut merged_delta = FlatStateDelta::default();
                for _ in 0..50 {
                    let height = chain_store.get_block_height(&flat_head).unwrap();
                    if height > final_head.height {
                        panic!("New flat head moved too far: new head = {flat_head}, height = {height}, final block height = {}", final_head.height);
                    }
                    if height == final_head.height {
                        break;
                    }
                    flat_head = chain_store.get_next_block_hash(&flat_head).unwrap();
                    let delta =
                        store_helper::get_delta(store, shard_id, flat_head).unwrap().unwrap();
                    // debug. don't merge > 10 deltas in prod
                    merged_delta.merge(delta.as_ref());
                }

                if old_flat_head != flat_head {
                    let old_height = chain_store.get_block_height(&old_flat_head).unwrap();
                    let height = chain_store.get_block_height(&flat_head).unwrap();
                    info!(target: "chain", %shard_id, %old_flat_head, %old_height, %flat_head, %height, "Catching up flat head");
                    let mut store_update = self.runtime_adapter.store().store_update();
                    store_helper::set_flat_head(&mut store_update, shard_id, &flat_head);
                    merged_delta.apply_to_flat_state(&mut store_update);
                    store_update.commit().unwrap();
                }
            }
            _ => {}
        }

        Ok(())
    }
}
