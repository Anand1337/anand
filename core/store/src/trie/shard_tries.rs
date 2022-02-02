use std::rc::Rc;
use std::sync::{Arc, RwLock};

use near_primitives::borsh::maybestd::collections::HashMap;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout;
use near_primitives::shard_layout::{ShardUId, ShardVersion};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    NumShards, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause, StateRoot,
};

use crate::db::{DBCol, StorageOp, StorageTxInner};
use crate::trie::trie_storage::{TrieCache, TrieCachingStorage};
use crate::trie::{TrieRefcountChange, POISONED_LOCK_ERR};
use crate::{StorageError, Store, StoreUpdate, Trie, TrieChanges, TrieUpdate};

struct ShardTriesInner {
    store: Store,
    /// Cache reserved for client actor to use
    caches: RwLock<HashMap<ShardUId, TrieCache>>,
    /// Cache for readers.
    view_caches: RwLock<HashMap<ShardUId, TrieCache>>,
}

#[derive(Clone)]
pub struct ShardTries(Arc<ShardTriesInner>);

impl ShardTries {
    fn get_new_cache(shards: &[ShardUId]) -> HashMap<ShardUId, TrieCache> {
        shards.iter().map(|&shard_id| (shard_id, TrieCache::new())).collect()
    }

    pub fn new(store: Store, shard_version: ShardVersion, num_shards: NumShards) -> Self {
        assert_ne!(num_shards, 0);
        let shards: Vec<_> = (0..num_shards)
            .map(|shard_id| ShardUId { version: shard_version, shard_id: shard_id as u32 })
            .collect();
        ShardTries(Arc::new(ShardTriesInner {
            store,
            caches: RwLock::new(Self::get_new_cache(&shards)),
            view_caches: RwLock::new(Self::get_new_cache(&shards)),
        }))
    }

    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn new_trie_update(&self, shard_uid: ShardUId, state_root: CryptoHash) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_trie_for_shard(shard_uid)), state_root)
    }

    pub fn new_trie_update_view(&self, shard_uid: ShardUId, state_root: CryptoHash) -> TrieUpdate {
        TrieUpdate::new(Rc::new(self.get_view_trie_for_shard(shard_uid)), state_root)
    }

    fn get_trie_for_shard_internal(&self, shard_uid: ShardUId, is_view: bool) -> Trie {
        let caches_to_use = if is_view { &self.0.view_caches } else { &self.0.caches };
        let cache = {
            let mut caches = caches_to_use.write().expect(POISONED_LOCK_ERR);
            caches.entry(shard_uid).or_insert_with(TrieCache::new).clone()
        };
        let store = Box::new(TrieCachingStorage::new(self.0.store.clone(), cache, shard_uid));
        Trie::new(store, shard_uid)
    }

    pub fn get_trie_for_shard(&self, shard_uid: ShardUId) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, false)
    }

    pub fn get_view_trie_for_shard(&self, shard_uid: ShardUId) -> Trie {
        self.get_trie_for_shard_internal(shard_uid, true)
    }

    pub fn get_store(&self) -> Store {
        self.0.store.clone()
    }

    pub fn update_cache(&self, transaction: &StorageTxInner) -> std::io::Result<()> {
        let mut caches = self.0.caches.write().expect(POISONED_LOCK_ERR);
        let mut shards = HashMap::<_, Vec<(_, Option<&[u8]>)>>::new();
        for op in &transaction.0 {
            match op {
                StorageOp::UpdateRC { col, key, value, add_rc } if *col == DBCol::ColState => {
                    let (shard_uid, hash) =
                        TrieCachingStorage::get_shard_uid_and_hash_from_key(key)?;
                    shards
                        .entry(shard_uid)
                        .or_insert(vec![])
                        .push((hash, if *add_rc < 0 { None } else { Some(value) }));
                }
                StorageOp::Put { col, .. } if *col == DBCol::ColState => unreachable!(),
                StorageOp::Delete { col, .. } if *col == DBCol::ColState => unreachable!(),
                _ => {}
            }
        }
        for (shard_uid, ops) in shards {
            let cache = caches.entry(shard_uid).or_insert_with(TrieCache::new).clone();
            cache.update_cache(&ops);
        }
        Ok(())
    }

    fn apply_deletions_inner(
        deletions: &Vec<TrieRefcountChange>,
        tries: ShardTries,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        store_update.tries = Some(tries);
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            deletions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::ColState,
                key.as_ref(),
                trie_node_or_value,
                -(*rc as i64),
            );
        }
        Ok(())
    }

    fn apply_insertions_inner(
        insertions: &Vec<TrieRefcountChange>,
        tries: ShardTries,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        store_update.tries = Some(tries);
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            insertions.iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::ColState,
                key.as_ref(),
                trie_node_or_value,
                *rc as i64,
            );
        }
        Ok(())
    }

    fn apply_all_inner(
        trie_changes: &TrieChanges,
        tries: ShardTries,
        shard_uid: ShardUId,
        apply_deletions: bool,
    ) -> Result<(StoreUpdate, StateRoot), StorageError> {
        let mut store_update = StoreUpdate::new_with_tries(tries.clone());
        ShardTries::apply_insertions_inner(
            &trie_changes.insertions,
            tries.clone(),
            shard_uid,
            &mut store_update,
        )?;
        if apply_deletions {
            ShardTries::apply_deletions_inner(
                &trie_changes.deletions,
                tries,
                shard_uid,
                &mut store_update,
            )?;
        }
        Ok((store_update, trie_changes.new_root))
    }

    pub fn apply_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        ShardTries::apply_insertions_inner(
            &trie_changes.insertions,
            self.clone(),
            shard_uid,
            store_update,
        )
    }

    pub fn apply_deletions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        ShardTries::apply_deletions_inner(
            &trie_changes.deletions,
            self.clone(),
            shard_uid,
            store_update,
        )
    }

    pub fn revert_insertions(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
        store_update: &mut StoreUpdate,
    ) -> Result<(), StorageError> {
        ShardTries::apply_deletions_inner(
            &trie_changes.insertions,
            self.clone(),
            shard_uid,
            store_update,
        )
    }

    pub fn apply_all(
        &self,
        trie_changes: &TrieChanges,
        shard_uid: ShardUId,
    ) -> Result<(StoreUpdate, StateRoot), StorageError> {
        ShardTries::apply_all_inner(trie_changes, self.clone(), shard_uid, true)
    }

    // apply_all with less memory overhead
    pub fn apply_genesis(
        &self,
        trie_changes: TrieChanges,
        shard_uid: ShardUId,
    ) -> (StoreUpdate, StateRoot) {
        assert_eq!(trie_changes.old_root, CryptoHash::default());
        assert!(trie_changes.deletions.is_empty());
        // Not new_with_tries on purpose
        let mut store_update = self.get_store().store_update();
        for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
            trie_changes.insertions.into_iter()
        {
            let key = TrieCachingStorage::get_key_from_shard_uid_and_hash(
                shard_uid,
                &trie_node_or_value_hash,
            );
            store_update.update_refcount(
                DBCol::ColState,
                key.as_ref(),
                &trie_node_or_value,
                rc as i64,
            );
        }
        (store_update, trie_changes.new_root)
    }
}

pub struct WrappedTrieChanges {
    tries: ShardTries,
    shard_uid: ShardUId,
    trie_changes: TrieChanges,
    state_changes: Vec<RawStateChangesWithTrieKey>,
    block_hash: CryptoHash,
}

impl WrappedTrieChanges {
    pub fn new(
        tries: ShardTries,
        shard_uid: ShardUId,
        trie_changes: TrieChanges,
        state_changes: Vec<RawStateChangesWithTrieKey>,
        block_hash: CryptoHash,
    ) -> Self {
        WrappedTrieChanges { tries, shard_uid, trie_changes, state_changes, block_hash }
    }

    pub fn state_changes(&self) -> &[RawStateChangesWithTrieKey] {
        &self.state_changes
    }

    pub fn insertions_into(&self, store_update: &mut StoreUpdate) -> Result<(), StorageError> {
        self.tries.apply_insertions(&self.trie_changes, self.shard_uid, store_update)
    }

    /// Save state changes into Store.
    ///
    /// NOTE: the changes are drained from `self`.
    pub fn state_changes_into(&mut self, store_update: &mut StoreUpdate) {
        let mut changes = Vec::new();
        for change_with_trie_key in self.state_changes.drain(..) {
            assert!(
                !change_with_trie_key.changes.iter().any(|RawStateChange { cause, .. }| matches!(
                    cause,
                    StateChangeCause::NotWritableToDisk
                )),
                "NotWritableToDisk changes must never be finalized."
            );

            assert!(
                !change_with_trie_key.changes.iter().any(|RawStateChange { cause, .. }| matches!(
                    cause,
                    StateChangeCause::Resharding
                )),
                "Resharding changes must never be finalized."
            );

            // Filtering trie keys for user facing RPC reporting.
            // NOTE: If the trie key is not one of the account specific, it may cause key conflict
            // when the node tracks multiple shards. See #2563.
            match &change_with_trie_key.trie_key {
                TrieKey::Account { .. }
                | TrieKey::ContractCode { .. }
                | TrieKey::AccessKey { .. }
                | TrieKey::ContractData { .. } => {}
                _ => continue,
            };

            changes.push(change_with_trie_key);
        }
        store_update.set_ser(DBCol::ColStateChanges, self.block_hash.as_ref(), &changes).unwrap();
    }

    pub fn wrapped_into(
        &mut self,
        store_update: &mut StoreUpdate,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.insertions_into(store_update)?;
        self.state_changes_into(store_update);
        store_update.set_ser(
            DBCol::ColTrieChanges,
            &shard_layout::get_block_shard_uid(&self.block_hash, &self.shard_uid),
            &self.trie_changes,
        )?;
        Ok(())
    }
}

pub fn get_state_changes(
    store: &Store,
    block_hash: &CryptoHash,
) -> Result<Vec<RawStateChangesWithTrieKey>, std::io::Error> {
    store
        .get_ser(DBCol::ColStateChanges, block_hash.as_ref())
        .map(|changes| changes.unwrap_or(Vec::new()))
}

pub fn get_state_changes_with_prefix(
    store: &Store,
    block_hash: &CryptoHash,
    prefix: &[u8],
) -> Result<Vec<RawStateChangesWithTrieKey>, std::io::Error> {
    get_state_changes(store, block_hash).map(|changes| {
        changes.into_iter().filter(|change| change.trie_key.to_vec().starts_with(prefix)).collect()
    })
}

pub fn get_state_changes_exact(
    store: &Store,
    block_hash: &CryptoHash,
    key: &[u8],
) -> Result<Vec<RawStateChangesWithTrieKey>, std::io::Error> {
    get_state_changes(store, block_hash).map(|changes| {
        changes.into_iter().filter(|change| change.trie_key.to_vec() == key).collect()
    })
}
