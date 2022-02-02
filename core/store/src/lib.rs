use std::io;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use lru::LruCache;

pub use db::{
    DBCol, CHUNK_TAIL_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY, HEADER_HEAD_KEY, HEAD_KEY,
    LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, NUM_COLS, SHOULD_COL_GC, SKIP_COL_GC, TAIL_KEY,
};
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
pub use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt, ReceivedData};
pub use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::{AccountId, CompiledContractCache, StateRoot};

use crate::db::{StorageInner, StorageTxInner, GENESIS_JSON_HASH_KEY, GENESIS_STATE_ROOTS_KEY};
pub use crate::trie::iterator::TrieIterator;
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator, TrieUpdateValuePtr};
pub use crate::trie::{
    get_state_changes, get_state_changes_exact, get_state_changes_with_prefix, split_state,
    ApplyStatePartResult, PartialStorage, ShardTries, Trie, TrieChanges, WrappedTrieChanges,
};

mod db;
mod trie;

#[derive(Clone)]
pub struct Store(Arc<StorageInner>);

impl Store {
    pub fn get(&self, column: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.0.get(column, key)
    }

    pub fn get_ser<T: BorshDeserialize>(&self, column: DBCol, key: &[u8]) -> io::Result<Option<T>> {
        self.0
            .get(column, key)
            .map(|v| v.map(|v| T::try_from_slice(&v)).transpose())
            .and_then(|v| v)
    }

    pub fn exists(&self, column: DBCol, key: &[u8]) -> io::Result<bool> {
        self.0.get(column, key).map(|v| v.is_some())
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate { storage: self.0.clone(), transaction: StorageTxInner::new(), tries: None }
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    storage: Arc<StorageInner>,
    transaction: StorageTxInner,
    /// Optionally has reference to the trie to clear cache on the commit.
    tries: Option<ShardTries>,
}

impl StoreUpdate {
    pub fn new_with_tries(tries: ShardTries) -> Self {
        StoreUpdate {
            storage: tries.get_store().0.clone(),
            transaction: StorageTxInner::new(),
            tries: Some(tries),
        }
    }

    pub fn set(&mut self, column: DBCol, key: &[u8], value: &[u8]) {
        self.transaction.put(column, key.to_vec(), value.to_vec());
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
        value: &T,
    ) -> io::Result<()> {
        self.transaction.put(column, key.to_vec(), value.try_to_vec()?);
        Ok(())
    }

    pub fn delete(&mut self, column: DBCol, key: &[u8]) {
        self.transaction.delete(column, key.to_vec());
    }

    pub fn update_refcount(&mut self, column: DBCol, key: &[u8], value: &[u8], rc_delta: i64) {
        self.transaction.update_rc(column, key.to_vec(), value.to_vec(), rc_delta);
    }

    /// Merge another store update into this one.
    pub fn merge(&mut self, other: StoreUpdate) {
        if let Some(tries) = other.tries {
            if self.tries.is_none() {
                self.tries = Some(tries);
            } else {
                debug_assert!(self.tries.as_ref().unwrap().is_same(&tries));
            }
        }
        self.merge_transaction(other.transaction);
    }

    /// Merge DB Transaction.
    fn merge_transaction(&mut self, transaction: StorageTxInner) {
        self.transaction.merge(transaction);
    }

    pub fn commit(self) -> io::Result<()> {
        if let Some(tries) = self.tries {
            assert_eq!(tries.get_store().0.deref() as *const _, self.storage.deref() as *const _);
            tries.update_cache(&self.transaction)?;
        }
        self.storage.write(self.transaction)
    }
}

pub fn read_with_cache<'a, T: BorshDeserialize + 'a>(
    storage: &Store,
    col: DBCol,
    cache: &'a mut LruCache<Vec<u8>, T>,
    key: &[u8],
) -> io::Result<Option<&'a T>> {
    let key_vec = key.to_vec();
    if cache.get(&key_vec).is_some() {
        return Ok(cache.get(&key_vec));
    }
    if let Some(result) = storage.get_ser(col, key)? {
        cache.put(key.to_vec(), result);
        return Ok(cache.get(&key_vec));
    }
    Ok(None)
}

pub fn create_store(path: &Path) -> Store {
    Store(Arc::new(StorageInner::new(path).unwrap()))
}

/// Reads an object from Trie.
/// # Errors
/// see StorageError
pub fn get<T: BorshDeserialize>(
    state_update: &TrieUpdate,
    key: &TrieKey,
) -> Result<Option<T>, StorageError> {
    state_update.get(key).and_then(|opt| {
        opt.map_or_else(
            || Ok(None),
            |data| {
                T::try_from_slice(&data)
                    .map_err(|_| {
                        StorageError::StorageInconsistentState("Failed to deserialize".to_string())
                    })
                    .map(Some)
            },
        )
    })
}

/// Writes an object into Trie.
pub fn set<T: BorshSerialize>(state_update: &mut TrieUpdate, key: TrieKey, value: &T) {
    let data = value.try_to_vec().expect("Borsh serializer is not expected to ever fail");
    state_update.set(key, data);
}

pub fn set_account(state_update: &mut TrieUpdate, account_id: AccountId, account: &Account) {
    set(state_update, TrieKey::Account { account_id }, account)
}

pub fn get_account(
    state_update: &TrieUpdate,
    account_id: &AccountId,
) -> Result<Option<Account>, StorageError> {
    get(state_update, &TrieKey::Account { account_id: account_id.clone() })
}

pub fn set_received_data(
    state_update: &mut TrieUpdate,
    receiver_id: AccountId,
    data_id: CryptoHash,
    data: &ReceivedData,
) {
    set(state_update, TrieKey::ReceivedData { receiver_id, data_id }, data);
}

pub fn get_received_data(
    state_update: &TrieUpdate,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<Option<ReceivedData>, StorageError> {
    get(state_update, &TrieKey::ReceivedData { receiver_id: receiver_id.clone(), data_id })
}

pub fn set_postponed_receipt(state_update: &mut TrieUpdate, receipt: &Receipt) {
    let key = TrieKey::PostponedReceipt {
        receiver_id: receipt.receiver_id.clone(),
        receipt_id: receipt.receipt_id,
    };
    set(state_update, key, receipt);
}

pub fn remove_postponed_receipt(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    receipt_id: CryptoHash,
) {
    state_update.remove(TrieKey::PostponedReceipt { receiver_id: receiver_id.clone(), receipt_id });
}

pub fn get_postponed_receipt(
    state_update: &TrieUpdate,
    receiver_id: &AccountId,
    receipt_id: CryptoHash,
) -> Result<Option<Receipt>, StorageError> {
    get(state_update, &TrieKey::PostponedReceipt { receiver_id: receiver_id.clone(), receipt_id })
}

pub fn get_delayed_receipt_indices(
    state_update: &TrieUpdate,
) -> Result<DelayedReceiptIndices, StorageError> {
    Ok(get(state_update, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default())
}

pub fn set_access_key(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
    access_key: &AccessKey,
) {
    set(state_update, TrieKey::AccessKey { account_id, public_key }, access_key);
}

pub fn remove_access_key(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
) {
    state_update.remove(TrieKey::AccessKey { account_id, public_key });
}

pub fn get_access_key(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Result<Option<AccessKey>, StorageError> {
    get(
        state_update,
        &TrieKey::AccessKey { account_id: account_id.clone(), public_key: public_key.clone() },
    )
}

pub fn get_access_key_raw(
    state_update: &TrieUpdate,
    raw_key: &[u8],
) -> Result<Option<AccessKey>, StorageError> {
    get(
        state_update,
        &trie_key_parsers::parse_trie_key_access_key_from_raw_key(raw_key)
            .expect("access key in the state should be correct"),
    )
}

pub fn set_code(state_update: &mut TrieUpdate, account_id: AccountId, code: &ContractCode) {
    state_update.set(TrieKey::ContractCode { account_id }, code.code().to_vec());
}

pub fn get_code(
    state_update: &TrieUpdate,
    account_id: &AccountId,
    code_hash: Option<CryptoHash>,
) -> Result<Option<ContractCode>, StorageError> {
    state_update
        .get(&TrieKey::ContractCode { account_id: account_id.clone() })
        .map(|opt| opt.map(|code| ContractCode::new(code, code_hash)))
}

/// Removes account, code and all access keys associated to it.
pub fn remove_account(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
) -> Result<(), StorageError> {
    state_update.remove(TrieKey::Account { account_id: account_id.clone() });
    state_update.remove(TrieKey::ContractCode { account_id: account_id.clone() });

    // Removing access keys
    let public_keys = state_update
        .iter(&trie_key_parsers::get_raw_prefix_for_access_keys(account_id))?
        .map(|raw_key| {
            trie_key_parsers::parse_public_key_from_access_key_key(&raw_key?, account_id).map_err(
                |_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse public key from raw key for AccessKey".to_string(),
                    )
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for public_key in public_keys {
        state_update.remove(TrieKey::AccessKey { account_id: account_id.clone(), public_key });
    }

    // Removing contract data
    let data_keys = state_update
        .iter(&trie_key_parsers::get_raw_prefix_for_contract_data(account_id, &[]))?
        .map(|raw_key| {
            trie_key_parsers::parse_data_key_from_contract_data_key(&raw_key?, account_id)
                .map_err(|_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse data key from raw key for ContractData".to_string(),
                    )
                })
                .map(Vec::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in data_keys {
        state_update.remove(TrieKey::ContractData { account_id: account_id.clone(), key });
    }
    Ok(())
}

pub fn get_genesis_state_roots(store: &Store) -> Result<Option<Vec<StateRoot>>, std::io::Error> {
    store.get_ser::<Vec<StateRoot>>(DBCol::ColBlockMisc, GENESIS_STATE_ROOTS_KEY)
}

pub fn get_genesis_hash(store: &Store) -> Result<Option<CryptoHash>, std::io::Error> {
    store.get_ser::<CryptoHash>(DBCol::ColBlockMisc, GENESIS_JSON_HASH_KEY)
}

pub fn set_genesis_hash(store_update: &mut StoreUpdate, genesis_hash: &CryptoHash) {
    store_update
        .set_ser::<CryptoHash>(DBCol::ColBlockMisc, GENESIS_JSON_HASH_KEY, genesis_hash)
        .expect("Borsh cannot fail");
}

pub fn set_genesis_state_roots(store_update: &mut StoreUpdate, genesis_roots: &Vec<StateRoot>) {
    store_update
        .set_ser::<Vec<StateRoot>>(DBCol::ColBlockMisc, GENESIS_STATE_ROOTS_KEY, genesis_roots)
        .expect("Borsh cannot fail");
}

pub struct StoreCompiledContractCache {
    pub store: Store,
}

/// Cache for compiled contracts code using Store for keeping data.
/// We store contracts in VM-specific format in DBCol::ColCachedContractCode.
/// Key must take into account VM being used and its configuration, so that
/// we don't cache non-gas metered binaries, for example.
impl CompiledContractCache for StoreCompiledContractCache {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error> {
        let mut store_update = self.store.store_update();
        store_update.set(DBCol::ColCachedContractCode, key, value);
        store_update.commit()
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        self.store.get(DBCol::ColCachedContractCode, key)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_no_cache_disabled() {
        #[cfg(feature = "no_cache")]
        panic!("no cache is enabled");
    }
}
