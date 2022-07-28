use std::path::Path;
use near_store::{Store, StoreConfig, DBCol};
use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;

fn main() {
    let store = Store::opener(&Path::new("/home/edvard/.near2"), &StoreConfig::default()).open();

    let mut a = vec![0; 4];
    for key_value in store.iter(DBCol::TransactionResult) {
        if key_value.is_err() {
            continue;
        }
        let (key, value) = key_value.unwrap();
        let hash = CryptoHash::try_from_slice(&key).unwrap();
        let execution_outcomes = Vec::<ExecutionOutcomeWithIdAndProof>::try_from_slice(&value).unwrap();
        a[std::cmp::min(execution_outcomes.len(), 3)] += 1;
    }
    println!("{:?}", a);
}
