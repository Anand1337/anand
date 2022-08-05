use near_chain::{ChainStore, near_chain_primitives, RuntimeAdapter};
use near_chain::ChainStoreAccess;
use near_primitives::account::id::AccountId;
use near_primitives::block::Block;
use near_primitives::transaction::{SignedTransaction, ExecutionOutcomeWithIdAndProof};
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;

/// Returns a list of transactions found in the block.
pub fn dump_tx_from_block(
    chain_store: &ChainStore,
    block: &Block,
    select_account_ids: Option<&Vec<AccountId>>,
) -> Vec<SignedTransaction> {
    let chunks = block.chunks();
    let mut res = vec![];
    for (_, chunk_header) in chunks.iter().enumerate() {
        res.extend(
            chain_store
                .get_chunk(&chunk_header.chunk_hash())
                .unwrap()
                .transactions()
                .into_iter()
                .filter(|signed_transaction| {
                    should_include_signed_transaction(signed_transaction, select_account_ids)
                })
                .map(|signed_transaction| signed_transaction.clone())
                .collect::<Vec<_>>(),
        );
    }
    return res;
}

fn should_include_signed_transaction(
    signed_transaction: &SignedTransaction,
    select_account_ids: Option<&Vec<AccountId>>,
) -> bool {
    match select_account_ids {
        None => true,
        Some(specified_ids) => specified_ids.contains(&signed_transaction.transaction.receiver_id),
    }
}

pub fn dump_tx_info(
    _runtime_adapter: &dyn RuntimeAdapter,
    chain_store: &ChainStore,
    mut block_hash: CryptoHash,
) -> anyhow::Result<()> {
    for _ in 0..100 {
        let block = chain_store.get_block(&block_hash)?;
        let shard_cnt = block.chunks().len();
        let outcomes = get_tx_outcomes_by_block_hash(chain_store, &block_hash, shard_cnt);
        tracing::info!("Outcomes: {:?}", outcomes);
        if let Ok(next_hash) = chain_store.get_next_block_hash(&block_hash) {
            block_hash = next_hash;
        } else {
            break;
        }
    }
    Ok(())
}

fn get_tx_outcomes_by_block_hash(
    chain_store: &ChainStore,
    block_hash: &CryptoHash,
    shard_cnt: usize,
) -> Result<Vec<Vec<Vec<ExecutionOutcomeWithIdAndProof>>>, near_chain_primitives::error::Error> {
    let mut all_outcomes = vec![];
    for shard_id in 0..shard_cnt {
        let outcome_ids = chain_store.get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id as ShardId).unwrap();
        let mut outcomes = vec![];
        for id in outcome_ids {
            outcomes.push(chain_store.get_outcomes_by_id(&id).unwrap());
        }
        all_outcomes.push(outcomes);
    }
    return Ok(all_outcomes)
}
