//! run_test is a framework for creating and executing runtime scenarios.
//! You can create Scenario in rust code or have it in a JSON file.
//! Scenario::run executes scenario, keeping track of different metrics.
//! So far, the only metric is how much time block production takes.
//! To create scenario in rust code you can use ScenarioBuilder which covers basic scenarios.
//! fuzzing provides Arbitrary trait for Scenario, thus enabling creating random scenarios.
pub mod fuzzing;
pub mod run_test;
pub mod scenario_builder;

pub use crate::run_test::{BlockConfig, NetworkConfig, Scenario, TransactionConfig};
pub use crate::scenario_builder::ScenarioBuilder;

#[test]
// Use this test as a base for creating reproducers.
fn scenario_smoke_test() {
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::transaction::{Action, TransferAction};
    use near_primitives::types::AccountId;

    let num_accounts = 5;

    let seeds: Vec<String> = (0..num_accounts).map(|i| format!("test{}", i)).collect();
    let accounts: Vec<AccountId> = seeds.iter().map(|id| id.parse().unwrap()).collect();

    let mut scenario = Scenario {
        network_config: NetworkConfig { seeds: seeds },
        blocks: Vec::new(),
        use_in_memory_store: true,
    };

    for h in 1..5 {
        let mut block = BlockConfig::at_height(h);
        let transaction = {
            let signer_id = accounts[h as usize].clone();
            let receiver_id = accounts[(h - 1) as usize].clone();
            let signer =
                InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, signer_id.as_ref());

            TransactionConfig {
                nonce: h,
                signer_id,
                receiver_id,
                signer,
                actions: vec![Action::Transfer(TransferAction { deposit: 10 })],
            }
        };
        block.transactions.push(transaction);
        scenario.blocks.push(block)
    }

    scenario.run().result.unwrap();
}

#[test]
fn perf_cliff() {
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::transaction::Action;
    use near_primitives::transaction::DeployContractAction;
    use near_primitives::types::AccountId;

    let code = vec![92; 256];
    let num_accounts = 200_000;
    let block_size = 100;
    let n_blocks = 240;

    let seeds: Vec<String> = (0..num_accounts).map(|i| format!("near_{}_{}", i, i)).collect();
    let accounts: Vec<AccountId> = seeds.iter().map(|id| id.parse().unwrap()).collect();

    let mut s = Scenario {
        network_config: NetworkConfig { seeds: seeds },
        blocks: Vec::new(),
        use_in_memory_store: false,
    };

    let mut i = 0;
    for h in 0..n_blocks {
        let mut block = BlockConfig::at_height((h + 1) as u64);
        for _ in 0..block_size {
            let signer_id = accounts[i].clone();
            let receiver_id = signer_id.clone();
            let signer =
                InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, signer_id.as_ref());
            block.transactions.push(TransactionConfig {
                nonce: i as u64,
                signer_id,
                receiver_id,
                signer,
                actions: vec![Action::DeployContract(DeployContractAction { code: code.clone() })],
            });
            i = (i + 97) % accounts.len();
        }
        s.blocks.push(block)
    }

    tracing_span_tree::span_tree().aggregate(true).enable();
    s.run().result.unwrap();
}
