use assert_matches::assert_matches;
use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_o11y::testonly::init_test_logger;
use near_primitives_core::types::{BlockHeight, NumShards};
use near_store::flat_state::{
    store_helper, FetchingStateStatus, FlatStorageStateStatus, NUM_PARTS_IN_ONE_STEP,
};
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Check correctness of flat storage creation.
#[test]
fn test_flat_storage_creation() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let chain_genesis = ChainGenesis::new(&genesis);
    let store = create_test_store();

    // Process some blocks with flat storage. Then remove flat storage data from disk.
    {
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(
            nearcore::NightshadeRuntime::test(Path::new("../../../.."), store.clone(), &genesis),
        )];
        let mut env =
            TestEnv::builder(chain_genesis.clone()).runtime_adapters(runtimes.clone()).build();
        for i in 1..4 {
            env.produce_block(0, i);
        }

        if cfg!(feature = "protocol_feature_flat_state") {
            // If chain was initialized from scratch, flat storage state should be created. During block processing, flat
            // storage head should be moved to block 1.
            assert_eq!(
                store_helper::get_flat_storage_state_status(&store, 0),
                FlatStorageStateStatus::Ready
            );
            let expected_flat_storage_head =
                env.clients[0].chain.get_block_hash_by_height(1).unwrap();
            assert_eq!(store_helper::get_flat_head(&store, 0), Some(expected_flat_storage_head));

            // Deltas for blocks 0 and 1 should not exist.
            for i in 0..2 {
                let block_hash = env.clients[0].chain.get_block_hash_by_height(i).unwrap();
                assert_eq!(store_helper::get_delta(&store, 0, block_hash), Ok(None));
            }
            // Deltas for blocks 2 and 3 should still exist, because they come after flat storage head.
            for i in 2..4 {
                let block_hash = env.clients[0].chain.get_block_hash_by_height(i).unwrap();
                assert_matches!(store_helper::get_delta(&store, 0, block_hash), Ok(Some(_)));
            }
        } else {
            assert_eq!(
                store_helper::get_flat_storage_state_status(&store, 0),
                FlatStorageStateStatus::DontCreate
            );
            assert_eq!(store_helper::get_flat_head(&store, 0), None);
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(3).unwrap();
        let epoch_id = env.clients[0].chain.runtime_adapter.get_epoch_id(&block_hash).unwrap();
        env.clients[0]
            .chain
            .runtime_adapter
            .remove_flat_storage_state_for_shard(0, &epoch_id)
            .unwrap();
    }

    // Create new chain and runtime using the same store. It should produce next blocks normally, but now it should
    // think that flat storage does not exist and background creation should be initiated.
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(nearcore::NightshadeRuntime::test(
        Path::new("../../../.."),
        store.clone(),
        &genesis,
    ))];
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes.clone()).build();
    for i in 4..6 {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_none());

    if !cfg!(feature = "protocol_feature_flat_state") {
        assert_eq!(
            store_helper::get_flat_storage_state_status(&store, 0),
            FlatStorageStateStatus::DontCreate
        );
        assert_eq!(store_helper::get_flat_head(&store, 0), None);
        // Stop the test here.
        return;
    }

    // At first, flat storage state should start saving deltas. Deltas for all newly processed blocks should be saved to
    // disk.
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 0),
        FlatStorageStateStatus::SavingDeltas
    );
    for i in 4..6 {
        let block_hash = env.clients[0].chain.get_block_hash_by_height(i).unwrap();
        assert_matches!(store_helper::get_delta(&store, 0, block_hash), Ok(Some(_)));
    }

    // Produce new block and run flat storage creation step.
    // We started the node from height 3, and now final head should move to height 4.
    // Because final head height became greater than height on which node started,
    // we must start fetching the state.
    env.produce_block(0, 6);
    assert!(!env.clients[0].run_flat_storage_creation_step().unwrap());
    let final_block_hash = env.clients[0].chain.get_block_hash_by_height(4).unwrap();
    assert_eq!(store_helper::get_flat_head(&store, 0), Some(final_block_hash));
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 0),
        FlatStorageStateStatus::FetchingState(FetchingStateStatus {
            part_id: 0,
            num_parts_in_step: NUM_PARTS_IN_ONE_STEP,
            num_parts: 1,
        })
    );

    // Run chain for a couple of blocks and check that statuses switch to `CatchingUp` and then to `Ready`.
    // We have a pause after processing each block because state fata is being fetched in rayon threads.
    // But we expect it to finish in <30s because state is small and there is only one state part.
    const BLOCKS_TIMEOUT: BlockHeight = 30;
    let start_height = 8;
    let mut next_height = start_height;
    let mut was_catching_up = false;
    while next_height < start_height + BLOCKS_TIMEOUT {
        env.produce_block(0, next_height);
        env.clients[0].run_flat_storage_creation_step().unwrap();
        next_height += 1;
        match store_helper::get_flat_storage_state_status(&store, 0) {
            FlatStorageStateStatus::FetchingState(..) => {
                assert!(!was_catching_up, "Flat storage state status inconsistency: it was catching up before fetching state");
            }
            FlatStorageStateStatus::CatchingUp => {
                was_catching_up = true;
            }
            FlatStorageStateStatus::Ready => {
                assert!(
                    was_catching_up,
                    "Flat storage state is ready but there was no flat storage catchup observed"
                );
                break;
            }
            status @ _ => {
                panic!(
                    "Unexpected flat storage state status for height {next_height}: {:?}",
                    status
                );
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    if next_height == start_height + BLOCKS_TIMEOUT {
        let status = store_helper::get_flat_storage_state_status(&store, 0);
        panic!("Apparently, node didn't fetch the whole state in {BLOCKS_TIMEOUT} blocks. Current status: {:?}", status);
    }

    // Finally, check that flat storage state was created.
    assert!(env.clients[0].run_flat_storage_creation_step().unwrap());
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_some());
}

/// Check that client can create flat storage on some shard while it already exists on another shard.
#[test]
fn test_flat_storage_creation_two_shards() {
    init_test_logger();
    let num_shards: NumShards = 2;
    let genesis = Genesis::test_sharded_new_version(
        vec!["test0".parse().unwrap()],
        1,
        vec![1; num_shards as usize],
    );
    let chain_genesis = ChainGenesis::new(&genesis);
    let store = create_test_store();

    // Process some blocks with flat storages for two shards. Then remove flat storage data from disk for shard 0.
    {
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(
            nearcore::NightshadeRuntime::test(Path::new("../../../.."), store.clone(), &genesis),
        )];
        let mut env =
            TestEnv::builder(chain_genesis.clone()).runtime_adapters(runtimes.clone()).build();
        for i in 1..4 {
            env.produce_block(0, i);
        }

        for shard_id in 0..num_shards {
            if cfg!(feature = "protocol_feature_flat_state") {
                assert_eq!(
                    store_helper::get_flat_storage_state_status(&store, shard_id),
                    FlatStorageStateStatus::Ready
                );
            } else {
                assert_eq!(
                    store_helper::get_flat_storage_state_status(&store, shard_id),
                    FlatStorageStateStatus::DontCreate
                );
            }
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(3).unwrap();
        let epoch_id = env.clients[0].chain.runtime_adapter.get_epoch_id(&block_hash).unwrap();
        env.clients[0]
            .chain
            .runtime_adapter
            .remove_flat_storage_state_for_shard(0, &epoch_id)
            .unwrap();
    }

    if !cfg!(feature = "protocol_feature_flat_state") {
        return;
    }

    // Check that flat storage is not ready for shard 0 but ready for shard 1.
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(nearcore::NightshadeRuntime::test(
        Path::new("../../../.."),
        store.clone(),
        &genesis,
    ))];
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes.clone()).build();
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_none());
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 0),
        FlatStorageStateStatus::SavingDeltas
    );
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(1).is_some());
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 1),
        FlatStorageStateStatus::Ready
    );

    // Run chain for a couple of blocks and check that flat storage for shard 0 is eventually created.
    const BLOCKS_TIMEOUT: BlockHeight = 30;
    let start_height = 4;
    let mut next_height = start_height;
    while next_height < start_height + BLOCKS_TIMEOUT {
        env.produce_block(0, next_height);
        env.clients[0].run_flat_storage_creation_step().unwrap();
        next_height += 1;
        if env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_some() {
            break;
        }
        thread::sleep(Duration::from_secs(1));
    }
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_some());
}
