use near_primitives::receipt::ReceiptResult;
use near_primitives::runtime::migration_data::MigrationData;
use near_primitives::types::Gas;

lazy_static_include::lazy_static_include_bytes! {
    /// File with receipts which were lost because of a bug in apply_chunks to the runtime config.
    /// Follows the ReceiptResult format which is HashMap<ShardId, Vec<Receipt>>.
    /// See https://github.com/near/nearcore/pull/4248/ for more details.
    MAINNET_RESTORED_RECEIPTS => "res/mainnet_restored_receipts.json",
}

lazy_static_include::lazy_static_include_bytes! {
    /// File with account ids and deltas that need to be applied in order to fix storage usage
    /// difference between actual and stored usage, introduced due to bug in access key deletion,
    /// see https://github.com/near/nearcore/issues/3824
    /// This file was generated using tools/storage-usage-delta-calculator
    MAINNET_STORAGE_USAGE_DELTA => "res/storage_usage_delta.json",
}

/// In test runs reads and writes here used 442 TGas, but in test on live net migration take
/// between 4 and 4.5s. We do not want to process any receipts in this block
const GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION: Gas = 1_000_000_000_000_000;

pub fn load_migration_data(chain_id: &str) -> MigrationData {
    let is_mainnet = chain_id == "mainnet";
    MigrationData {
        storage_usage_delta: if is_mainnet {
            serde_json::from_slice(&MAINNET_STORAGE_USAGE_DELTA).unwrap()
        } else {
            Vec::new()
        },
        storage_usage_fix_gas: if is_mainnet {
            GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION
        } else {
            0
        },
        restored_receipts: if is_mainnet {
            serde_json::from_slice(&MAINNET_RESTORED_RECEIPTS)
                .expect("File with receipts restored after apply_chunks fix have to be correct")
        } else {
            ReceiptResult::default()
        },
    }
}
