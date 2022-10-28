use crate::RuntimeAdapter;
use near_primitives::types::{BlockHeight, NumShards};
use std::sync::Arc;

#[derive(Clone)]
pub enum MigrationStatus {
    SavingDeltas,
    FetchingState,
    CatchingUp,
    Finished,
}

pub struct FlatStorageMigrator {
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub statuses: Vec<MigrationStatus>,
    pub starting_height: BlockHeight,
}

impl FlatStorageMigrator {
    pub fn new(
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        num_shards: NumShards,
        starting_height: BlockHeight,
    ) -> Self {
        Self {
            runtime_adapter,
            statuses: vec![MigrationStatus::SavingDeltas; num_shards as usize],
            starting_height,
        }
    }
}
