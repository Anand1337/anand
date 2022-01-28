use crate::borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    Serialize,
    Deserialize,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Debug,
)]
pub enum UpgradeMode {
    Normal,
    Emergency,
}

impl Default for UpgradeMode {
    fn default() -> Self {
        UpgradeMode::Normal
    }
}
