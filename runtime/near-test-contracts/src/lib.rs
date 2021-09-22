use serde_json::json;
use std::path::Path;

use once_cell::sync::OnceCell;

pub fn rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_rs.wasm")).as_slice()
}

pub fn nightly_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("nightly_test_contract_rs.wasm")).as_slice()
}

pub fn ts_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_ts.wasm")).as_slice()
}

pub fn tiny_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("tiny_contract_rs.wasm")).as_slice()
}

pub fn aurora_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("aurora_engine.wasm")).as_slice()
}

pub fn get_aurora_contract_data() -> (&'static [u8], &'static str, Option<Vec<u8>>) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (
        CONTRACT.get_or_init(|| read_contract("aurora_engine.wasm")).as_slice(),
        "state_migration",
        None,
    )
}

pub fn get_aurora_small_contract_data() -> (&'static [u8], &'static str, Option<Vec<u8>>) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (
        CONTRACT.get_or_init(|| read_contract("aurora_engine_small.wasm")).as_slice(),
        "state_migration",
        None,
    )
}

pub fn get_aurora_with_deploy_data() -> (&'static [u8], &'static str, Option<Vec<u8>>) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (
        CONTRACT.get_or_init(|| read_contract("aurora_engine_with_deploy.wasm")).as_slice(),
        "state_migration",
        None,
    )
}

pub fn get_multisig_contract_data() -> (&'static [u8], &'static str, Option<Vec<u8>>) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (
        CONTRACT.get_or_init(|| read_contract("multisig.wasm")).as_slice(),
        "get_request_nonce",
        Some(json!({"num_confirmations": 1}).to_string().as_bytes().to_vec()),
    )
}

pub fn get_voting_contract_data() -> (&'static [u8], &'static str, Option<Vec<u8>>) {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    (
        CONTRACT.get_or_init(|| read_contract("voting_contract.wasm")).as_slice(),
        "get_result",
        Some(vec![]),
    )
}

pub fn get_rs_contract_data() -> (&'static [u8], &'static str, Option<Vec<u8>>) {
    (rs_contract(), "hello0", None)
}

/// Read given wasm file or panic if unable to.
fn read_contract(file_name: &str) -> Vec<u8> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = base.join("res").join(file_name);
    match std::fs::read(&path) {
        Ok(data) => data,
        Err(err) => panic!("{}: {}", path.display(), err),
    }
}

#[test]
fn smoke_test() {
    assert!(!rs_contract().is_empty());
    assert!(!nightly_rs_contract().is_empty());
    assert!(!ts_contract().is_empty());
    assert!(!tiny_contract().is_empty());
}
