extern crate chrono;

use std::path::Path;
use near_store::{Store, StoreConfig, DBCol, db::Mode};
use borsh::BorshDeserialize;
use near_primitives::transaction::{SignedTransaction, Action};
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::hash::CryptoHash;
use std::collections::HashMap;
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, ExecutionMetadata, ExecutionOutcome};
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::env;
use chrono::Local;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn get_height(header: &near_primitives::block_header::BlockHeader) -> u64 {
    match header {
        near_primitives::block_header::BlockHeader::BlockHeaderV1(a) => a.inner_lite.height,
        near_primitives::block_header::BlockHeader::BlockHeaderV2(a) => a.inner_lite.height,
        near_primitives::block_header::BlockHeader::BlockHeaderV3(a) => a.inner_lite.height,
    }
}

fn main() {
    let store = Store::opener(&Path::new("/home/edvard/.near2"), &StoreConfig::default()).mode(Mode::ReadOnly).open();
    let mut cnt = 0;
    for key_value in store.iter(DBCol::State) {
        if key_value.is_err() {
            continue;
        }
        let (key, value) = key_value.unwrap();
        let hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
        cnt += 1;
        println!("{}", hash);
    }
    println!("{}", cnt);
}
