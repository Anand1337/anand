use std::fs;
use rand::Rng;
use std::path::Path;
use near_store::{StoreConfig, create_store_with_config, DBCol};

fn u32_to_vec(value: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4);
	bytes.extend(&value.to_be_bytes());
    bytes
}

fn main() {
	let path = "/home/edvard/nearcore/test_rocksdb";

	fs::remove_dir_all(path).unwrap();
	fs::create_dir(path).unwrap();

    let store = create_store_with_config(Path::new(path), &StoreConfig::read_write());
    for i in 0..65_000_000 {
        let mut store_update = store.store_update();
        store_update.set_ser(DBCol::State, &u32_to_vec(i), &true).unwrap();
        let _ = store_update.commit();
    }
}
