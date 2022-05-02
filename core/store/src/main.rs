use std::fs;
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
    for (lf, rg) in [(0, 500_000), (500_000, 1_000_000)] {
        for _ in 0..10 {
            for i in lf..rg {
                let mut store_update = store.store_update();
                store_update.update_refcount(DBCol::State, &u32_to_vec(i), &[0], 1);
                let _ = store_update.commit();
            }
            let _ = store.get_rocksdb().unwrap().db.flush();
            store.get_rocksdb().unwrap().db.compact_range(None::<&[u8]>, None::<&[u8]>);
        }
    }

    for i in 0..10 {
        let store = create_store_with_config(Path::new(path), &StoreConfig::read_only());
        std::fs::read("/home/edvard/nearcore/x");
        let value = store.get_raw(DBCol::State, &u32_to_vec(0)).expect(":(").unwrap();
        std::fs::read("/home/edvard/nearcore/x");
        println!("Got: {:?}", value);
    }
}
