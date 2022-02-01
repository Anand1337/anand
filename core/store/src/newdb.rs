use std::io;
use std::path::Path;

use super::db::{
    refcount::merge_refcounted_records, DBCol, DBError, DBOp, DBTransaction, Database, RocksDB,
};
use near_experimental_storage::Storage;

const NUM_BUCKETS: u64 = 1 << 30;
const NUM_LOCKS: usize = 1 << 10;

pub struct WrappedRocksDB {
    rocks_db: RocksDB,
    storage: Storage,
}

impl WrappedRocksDB {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DBError> {
        Ok(WrappedRocksDB {
            rocks_db: RocksDB::new(path.as_ref())?,
            storage: Storage::new(path.as_ref().join("newdb"), NUM_BUCKETS, NUM_LOCKS)?,
        })
    }
}

fn use_new_storage(col: DBCol) -> bool {
    col != DBCol::ColStateChanges && col != DBCol::ColStateDlInfos && col != DBCol::ColPeers
}

fn mkkey(col: DBCol, key: &[u8]) -> Vec<u8> {
    let mut res = Vec::with_capacity(key.len() + 1);
    res.push(col as u8);
    res.extend_from_slice(key);
    res
}

impl Database for WrappedRocksDB {
    fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        if use_new_storage(col) {
            self.storage.get(&mkkey(col, key)).map(|v| RocksDB::get_with_rc_logic(col, v))
        } else {
            self.rocks_db.get(col, key)
        }
    }

    fn iter<'a>(
        &'a self,
        column: DBCol,
    ) -> Box<(dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a)> {
        if use_new_storage(column) {
            unimplemented!()
        } else {
            self.rocks_db.iter(column)
        }
    }

    fn iter_without_rc_logic<'a>(
        &'a self,
        column: DBCol,
    ) -> Box<(dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a)> {
        if use_new_storage(column) {
            unimplemented!()
        } else {
            self.rocks_db.iter_without_rc_logic(column)
        }
    }

    fn iter_prefix<'a>(
        &'a self,
        col: DBCol,
        key_prefix: &'a [u8],
    ) -> Box<(dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a)> {
        if use_new_storage(col) {
            unimplemented!()
        } else {
            self.rocks_db.iter_prefix(col, key_prefix)
        }
    }

    fn write(&self, mut transaction: DBTransaction) -> Result<(), io::Error> {
        let mut new_ops = Vec::new();
        for op in transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => {
                    if use_new_storage(col) {
                        self.storage.put(&mkkey(col, &key), value)?
                    } else {
                        new_ops.push(DBOp::Insert { col, key, value })
                    }
                }
                DBOp::UpdateRefcount { col, key, value } => {
                    if use_new_storage(col) {
                        let k = mkkey(col, &key);
                        let v = if let Some(mut v) = self.storage.get(&k)? {
                            merge_refcounted_records(&mut v, &value);
                            v
                        } else {
                            value
                        };
                        self.storage.put(&k, v)?
                    } else {
                        new_ops.push(DBOp::UpdateRefcount { col, key, value })
                    }
                }
                DBOp::Delete { col, key } => {
                    if use_new_storage(col) {
                        self.storage.delete(&mkkey(col, &key))?
                    } else {
                        new_ops.push(DBOp::Delete { col, key })
                    }
                }
                DBOp::DeleteAll { col } => {
                    if use_new_storage(col) {
                        unimplemented!()
                    } else {
                        new_ops.push(DBOp::DeleteAll { col })
                    }
                }
            }
        }
        transaction.ops = new_ops;
        self.rocks_db.write(transaction)
    }

    fn as_rocksdb(&self) -> Option<&RocksDB> {
        Some(&self.rocks_db)
    }
}
