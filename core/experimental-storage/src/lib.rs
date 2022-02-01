use blake3;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Seek, SeekFrom};
use std::mem::drop;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, MutexGuard,
};

type Hash = [u8; 32];

pub struct Storage {
    file: File,
    num_buckets: u64,
    locks: Box<[Mutex<()>]>,
    size: AtomicU64,
}

#[derive(Copy, Clone, Debug)]
pub struct DataError();

impl Display for DataError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Data error")
    }
}

impl Error for DataError {}

fn check(v: bool) -> io::Result<()> {
    if v {
        Ok(())
    } else {
        Err(io::Error::new(ErrorKind::InvalidData, DataError()))
    }
}

struct Bucket<'a> {
    hash: Hash,
    index: u64,
    #[allow(dead_code)]
    guard: MutexGuard<'a, ()>,
    data: Vec<(Hash, Vec<u8>)>,
}

impl Storage {
    pub fn new(path: impl AsRef<Path>, num_buckets: u64, num_locks: usize) -> io::Result<Self> {
        assert!(
            num_buckets != 0
                && num_buckets & (num_buckets - 1) == 0
                && num_buckets <= 1 << 59
                && num_locks != 0
                && num_locks & (num_locks - 1) == 0
                && num_locks as u64 <= num_buckets
        );
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let mut size = (&file).seek(SeekFrom::End(0))?;
        let min_size = num_buckets << 4;
        if size < min_size {
            size = min_size;
            file.set_len(size)?;
        }
        Ok(Storage {
            file,
            num_buckets: num_buckets,
            locks: {
                let mut locks = Vec::with_capacity(num_locks);
                locks.resize_with(num_locks, Default::default);
                locks.into_boxed_slice()
            },
            size: AtomicU64::new(size),
        })
    }

    fn read_bucket(&self, key: &[u8]) -> io::Result<Bucket> {
        let hash = *blake3::hash(key).as_bytes();
        let index = u64::from_le_bytes(hash[..8].try_into().unwrap()) & (self.num_buckets - 1);
        let guard = self.locks[index as usize & (self.locks.len() - 1)].lock().unwrap();
        let mut pointers = [0; 16];
        self.file.read_exact_at(&mut pointers, index << 4)?;
        let offset = u64::from_le_bytes(pointers[..8].try_into().unwrap());
        let length = u64::from_le_bytes(pointers[8..].try_into().unwrap());
        let mut data = Vec::new();
        if offset != 0 || length != 0 {
            let mut bytes = Vec::with_capacity(length as usize);
            bytes.resize_with(length as usize, Default::default);
            self.file.read_exact_at(&mut bytes, offset)?;
            let mut rem = &bytes[..];
            while rem.len() != 0 {
                check(rem.len() >= 36)?;
                let cur_hash = rem[..32].try_into().unwrap();
                let cur_len = u32::from_le_bytes(rem[32..36].try_into().unwrap()) as usize;
                rem = &rem[36..];
                check(rem.len() >= cur_len)?;
                data.push((cur_hash, rem[..cur_len].into()));
                rem = &rem[cur_len..];
            }
        }
        Ok(Bucket { hash, index, guard, data })
    }

    fn write_bucket(&self, index: u64, data: Vec<(Hash, Vec<u8>)>) -> io::Result<()> {
        let mut bytes = Vec::new();
        for (hash, value) in data {
            bytes.extend_from_slice(&hash);
            let len = value.len();
            if len > u32::MAX as usize {
                panic!("Maximum value size exceeded");
            }
            bytes.extend_from_slice(&(len as u32).to_le_bytes());
            bytes.extend_from_slice(&value);
        }
        let length = bytes.len() as u64;
        let offset = self.size.fetch_add(length, Ordering::Relaxed);
        if offset + length < offset {
            panic!("File size overflow");
        }
        let mut pointers = [0; 16];
        pointers[..8].copy_from_slice(&offset.to_le_bytes());
        pointers[8..].copy_from_slice(&length.to_le_bytes());
        self.file.write_all_at(&bytes, offset)?;
        self.file.write_all_at(&pointers, index << 4)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let bucket = self.read_bucket(key)?;
        for (hash, value) in bucket.data {
            if hash == bucket.hash {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn put(&self, key: &[u8], value: Vec<u8>) -> io::Result<()> {
        let mut bucket = self.read_bucket(key)?;
        let mut it = bucket.data.iter_mut();
        loop {
            match it.next() {
                Some((hash, val)) => {
                    if *hash == bucket.hash {
                        *val = value;
                        break;
                    }
                }
                None => {
                    bucket.data.push((bucket.hash, value));
                    break;
                }
            }
        }
        self.write_bucket(bucket.index, bucket.data)
    }

    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        let mut bucket = self.read_bucket(key)?;
        let mut it = bucket.data.iter_mut().enumerate();
        while let Some((i, (hash, val))) = it.next() {
            if *hash == bucket.hash {
                drop((hash, val));
                drop(it);
                bucket.data.remove(i);
                return self.write_bucket(bucket.index, bucket.data);
            }
        }
        Ok(())
    }
}
