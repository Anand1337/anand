use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::env::args;
use std::path::Path;
use std::time::Instant;

use near_experimental_storage::Storage as S;

const KEY_LEN: usize = 64;
const VALUE_LEN: usize = 1000;
const BATCH_SIZE: usize = 1000;
const BATCHES_PER_PHASE: usize = 1000;
const NUM_ITERATIONS: usize = 1000;

struct Storage {
    db: S,
    n: u64,
    rng: SmallRng,
    key: Vec<u8>,
    value: Vec<u8>,
}

fn gen_vector(rng: &mut impl Rng, min_len: usize, max_len: usize, dest: &mut Vec<u8>) {
    dest.resize(rng.gen_range(min_len, max_len + 1), 0);
    rng.fill_bytes(dest);
}

impl Storage {
    fn new(path: &Path) -> Self {
        Storage {
            db: S::new(path, 1 << 30, 1 << 10).unwrap(),
            n: 0,
            rng: SmallRng::from_entropy(),
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    fn gen_i(&mut self) -> u64 {
        self.rng.gen_range(0, self.n)
    }

    fn gen_k(&mut self, i: u64) {
        gen_vector(&mut SmallRng::seed_from_u64(i), KEY_LEN / 2, KEY_LEN, &mut self.key);
    }

    fn get(&mut self) {
        let i = self.gen_i();
        self.gen_k(i);
        self.db.get(&self.key).unwrap();
    }

    fn put(&mut self, i: u64) {
        self.gen_k(i);
        gen_vector(&mut self.rng, VALUE_LEN / 2, VALUE_LEN, &mut self.value);
        self.db.put(&self.key, self.value.clone()).unwrap();
    }

    fn put_new(&mut self) {
        let i = self.n;
        self.n += 1;
        self.put(i);
    }

    fn put_existing(&mut self) {
        let i = self.gen_i();
        self.put(i);
    }

    fn commit(&mut self) {}

    fn phase(&mut self, name: &str, iteration: usize, mut f: impl FnMut(&mut Self)) {
        let start = Instant::now();
        for _ in 0..BATCHES_PER_PHASE {
            for _ in 0..BATCH_SIZE {
                f(self);
            }
            self.commit();
        }
        println!("{name:6} {iteration:4} took {:6}ms", start.elapsed().as_millis());
    }

    fn run(&mut self) {
        for iteration in 0..NUM_ITERATIONS {
            self.phase("append", iteration, Self::put_new);
            self.phase("update", iteration, Self::put_existing);
            self.phase("mixed", iteration, |s| {
                if s.rng.gen_ratio(1, 2) {
                    if s.rng.gen_ratio(1, 2) {
                        s.put_new();
                    } else {
                        s.put_existing();
                    }
                } else {
                    s.get();
                }
            });
            self.phase("read", iteration, Self::get);
        }
    }
}

fn main() {
    let arg = args().nth(1).unwrap();
    let path = Path::new(&arg);
    if path.exists() {
        panic!("Path {} already exists, remove it first", path.to_str().unwrap());
    }
    let start = Instant::now();
    let mut storage = Storage::new(path);
    println!("init        took {:6}ms", start.elapsed().as_millis());
    storage.run();
}
