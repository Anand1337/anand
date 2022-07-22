extern crate chrono;

use std::path::Path;
use near_store::{Store, StoreConfig, DBCol, db::Mode};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::env;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let keys_filename = args[1].parse::<String>().unwrap();
    let lines = read_lines(keys_filename);
    if !lines.is_ok() {
        println!(":(");
        return ();
    }
    let lines: Vec<String> = lines.unwrap().map(|l| l.expect("Could not parse line")).collect();

    let start = std::time::Instant::now();
    let store = Store::opener(&Path::new("/home/edvard/.near"), &StoreConfig::default()).mode(Mode::ReadOnly).open();
    println!("opened store in: {}", start.elapsed().as_micros());
	let mut dist = vec![];
    for line in &lines {
        let hash = CryptoHash::from_str(&line).unwrap();
        // println!("IN");
        let start = std::time::Instant::now();
        let _ = store.get_ser::<PartialMerkleTree>(DBCol::BlockMerkleTree, &hash.0).unwrap().unwrap();
        let duration = start.elapsed().as_micros();
        // println!("OUT: {}", duration);
        dist.push(duration);
    }

    dist.sort();
    for p in [0, 1, 5, 10, 50, 90, 95, 99, 100] {
        let i = std::cmp::min(dist.len() * p / 100, dist.len() - 1);
        println!("{} {}", p, dist[i]);
    }
}
