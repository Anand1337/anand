extern crate chrono;

use std::path::Path;
use near_store::{Store, StoreConfig, DBCol, db::Mode};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardUId, ShardLayout};
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
    let store = Store::opener(&Path::new("/home/edvard/.near2"), &StoreConfig::default()).mode(Mode::ReadOnly).open();
    println!("opened store in: {}", start.elapsed().as_micros());
	let mut dist = vec![];
    for line in &lines {
        let tokens: Vec<String> = line.split(" ").map(|s| s.to_string()).collect();
        assert!(tokens.len() == 3, ":(");
        let shard_version = tokens[0].parse::<u32>().unwrap();
        let shard_id = tokens[1].parse::<u64>().unwrap();
        let hash = CryptoHash::from_str(&tokens[2]).unwrap();
        let key = near_store::TrieCachingStorage::get_key_from_shard_uid_and_hash(ShardUId::from_shard_id_and_layout(shard_id, &ShardLayout::v0(4, shard_version)), &hash);
        // println!("{:?} {:?} {:?}", shard_id, &hash, &key);
        let start = std::time::Instant::now();
        let _ = store.get(DBCol::State, &key).unwrap().unwrap();
        let duration = start.elapsed().as_micros();
        // println!("OUT: {}", duration);
        dist.push(duration);
    }

    dist.sort();
    for p in [0, 1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99, 100] {
        let i = std::cmp::min(dist.len() * p / 100, dist.len() - 1);
        println!("{} {}", p, dist[i]);
    }
}
