#[macro_use]
extern crate bencher;

use std::{ops::Deref, sync::Arc};

use bencher::Bencher;
use near_crypto::SecretKey;
use near_network::{
    network_protocol::{PeerIdOrHash, RawRoutedMessage, RoutedMessageBody},
    types::PartialEncodedChunkForwardMsg,
};
use near_o11y::metrics::{try_create_int_counter_vec, IntCounter, IntCounterVec};
use near_primitives::{
    hash::CryptoHash,
    sharding::{ChunkHash, PartialEncodedChunkPart},
};
use once_cell::sync::Lazy;

static COUNTERS: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec("near_test_counters_1", "Just counters", &["shard_id"]).unwrap()
});

const NUM_SHARDS: usize = 8;

fn inc_counter_vec_with_label_values(bench: &mut Bencher) {
    bench.iter(|| {
        for shard_id in 0..NUM_SHARDS {
            COUNTERS.with_label_values(&[&shard_id.to_string()]).inc();
        }
    });
}

fn raw_routed_message_sign(bench: &mut Bencher) {
    let node_key = SecretKey::from_random(near_crypto::KeyType::ED25519);

    // 25k ns/iter
    bench.iter(|| {
        let message = RawRoutedMessage {
            target: PeerIdOrHash::Hash(CryptoHash::new()),
            body: RoutedMessageBody::_UnusedReceiptOutcomeResponse,
        };
        message.sign(&node_key, 100, None)
    });
}

fn raw_routed_message_sign_with_size(bench: &mut Bencher, part_size: u64) {
    let node_key = SecretKey::from_random(near_crypto::KeyType::ED25519);

    let mut v = Vec::new();
    for _ in 0..part_size {
        v.push(18 as u8);
    }

    let part =
        PartialEncodedChunkPart { part_ord: 12, part: v.into_boxed_slice(), merkle_proof: vec![] };

    let msg = PartialEncodedChunkForwardMsg {
        chunk_hash: ChunkHash { 0: CryptoHash::new() },
        inner_header_hash: CryptoHash::new(),
        merkle_root: CryptoHash::new(),
        signature: node_key.sign(&[]),
        prev_block_hash: CryptoHash::new(),
        height_created: 100,
        shard_id: 4,
        parts: vec![part],
    };
    // 25k ns/iter
    bench.iter(|| {
        let message = RawRoutedMessage {
            target: PeerIdOrHash::Hash(CryptoHash::new()),
            body: RoutedMessageBody::PartialEncodedChunkForward(msg.clone()),
        };
        message.sign(&node_key, 100, None)
    });
}

fn raw_routed_message_sign_partial_encoded_chunk_1k(bench: &mut Bencher) {
    // 25k nanoseconds => 40k msg/core/second
    raw_routed_message_sign_with_size(bench, 1000);
}

fn raw_routed_message_sign_partial_encoded_chunk_30k(bench: &mut Bencher) {
    // 40k nanoseconds => 25k msg/core/second
    raw_routed_message_sign_with_size(bench, 30000);
}

fn raw_routed_message_sign_partial_encoded_chunk_1m(bench: &mut Bencher) {
    // 500k nanoseconds => 2k msg/core/second
    raw_routed_message_sign_with_size(bench, 1000000);
}

benchmark_group!(
    benches,
    inc_counter_vec_with_label_values,
    raw_routed_message_sign,
    raw_routed_message_sign_partial_encoded_chunk_1k,
    raw_routed_message_sign_partial_encoded_chunk_30k,
    raw_routed_message_sign_partial_encoded_chunk_1m
);
benchmark_main!(benches);
