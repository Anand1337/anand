#!/bin/bash
set -e
cd "`dirname $0`"
cargo build --target wasm32-unknown-unknown --release
cp ./target/wasm32-unknown-unknown/release/endtoend.wasm ./res/
