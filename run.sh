#!/bin/bash

rm fn_data
START=67148923; END=67354923; cargo build -p neard --release && ./target/release/neard view_state apply_range --shard-id=1 --start-index $START --end-index $END > fn_data &

python3 e.py > output.txt
