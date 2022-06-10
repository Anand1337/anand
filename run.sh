#!/bin/bash

make neard
for shard_id in {0,1,2,3}; do
  rm fn_data_verbose_$shard_id
  START=67240000
  END=67421208
  ./target/release/neard view_state apply_range --shard-id=$shard_id --start-index $START --end-index $END > fn_data_verbose_$shard_id &
done

# python3 e.py > output.txt
