#!/bin/bash

make neard
rm fn_data_verbose
START=56421132
END=67421208
./target/release/neard view_state apply_range --shard-id=0 --start-index $START --end-index $END > fn_data_verbose

# python3 e.py > output.txt
