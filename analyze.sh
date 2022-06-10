#!/bin/bash

rm output.txt
for shard_id in {0,1,2,3}; do
  echo "shard: $shard_id" >> output.txt
  python3 e.py fn_data_verbose_$shard_id >> output.txt
done

