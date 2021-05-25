#!/bin/bash
# Our custom function
cust_func(){
  DB_FOLDER="./temp$1"
  LOG_FILE="./result$1.txt"
  ulimit -l 134217728 && ./db_bench --benchmarks=fillrandom --use_existing_db=0 --disable_auto_compactions=0 --sync=0 --db=$DB_FOLDER \
  --wal_dir=$DB_FOLDER --num=10000000 --num_levels=6 --key_size=16 --value_size=400 --block_size=8192 \
  --cache_size=6442450944 --cache_numshardbits=6 --compression_max_dict_bytes=0 --compression_ratio=0.5 \
  --compression_type=none --level_compaction_dynamic_level_bytes=true --bytes_per_sync=8388608 --cache_index_and_filter_blocks=0 \
  --pin_l0_filter_and_index_blocks_in_cache=1 --benchmark_write_rate_limit=0 --hard_rate_limit=3 --rate_limit_delay_max_milliseconds=1000000 \
  --write_buffer_size=134217728 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --verify_checksum=1 \
  --delete_obsolete_files_period_micros=62914560 --max_bytes_for_level_multiplier=8 --statistics=0 --stats_per_interval=1 \
  --stats_interval_seconds=60 --histogram=1 --memtablerep=skip_list --bloom_bits=10 --open_files=-1 --max_background_compactions=1 \
  --max_write_buffer_number=8 --max_background_flushes=1 --level0_file_num_compaction_trigger=4 --level0_stop_writes_trigger=20 \
  --level0_slowdown_writes_trigger=12 --threads=1 --allow_concurrent_memtable_write=true --disable_wal=1 --seed=1606010580 < ../input.txt > $LOG_FILE 2>&1
}
cd release
# For loop 5 times
for i in {1..8}
do
	cust_func $i & # Put a function in the background
done

## Put all cust_func in the background and bash
## would wait until those are completed
## before displaying all done message
wait
echo "All done"