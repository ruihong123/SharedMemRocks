import argparse
import json
import os
import subprocess
from distutils.dir_util import copy_tree
from shutil import copyfile, rmtree
from threading import Timer
TIMES_TO_RUN = 8
# STUDENT_TEST_LOG_NAME = 'test-results'
# STUDENT_SUBMISSION_DIR = "Assignment4"
# SCORES = {}
SEARCH_TERMS = ['fillrandom']
DB_BENCH_DIR = "./release"
# LAB_NUMBER = 3
TIMEOUT = 5 * 60
def run(cmd, out, cwd, timeout_sec):
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=out, stderr=out)
    print("Process start!")
    kill_proc = lambda p: p.kill()
    timer = Timer(timeout_sec, kill_proc, [proc])
    try:
        timer.start()
        proc.wait()
    finally:
        timer.cancel()

for run_index in range(TIMES_TO_RUN):
    db_path = os.path.join("./temp"+ str(run_index))
    with open("result" + str(run_index) + '.txt', 'w+') as out:
        cmd = ['ulimit -l 134217728 && ', './db_bench', '--benchmarks=fillrandom --use_existing_db=0 --disable_auto_compactions=0 --sync=0',
               '--db=./temp' + str(run_index), '--wal_dir=./temp' + str(run_index), '--num=10000000', '--num_levels=6', '--key_size=16', '--value_size=400', '--block_size=8192', '--cache_size=6442450944',
               '--cache_numshardbits=6', '--compression_max_dict_bytes=0', '--compression_ratio=0.5', '--compression_type=none', '--level_compaction_dynamic_level_bytes=true', '--bytes_per_sync=8388608',
               '--cache_index_and_filter_blocks=0', '--pin_l0_filter_and_index_blocks_in_cache=1', '--benchmark_write_rate_limit=0',
               '--hard_rate_limit=3', '--rate_limit_delay_max_milliseconds=1000000', '--write_buffer_size=67108864', '--target_file_size_base=134217728',
               '--max_bytes_for_level_base=1073741824', '--verify_checksum=1', '--delete_obsolete_files_period_micros=62914560',
               '--max_bytes_for_level_multiplier=8', '--statistics=0', '--stats_per_interval=1', '--stats_interval_seconds=60',
               '--histogram=1', '--memtablerep=skip_list', '--bloom_bits=10', '--open_files=-1', '--max_background_compactions=1',
               '--max_write_buffer_number=1', '--max_background_flushes=1', '--level0_file_num_compaction_trigger=4', '--level0_stop_writes_trigger=20',
               '--level0_slowdown_writes_trigger=12', '--threads=1', '--allow_concurrent_memtable_write=true', '--disable_wal=1', '--seed=1606010580', '<', '../input.txt']

        # run(cmd, out, DB_BENCH_DIR, TIMEOUT)
        # subprocess.call("ulimit -l 134217728 && ./db_bench --benchmarks=fillrandom --use_existing_db=0 --disable_auto_compactions=0 --sync=0 --db=./temp" + str(run_index) + " --wal_dir=./temp" + str(run_index) + " --num=10000000 --num_levels=6 --key_size=16 --value_size=400 --block_size=8192 --cache_size=6442450944 --cache_numshardbits=6 --compression_max_dict_bytes=0 --compression_ratio=0.5 --compression_type=none --level_compaction_dynamic_level_bytes=true --bytes_per_sync=8388608 --cache_index_and_filter_blocks=0 --pin_l0_filter_and_index_blocks_in_cache=1 --benchmark_write_rate_limit=0 --hard_rate_limit=3 --rate_limit_delay_max_milliseconds=1000000 --write_buffer_size=134217728 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --verify_checksum=1 --delete_obsolete_files_period_micros=62914560 --max_bytes_for_level_multiplier=8 --statistics=0 --stats_per_interval=1 --stats_interval_seconds=60 --histogram=1 --memtablerep=skip_list --bloom_bits=10 --open_files=-1 --max_background_compactions=1 --max_write_buffer_number=8 --max_background_flushes=1 --level0_file_num_compaction_trigger=4 --level0_stop_writes_trigger=20 --level0_slowdown_writes_trigger=12 --threads=1 --allow_concurrent_memtable_write=true --disable_wal=1 --seed=1606010580 < ../input.txt > ./result" + str(run_index) + '.txt 2>&1', shell=True, cwd=DB_BENCH_DIR)
        print(out)
        proc = subprocess.Popen(cmd, cwd=DB_BENCH_DIR, stdout=out, stderr=out)
