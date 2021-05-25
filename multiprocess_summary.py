import argparse
import json
import os
import subprocess
from distutils.dir_util import copy_tree
from shutil import copyfile, rmtree
from threading import Timer
TIMES_TO_RUN = 4
# STUDENT_TEST_LOG_NAME = 'test-results'
# STUDENT_SUBMISSION_DIR = "Assignment4"
# SCORES = {}
SEARCH_TERMS = ['fillrandom']
DB_BENCH_DIR = "./release"
# LAB_NUMBER = 3
# TIMEOUT = 10 * 60
total_bandwidth=0
for run_index in range(TIMES_TO_RUN):
    db_path = os.path.join(DB_BENCH_DIR + "/result"+ str(run_index+1) + '.txt')

    with open(db_path, 'r') as out:
        test_results = out.read()
    for line in test_results.split('\n'):
        for term in SEARCH_TERMS:
            if term in line:
                print("line split " + line.split()[6])
                total_bandwidth = total_bandwidth + float(line.split()[6])
print("total bandwidth is " + str(total_bandwidth))