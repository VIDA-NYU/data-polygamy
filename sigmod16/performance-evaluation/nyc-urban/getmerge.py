# Copyright (C) 2016 New York University
# This file is part of Data Polygamy which is released under the Revised BSD License
# See file LICENSE for full license details.

import sys
import subprocess
import os

output_events_dir = sys.argv[1]
output_outliers_dir = sys.argv[2]
perm = sys.argv[3]
files = []
datasets = ["weather", "311", "crash", "citibike", "taxispeed", "gas-prices"]
# hdfs dfs -ls -h relationships/*
for line in sys.stdin:
    if "Found" in line:
        continue
    if "_SUCCESS" in line:
        continue
    l = line.split(" ")
    for d in datasets:
        if (d in line) and (perm in line):
            files.append(l[-1].replace("\n",""))
            break

for file in files:
    output_dir = output_events_dir
    if "outlier" in file:
        output_dir = output_outliers_dir
    filename = os.path.basename(os.path.dirname(file)) + "-" + os.path.basename(file)
    print filename
    subprocess.call(['hdfs dfs -getmerge ' + file + ' ' + os.path.join(output_dir, filename)], shell=True)
    print ""
