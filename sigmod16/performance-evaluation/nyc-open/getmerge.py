# Copyright (C) 2016 New York University
# This file is part of Data Polygamy which is released under the Revised BSD License
# See file LICENSE for full license details.

import sys
import subprocess
import os
import tempfile
import re

pattern = re.compile("([a-zA-Z0-9]{4}\-[a-zA-Z0-9]{4})\-([a-zA-Z0-9]{4}\-[a-zA-Z0-9]{4})")

output_events_dir = sys.argv[1]
output_outliers_dir = sys.argv[2]
metadata_file = sys.argv[3]
files = []

command = "hdfs dfs -ls -h relationships/*"

datasets = {}
f = open(metadata_file)
line = f.readline()
while line != "":
    l = line.split(",")
    db = l[0]
    datasets[db] = 1
    line = f.readline()
f.close()

temp = tempfile.TemporaryFile()
out = subprocess.call(command, shell=True, stdout=temp)
temp.seek(0)
line = temp.readline()
while line != "":
    if ("Found" in line) or ("_SUCCESS" in line):
        line = temp.readline()
        continue
    l = line.split(" ")
    file = l[-1].replace("\n","")
    groups = pattern.search(file)
    if groups is None:
        line = temp.readline()
        continue
    d = groups.groups()[0]
    if datasets.has_key(d):
        files.append(file)
    line = temp.readline()

for file in files:
    output_dir = output_events_dir
    if "outlier" in file:
        output_dir = output_outliers_dir
    filename = os.path.basename(os.path.dirname(file)) + "-" + os.path.basename(file)
    print filename
    subprocess.call(['hdfs dfs -getmerge ' + file + ' ' + os.path.join(output_dir, filename)], shell=True)
    print ""
