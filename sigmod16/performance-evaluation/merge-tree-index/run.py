# Copyright (C) 2016 New York University
# This file is part of Data Polygamy which is released under the Revised BSD License
# See file LICENSE for full license details.

import os
import sys
import subprocess

data_1d = {0: [0,0]}
data_3d = {0: [0,0]}
last = 60
os.environ['HADOOP_HEAPSIZE'] = "100000"

# 1D

for i in range(1, last + 1):
    p = subprocess.Popen("hadoop jar ../../data-polygamy.jar edu.nyu.vida.data_polygamy.exp.StandaloneExp " +
                         " ".join([str(i), "../../taxi-time-series/taxi-city-timeseries", "../../../data/neighborhood-graph.txt", "../../../data/neighborhood.txt", "False"]),
                         shell=True, stdout=subprocess.PIPE, close_fds=True)
    stdout = p.stdout.read().replace("\n", "")
    index_time = int(stdout.split("\t")[1])
    query_time = int(stdout.split("\t")[2])

    data_1d[i] = [index_time, query_time]

# 3D

for i in range(1, last + 1):
    p = subprocess.Popen("hadoop jar ../../data-polygamy.jar edu.nyu.vida.data_polygamy.exp.StandaloneExp " +
                         " ".join([str(i), "../../taxi-time-series/taxi-nbhd-timeseries", "../../../data/neighborhood-graph.txt", "../../../data/neighborhood.txt", "True"]),
                         shell=True, stdout=subprocess.PIPE, close_fds=True)
    stdout = p.stdout.read().replace("\n", "")
    index_time = int(stdout.split("\t")[1])
    query_time = int(stdout.split("\t")[2])

    data_3d[i] = [index_time, query_time]

    stdout_previous = None
    
    while (index_time < data_3d[i-1][0]):
        print "Have to repeat previous..."
        print "   Previous index is " + str(data_3d[i-1][0]) + " and query is " + str(data_3d[i-1][1])
        print "   This index is " + str(index_time) + " and query is " + str(query_time)
        p = subprocess.Popen("hadoop jar ../../data-polygamy.jar edu.nyu.vida.data_polygamy.exp.StandaloneExp " +
                             " ".join([str(i), "../../taxi-time-series/taxi-nbhd-timeseries", "../../../data/neighborhood-graph.txt", "../../../data/neighborhood.txt", "True"]),
                             shell=True, stdout=subprocess.PIPE, close_fds=True)
        stdout_previous = p.stdout.read().replace("\n", "")
        data_3d[i-1][0] = int(stdout_previous.split("\t")[1])
        data_3d[i-1][1] = int(stdout_previous.split("\t")[2])

    if (stdout_previous):
        print "(NEW) " + stdout_previous
    print stdout

f = open("standalone.out", "w")
# 1D
f.write("1D:\n")
for i in range(1, last + 1):
    f.write("%d\t%d\t%d\n"%(i, data_1d[i][0], data_1d[i][1]))
# 2D
f.write("3D:\n")
for i in range(1, last + 1):
    f.write("%d\t%d\t%d\n"%(i, data_3d[i][0], data_3d[i][1]))
f.close()
