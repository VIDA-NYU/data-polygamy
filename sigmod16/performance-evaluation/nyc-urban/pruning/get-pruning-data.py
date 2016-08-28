# Copyright (C) 2016 New York University
# This file is part of Data Polygamy which is released under the Revised BSD License
# See file LICENSE for full license details.

import sys
import os
import subprocess

events = sys.argv[1]
perm = sys.argv[2]
temp_res = ["hour","day","week","month"]
spatial_res = ["nbhd","city"]
#score = ["0.6", "0.8", "0.9"]
score = ["0.6","0.8"]

for temp in temp_res:
    for spatial in spatial_res:
        for s in score:
            subprocess.call(["hadoop jar ../../../data-polygamy.jar edu.nyu.vida.data_polygamy.exp.DataRelationships relationship ../output-" + events + "/ ../metadata/ " + " " + events + " " + perm + " " + s + " 0 0.05 " + temp + " " + spatial + " results/"], shell=True)

