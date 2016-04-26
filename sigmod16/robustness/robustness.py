# Copyright (C) 2016 New York University
# This file is part of Data Polygamy which is released under the Revised BSD License
# See file LICENSE for full license details.

import os
import sys
import math
import matplotlib
matplotlib.use('Agg')
matplotlib.rc('font', family='sans-serif')
import matplotlib.pyplot as plt
import matplotlib.font_manager as font
import matplotlib.ticker as ticker
import locale
locale.setlocale(locale.LC_ALL, 'en_US')

if sys.argv[1] == "help":
    print "[file] [dimension] [use title]"
    sys.exit(0)

file = sys.argv[1]
dimension = sys.argv[2]
use_title = eval(sys.argv[3])
color = "#003399"
attribute_name = {"count-db_idx": "Density of Taxis",
                  "unique-medallion_id": "Unique Number of Taxis",
                  "avg-miles": "Average Miles",
                  "avg-fare": "Average Taxi Fare"}

amplitude = []
attributes = {}
pValue = {}
score = {}
strength = {}

limit_score = {}
limit_strength = {}

f = open(file, "r")
line = f.readline()
current_amplitude = 0
while line != "":
    if "Amplitude" in line:
        current_amplitude = int(line.split(":")[1])
        amplitude.append(current_amplitude)
        line = f.readline()
    elif "Attribute" in line:
        att = line.split(":")[1].strip()
        attributes[att] = 0
        scoreAtt = float(f.readline().split(":")[1])
        strengthAtt = float(f.readline().split(":")[1])
        pValueAtt = float(f.readline().split(":")[1].replace("[not significant]",""))
        if (score.has_key(att)):
            score[att].append(scoreAtt)
        else:
            score[att] = [scoreAtt]
        if (strength.has_key(att)):
            strength[att].append(strengthAtt)
        else:
            strength[att] = [strengthAtt]
        if (pValue.has_key(att)):
            pValue[att].append(pValueAtt)
        else:
            pValue[att] = [pValueAtt]
        if (scoreAtt < 0.6) and (not limit_score.has_key(att)):
            limit_score[att] = current_amplitude
        if (len(strength[att]) > 1):
            if (strengthAtt > strength[att][-2]) and (not limit_strength.has_key(att)):
                limit_strength[att] = current_amplitude
        line = f.readline()
        
for att in attributes.keys():
    
    print "Attribute: " + att
    
    # Score

    xlabel = "Noise (%)"
    ylabel = "Relationship Score"
    
    plt.figure(figsize=(8, 6), dpi=80)
    
    f, ax = plt.subplots()
    ax.set_axis_bgcolor("#E0E0E0")
    
    line_score, = ax.plot([amplitude[i]/100.0 for i in range(0,1000,10)],
                          [score[att][i] for i in range(0,1000,10)],
                          linewidth=2.0, color=color, linestyle='-')
    
    ax.set_ylim([-1,1.01])
    
    ax.spines["top"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    
    ax.set_xlabel(xlabel,fontproperties=font.FontProperties(size=22,weight='bold'))
    ax.set_ylabel(ylabel,fontproperties=font.FontProperties(size=22,weight='bold'))
    
    ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
    ax.set_axisbelow(True)
    ax.tick_params(axis='both', labelsize=22)
    if use_title:
        ax.set_title(attribute_name[att],
                     fontproperties=font.FontProperties(style="italic",size=22,weight='bold'))
    
    filename = att + "-score-" + dimension
    plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
    plt.clf()
    
    # Strength

    xlabel = "Noise (%)"
    ylabel = "Relationship Strength"
    
    plt.figure(figsize=(8, 6), dpi=80)
    
    f, ax = plt.subplots()
    ax.set_axis_bgcolor("#E0E0E0")
    
    line_strength, = ax.plot([amplitude[i]/100.0 for i in range(0,1000,10)],
                             [strength[att][i] for i in range(0,1000,10)],
                             linewidth=2.0, color=color, linestyle='-')
    
    ax.set_ylim([0,1.01])
    
    ax.spines["top"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    
    ax.set_xlabel(xlabel,fontproperties=font.FontProperties(size=22,weight='bold'))
    ax.set_ylabel(ylabel,fontproperties=font.FontProperties(size=22,weight='bold'))
    
    ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
    ax.set_axisbelow(True)
    ax.tick_params(axis='both', labelsize=22)
    if use_title:
        ax.set_title(attribute_name[att],
                     fontproperties=font.FontProperties(style="italic",size=22,weight='bold'))
    
    filename = att + "-strength-" + dimension
    plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
    plt.clf()
    
    print ""
    
    # P-Value

    xlabel = "Noise (%)"
    ylabel = "p-value"
    
    plt.figure(figsize=(8, 6), dpi=80)
    
    f, ax = plt.subplots()
    ax.set_axis_bgcolor("#E0E0E0")
    
    line_pvalue, = ax.plot([amplitude[i]/100.0 for i in range(0,1000,10)],
                           [pValue[att][i] for i in range(0,1000,10)],
                           linewidth=2.0, color=color, linestyle='-')
    line_alpha, = ax.plot([amplitude[i]/100.0 for i in range(0,1000,10)],
                          [0.05 for i in range(0,1000,10)],
                          linewidth=2.0, color="k", linestyle='--')
    
    ax.text(9.7, 0.051, r"$\alpha$", ha='center', va='bottom',
            fontproperties=font.FontProperties(weight='bold',size='22'))
    
    ax.set_ylim(ymin=-0.005)
    
    ax.spines["top"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    
    ax.set_xlabel(xlabel, fontproperties=font.FontProperties(size=22,weight='bold'))
    ax.set_ylabel(ylabel, fontproperties=font.FontProperties(size=22,weight='bold'))
    
    ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
    ax.set_axisbelow(True)
    ax.tick_params(axis='both', labelsize=22)
    if use_title:
        ax.set_title(attribute_name[att],
                     fontproperties=font.FontProperties(style="italic",size=22,weight='bold'))
    
    filename = att + "-pvalue-" + dimension
    plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
    plt.clf()
