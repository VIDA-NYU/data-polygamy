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
from operator import itemgetter

if sys.argv[1] == "help":
    print "[dir] [events] [permutation] [temp res] [spatial res]"
    sys.exit(0)

scores = ["0.6", "0.8"]
scores_color = {"0.6": "#0099CC",
                "0.8": "#66CCFF"}

dir = sys.argv[1]
events = sys.argv[2]
perm = sys.argv[3]
temp_res = sys.argv[4]
spatial_res = sys.argv[5]
use_y_label = eval(sys.argv[6])

def generate_data(score, events, perm, temp_res, spatial_res):

    partial_filename = score + "-" + events + "-" + perm
    partial_filename += "-" + temp_res + "-" + spatial_res + "-"

    # searching for files
    sparsity_files = []
    files = os.listdir(dir)
    for file in files:
        if file.startswith(partial_filename):
            n_datasets = int(file.split("-")[5].replace(".out",""))
            sparsity_files.append((n_datasets, os.path.join(dir,file)))
    sparsity_files = sorted(sparsity_files, key=itemgetter(0))

    # getting x and y axis
    sign_edges_data = []
    sign_edges_ = []
    score_edges_data = []
    score_edges_ = []
    max_edges_data = []
    max_edges_ = []
    for file in sparsity_files:
        f = open(file[1])
        x = int(f.readline().split(":")[1].strip())
        att = int(f.readline().split(":")[1].strip())
        max_edges_str = f.readline().split(":")
        max_edges = float(max_edges_str[1].strip())
        sign_edges_str = f.readline().split(":")
        sign_edges = float(sign_edges_str[1].strip())
        score_edges_str = f.readline().split(":")
        score_edges = float(score_edges_str[1].strip())
        f.close()
        
        sign_edges_log = -1
        try:
            sign_edges_log = math.log10(sign_edges)
        except:
            pass
            
        score_edges_log = -1
        try:
            score_edges_log = math.log10(score_edges)
        except:
            pass
            
        max_edges_log = -1
        try:
            max_edges_log = math.log10(max_edges)
        except:
            pass
        
        sign_edges_data.append((x,sign_edges_log))
        sign_edges_.append((x,sign_edges))
        score_edges_data.append((x,score_edges_log))
        score_edges_.append((x,score_edges))
        max_edges_data.append((x,max_edges_log))
        max_edges_.append((x,max_edges))
        
    return (max_edges_data, sign_edges_data, score_edges_data,
            max_edges_, sign_edges_, score_edges_)

# plots
xlabel = "Number of Data Sets"
ylabel = "Number of Relationships"

plt.figure(figsize=(8, 6), dpi=80)

f, ax = plt.subplots()
ax.set_axis_bgcolor("#E0E0E0")

output = ""

refs = []
legend = []
sign_edges_data = []
max_edges_data = []
for score in scores:
    score_data = []
    data = generate_data(score, events, perm, temp_res, spatial_res)
    (max_edges_data, sign_edges_data, score_data, max_edges_, sign_edges_, score_edges_) = data
    line, = ax.plot([x[0] for x in score_data],
                    [x[1] for x in score_data],
                    color=scores_color[score],
                    linestyle='-',
                    linewidth=2.0)
    #ax.plot([x[0] for x in score_data],
    #        [x[1] for x in score_data],
    #        linestyle='None',
    #        mec="k",
    #        mfc="k",
    #        marker='x',
    #        ms=8.0,
    #        mew=1.5)
    output += score + ": " + str(score_edges_) + "\n"
    refs.append(line)
    legend.append(r"Significant Relationships, $|\tau| \geq %.2f$" %(float(score)))

output += "Significant: " + str(sign_edges_) + "\n"
output += "Max: " + str(max_edges_) + "\n"

line1, = ax.plot([x[0] for x in sign_edges_data],
                 [x[1] for x in sign_edges_data],
                 color="#003399",
                 linestyle='-',
                 linewidth=2.0)
#ax.plot([x[0] for x in sign_edges_data],
#        [x[1] for x in sign_edges_data],
#        linestyle='None',
#        mec="k",
#        mfc="k",
#        marker='x',
#        ms=8.0,
#        mew=1.5)
line2, = ax.plot([x[0] for x in max_edges_data],
                 [x[1] for x in max_edges_data],
                 'k--',
                 linewidth=2.0)
#ax.plot([x[0] for x in max_edges_data],
#        [x[1] for x in max_edges_data],
#        linestyle='None',
#        mec="k",
#        mfc="k",
#        marker='x',
#        ms=8.0,
#        mew=1.5)

refs = [line2, line1] + refs
legend = ["Possible Relationships", "Significant Relationships"] + legend

plot_legend = ax.legend(refs, legend, handlelength=3, loc='upper left', borderpad=0.5, shadow=True, prop={'size':15,'style':'italic'})
plot_legend.get_frame().set_lw(0.5)

ax.set_yticks([0,1,2,3,4])
ax.set_yticklabels(["1","10","100","1K","10K"])
ax.set_ylim(-1.1, 5)

ax.tick_params(axis='both', labelsize=22)

ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)

ax.set_xlim(xmin=2)
ax.set_xlabel(xlabel,fontproperties=font.FontProperties(size=22,weight='bold'))
if (use_y_label):
    ax.set_ylabel(ylabel,fontproperties=font.FontProperties(size=22,weight='bold'))

ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
ax.set_axisbelow(True)

filename = events + "-" + perm + "-" + temp_res + "-" + spatial_res
plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
f = open(filename + ".out", "w")
f.write(output)
f.close()
plt.clf()
