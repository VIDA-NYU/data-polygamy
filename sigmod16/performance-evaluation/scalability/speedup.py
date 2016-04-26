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
#locale.setlocale(locale.LC_ALL, 'en_US.utf8')

if len(sys.argv) != 1:
    print "Usage: speedup.py"
    sys.exit(0)
               
colors = {'aggregation':   "#003399",
          'index':         "#0099CC",
          'relationship':  "#66CCFF"}
          
legend = {'aggregation':   "Scalar Function Computation",
          'index':         "Feature Identification",
          'relationship':  "Relationship Computation"}

n_nodes = [1, 2, 4, 8, 16]
phases = ['aggregation', 'index', 'relationship']
data = {}
for phase in phases:
    data[phase] = {}

f = open('times.out', 'r')
line = f.readline()
while line != '':
    line = line.replace("\n","")
    if line == '':
        line = f.readline()
        continue
    # number of nodes
    n_node = int(line.split(":")[1].strip())
    line = f.readline()
    # aggregation
    data['aggregation'][n_node] = float(line.split("\t")[1].strip())
    line = f.readline()
    # index creation
    data['index'][n_node] = float(line.split("\t")[1].strip())
    line = f.readline()
    # relationship
    data['relationship'][n_node] = float(line.split("\t")[1].strip())
    line = f.readline()
    
# plots
xlabel = "Number of Nodes"
ylabel = "Speedup"

plt.figure(figsize=(8, 6), dpi=80)
    
f, ax = plt.subplots()
ax.set_axis_bgcolor("#E0E0E0")

output = ""

refs = []
legends = []
for phase in phases:

    output += "%s\n"%phase
    
    x_data = []
    y_data = []
    
    for x in n_nodes[1:]:
        x_data.append(x)
        y_data.append(data[phase][n_nodes[0]]/data[phase][x])
        output += str(x) + " nodes\t" + str(y_data[-1]) + "\n"
    line, = ax.plot(x_data, y_data, linewidth=2.0, color=colors[phase], linestyle='-', marker='|', mec=colors[phase], mfc=colors[phase], ms=12, mew=2)
    ghost_line, = ax.plot([-1,-2], [-1,-2], linewidth=2.0, color=colors[phase], linestyle='-')
    refs.append(ghost_line)
    legends.append(legend[phase])
    
plot_legend = ax.legend(refs, legends, handlelength=3, loc='upper left', borderpad=0.5, shadow=True, prop={'size':15,'style':'italic'})
plot_legend.get_frame().set_lw(0.5)

ax.set_yticks(range(2,17,2))
    
ax.tick_params(axis='both', labelsize=22)
    
ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)
    
ax.set_xlim(1,17)
ax.set_ylim(1,17)

ax.set_xlabel(xlabel,fontproperties=font.FontProperties(size=22,weight='bold'))
ax.set_ylabel(ylabel,fontproperties=font.FontProperties(size=22,weight='bold'))
        
ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
ax.set_axisbelow(True)
    
filename = "speedup"
plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
f = open(filename + ".out", "w")
f.write(output)
f.close()
plt.clf()
    
