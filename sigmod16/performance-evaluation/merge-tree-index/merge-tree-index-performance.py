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
locale.setlocale(locale.LC_ALL, 'en_US.utf8')

colors = {"index": "#003399",
          "query": "#0099CC"}

def consume(index_query, months):
   new_index_query = []
   new_months = []
   previous_query = None
   previous_index = None
   changed = False
   for i in range(len(index_query)):
      index = int(index_query[i][0])
      query = int(index_query[i][1])
      if previous_index:
         if index < previous_index:
            changed = True
            del new_index_query[-1]
            del new_months[-1]
         elif query < previous_query:
            changed = True
            del new_index_query[-1]
            del new_months[-1]
      new_index_query.append(index_query[i])
      previous_index = index
      previous_query = query
      new_months.append(months[i])

   return (new_index_query, new_months, changed)

_1d = []
_2d = []
months_1d = []
months_2d = []

f = open("standalone.out")
line = f.readline()
for i in range(60):
   line = f.readline()
   index = line.split("\t")[1]
   query = line.split("\t")[2]
   _1d.append((index, query))
   months_1d.append(int(line.split("\t")[0]))
line = f.readline()
for i in range(55):
   line = f.readline()
   index = line.split("\t")[1]
   query = line.split("\t")[2]
   _2d.append((index, query))
   months_2d.append(int(line.split("\t")[0]))

(_1d, months_1d, changed) = consume(_1d,months_1d)
while changed:
    (_1d, months_1d, changed) = consume(_1d,months_1d)

(_2d, months_2d, changed) = consume(_2d,months_2d)
while changed:
    (_2d, months_2d, changed) = consume(_2d,months_2d)

# plot 1D

xlabel = "Size of Graph (# Edges)"
ylabel = "Time (s)"

plt.figure(figsize=(8, 6), dpi=80)

f, ax = plt.subplots()
ax.set_axis_bgcolor("#E0E0E0")

x_data = [(i*720)-1 for i in months_1d]
line_index, = ax.plot(x_data, [float(x[0])/1000000000 for x in _1d], linewidth=2.0, color=colors["index"], linestyle='-')
line_query, = ax.plot(x_data, [float(x[1])/1000000000 for x in _1d], linewidth=2.0, color=colors["query"], linestyle='-')

plot_legend = ax.legend([line_index,line_query],
                        ["Index Creation Time","Index Querying Time"],
                        handlelength=3,
                        loc='upper left',
                        borderpad=0.5,
                        shadow=True,
                        prop={'size':16,'style':'italic'})
plot_legend.get_frame().set_lw(0.5)

ax.tick_params(axis='both', labelsize=22)

def my_formatter_fun(x, p):
    if (x/1000) >= 1:
        return locale.format('%d', x/1000, grouping=True) + "K"
    else:
        return locale.format('%d', x, grouping=True)
ax.get_xaxis().set_major_formatter(ticker.FuncFormatter(my_formatter_fun))

ax.set_xticks([x for x in range(0,45000,10000)])

ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)

ax.set_xlabel(xlabel,fontproperties=font.FontProperties(size=22,weight='bold'))
ax.set_ylabel(ylabel,fontproperties=font.FontProperties(size=22,weight='bold'))

ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
ax.set_axisbelow(True)

filename = "standalone-index-1d"
plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
plt.clf()

# plot 3D

xlabel = "Size of Graph (# Edges)"
ylabel = "Time (s)"

plt.figure(figsize=(8, 6), dpi=80)

f, ax = plt.subplots()
ax.set_axis_bgcolor("#E0E0E0")

x_data = [(i*30*24)*550 + 260*((i*30*24)-1) for i in months_2d]
line_index, = ax.plot(x_data, [float(x[0])/1000000000 for x in _2d], linewidth=2.0, color=colors["index"], linestyle='-')
line_query, = ax.plot(x_data, [float(x[1])/1000000000 for x in _2d], linewidth=2.0, color=colors["query"], linestyle='-')

plot_legend = ax.legend([line_index,line_query],
                        ["Index Creation Time","Index Querying Time"],
                        handlelength=3,
                        loc='upper left',
                        borderpad=0.5,
                        shadow=True,
                        prop={'size':16,'style':'italic'})
plot_legend.get_frame().set_lw(0.5)

ax.tick_params(axis='both', labelsize=22)

def my_formatter_fun(x, p):
    if (x/1000000) >= 1:
        return locale.format('%d', x/1000000, grouping=True) + "M"
    elif (x/1000) >= 1:
        return locale.format('%d', x/1000, grouping=True) + "K"
    else:
        return locale.format('%d', x, grouping=True)
ax.get_xaxis().set_major_formatter(ticker.FuncFormatter(my_formatter_fun))

ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)

ax.set_xlim(xmax=33000000)
ax.set_xlabel(xlabel,fontproperties=font.FontProperties(size=22,weight='bold'))

ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
ax.set_axisbelow(True)

filename = "standalone-index-3d"
plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
plt.clf()
