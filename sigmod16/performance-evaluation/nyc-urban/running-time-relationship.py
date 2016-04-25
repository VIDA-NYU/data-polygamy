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

datasets = ["311","crash","taxispeed","citibike","weather","gas-prices"]

temp = {"hour": ["hour","day","week","month"],
        "day": ["day","week","month"],
        "week": ["week","month"],
        "month": ["month"]}

spatial = {"nbhd": ["nbhd","city"],
           "city": ["city"]}

if sys.argv[1] == "help":
    print "[metadata-dir] [file] [y label]"
    sys.exit(0)

phases = ["aggregates", "index", "relationship-restricted"]
legends = {"aggregates": "Scalar Function Computation",
           "index": "Event Computation",
           "relationship-restricted": "Relationship Computation"}
phases_color = {"aggregates": "#003399",
                "index": "#0099CC",
                "relationship-restricted": "#003399"}

metadata_dir = sys.argv[1]
use_y_label = eval(sys.argv[3])

data = {"aggregates": [],
        "index": [],
        "relationship-restricted": []}
f = open(sys.argv[2])
line = f.readline()
current_n_datasets = 0
max_n_datasets = -1
min_n_datasets = sys.maxint
while line != "":
    if line.endswith("datasets\n"):
        current_n_datasets = int(line.split(" ")[0])
        max_n_datasets = max(max_n_datasets, current_n_datasets)
        min_n_datasets = min(min_n_datasets, current_n_datasets)
    elif line.startswith("No aggregates"):
        pass
    elif line.startswith("No indices"):
        pass
    elif line.strip() == "\n":
        pass
    else:
        l = line.split("\t")
        data[l[0]].append([current_n_datasets, int(l[1])])
    line = f.readline()

def get_number_relationships(data_dir, datasets, n):
    
    n_relationships = 0
    n_datasets = datasets[:n]

    dataset_att = {}
    dataset_res = {}

    # searching for files
    files = os.listdir(data_dir)
    for file in files:
        for dataset in n_datasets:
            data = dataset + "-"
            if data in file and not file.startswith("."):
                l = file.replace(".aggregates","").split("-")
                spatial_res = l[-1]
                temp_res = l[-2]
                dataset_res[dataset] = []
                for t in temp[temp_res]:
                    for s in spatial[spatial_res]:
                        dataset_res[dataset].append(t + "-" + s)

                f = open(os.path.join(data_dir,file))
                line = f.readline()
                dataset_att[dataset] = int(f.readline().strip())
                f.close()

    for i in range(len(n_datasets)):
        ds1 = n_datasets[i]
        for j in range(1, len(n_datasets)):
            ds2 = n_datasets[j]
            for res in dataset_res[ds1]:
                if (res in dataset_res[ds2]):
                    n_relationships += dataset_att[ds1]*dataset_att[ds2]

    return n_relationships

# plots
xlabel = "Number of Data Sets"
ylabel = "Evaluation Rate (rel. / min)"

plt.figure(figsize=(8, 6), dpi=80)

f, ax = plt.subplots()
ax.set_axis_bgcolor("#E0E0E0")

output = ""

n_relationships = []
n_datasets = []
for i in range(2, current_n_datasets+1):
    n_datasets.append(i)
    n_relationships.append(get_number_relationships(metadata_dir, datasets, i))

phase = "relationship-restricted"
x_data = []
y_data = []
for i in range(len(data[phase])):
    x_data.append(n_datasets[i])
    y_data.append(n_relationships[i]/(float(data[phase][i][1])/60000))

line, = ax.plot(x_data, y_data, linewidth=2.0, color=phases_color[phase], linestyle='-')
output += str([x for x in n_relationships]) + ": " + str([x[1] for x in data[phase]]) + "\n"

ax.tick_params(axis='both', labelsize=22)

plt.minorticks_off()

def my_formatter_fun_y(x, p):
    if (x/1000) >= 1:
        return locale.format('%d', x/1000, grouping=True) + "K"
    else:
        return locale.format('%d', x, grouping=True)
ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(my_formatter_fun_y))

def my_formatter_fun(x, p):
    if (x/1000) >= 1:
        return locale.format('%d', x/1000, grouping=True) + "K"
    else:
        return locale.format('%d', x, grouping=True)
ax.get_xaxis().set_major_formatter(ticker.FuncFormatter(my_formatter_fun))

ax.spines["top"].set_visible(False)
ax.spines["bottom"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)

ax.set_ylim(0,12000)
ax.set_xlabel(xlabel,fontproperties=font.FontProperties(size=22,weight='bold'))
if (use_y_label):
    ax.set_ylabel(ylabel,fontproperties=font.FontProperties(size=22,weight='bold'))

ax.grid(b=True, axis='both', color='w', linestyle='-', linewidth=0.7)
ax.set_axisbelow(True)

filename = "running-time-relationship"
plt.savefig(filename + ".png", bbox_inches='tight', pad_inches=0.05)
f = open(filename + ".out", "w")
f.write(output)
f.close()
plt.clf()
