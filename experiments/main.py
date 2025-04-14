import json
import pathlib
import re
from argparse import ArgumentParser

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


parser = ArgumentParser(description="Plot execution time by item count, delay, and configuration.")
parser.add_argument(
    "--dir",
    type=str,
    default="first",
    help="Directory containing the JSON files with the results."
)
args = parser.parse_args()

res_dir = pathlib.Path(args.dir)

HEATMAP_FORMULAS = [
    "renoir / flowunits",
    "log(renoir / flowunits)",
    "flowunits / renoir * 100",
    "(1 - flowunits / renoir) * 100",
]

rows = []
for file_path in res_dir.glob("*.json"):
    print(f"Processing {file_path}")

    haystack = file_path.stem
    match = re.match(r"(\d+ms)-(\d+\wbit)", haystack)
    if not match: raise ValueError(f"File name {haystack} does not match expected pattern.")
    delay = str(match.group(1))
    bandwidth = str(match.group(2))
    if bandwidth == "0mbit":
        bandwidth = "unlimited"
    
    with open(file_path, 'r') as f:
        results = json.load(f)["results"]
        for entry in results:
            data = {}
            data["config"] = entry["parameters"]["type"]
            data["size"] = str(entry["parameters"]["size"])
            data["execution time"] = entry["mean"]
            data["stddev"] = entry["stddev"]
            data["min"] = entry["min"]
            data["max"] = entry["max"]
            data["bandwidth"] = bandwidth
            data["delay"] = delay
            rows.append(data)
df = pd.DataFrame(rows)
df = df[df['delay'] != '1ms']

keys_delays = ["0ms", "10ms", "100ms"] # todo change to 10 ms
keys_bandwidths = ["unlimited",  "1gbit", "100mbit", "10mbit" ] # unlimited

pivot_df = df[df['size'] == "10000000"].pivot_table(index=['delay', 'bandwidth'], columns='config', values='execution time')

pivot_df['ratio'] = pivot_df['renoir'] / pivot_df['flowunits']

pivot_df = pivot_df.reset_index()

pivot_df['delay'] = pd.Categorical(pivot_df['delay'], categories=keys_delays, ordered=True)
pivot_df['bandwidth'] = pd.Categorical(pivot_df['bandwidth'], categories=keys_bandwidths, ordered=True)
pivot_df = pivot_df.sort_values(['delay', 'bandwidth'])

fig = sns.heatmap(
    np.log2(pivot_df['ratio'].values.reshape(len(keys_delays), len(keys_bandwidths))),
    annot=pivot_df['ratio'].values.reshape(len(keys_delays), len(keys_bandwidths)),
    fmt=".2f",
    cmap="crest",
    linewidths=0.5,
    annot_kws={"size": 14},
    xticklabels=keys_bandwidths,
    yticklabels=keys_delays,
    cbar=False,
)

fig.tick_params(axis='both', which='major', labelsize=14)
fig.set_xlabel("Bandwidth", fontdict={"size": 14, "weight": "bold"})
fig.set_ylabel("Delay", fontdict={"size": 14, "weight": "bold"})
plt.yticks(rotation=0)

plt.tight_layout()
plt.show()