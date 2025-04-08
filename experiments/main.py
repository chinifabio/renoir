import json
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors
from argparse import ArgumentParser
import pathlib
# import seaborn as sns

parser = ArgumentParser(description="Plot execution time by item count, delay, and configuration.")
parser.add_argument(
    "--dir",
    type=str,
    default="first",
    help="Directory containing the JSON files with the results."
)
args = parser.parse_args()

res_dir = pathlib.Path(args.dir)
files = {}
palette_map = {
    "renoir": {},
    "flowunits": {}
}

keys = []
for file_path in res_dir.glob("*.json"):
    delay = file_path.stem
    keys.append(delay)
    files[delay] = file_path

    renoir_color = mcolors.to_rgba("blue", 1 - (keys.index(delay) * 0.05))
    flowunits_color = mcolors.to_rgba("red", 1 - (keys.index(delay) * 0.05))

    palette_map["renoir"][delay] = renoir_color
    palette_map["flowunits"][delay] = flowunits_color

# files = {
#     "1ms": "data/1ms.json",
#     "20ms": "data/20ms.json",
#     "100ms": "data/100ms.json"
# }

# flare_palette = sns.color_palette("flare", n_colors=3)
# crest_palette = sns.color_palette("crest", n_colors=3)
# delays = ["1ms", "20ms", "100ms"]
# palette_map = {
#     "renoir": dict(zip(delays, flare_palette)),
#     "flowunits": dict(zip(delays, crest_palette)),
# }

data = []
for delay, file_path in files.items():
    with open(file_path, 'r') as f:
        results = json.load(f)["results"]
        for entry in results:
            config = entry["parameters"]["type"]
            size = entry["parameters"]["size"]
            mean = entry["mean"]
            label = f"{int(float(size)):,}"
            color = mcolors.to_rgba(palette_map[config][delay])
            data.append({
                "delay": delay,
                "config": config,
                "size": label,
                "mean": mean,
                "color": color
            })

sizes = sorted(list(set(d["size"] for d in data)), key=lambda x: float(x.replace(",", "")))
configs = ["flowunits", "renoir"]
keys = list(files.keys())

x_labels = sizes
x = np.arange(len(x_labels))
bar_width = 0.1

offsets = []
for i, delay in enumerate(keys):
    for j, config in enumerate(configs):
        offsets.append(((i * len(configs) + j) - 2.5) * bar_width)

fig, ax = plt.subplots(figsize=(12, 6))

for idx, entry in enumerate(data):
    x_pos = sizes.index(entry["size"]) + offsets[keys.index(entry["delay"]) * 2 + configs.index(entry["config"])]
    ax.bar(x_pos, entry["mean"], width=bar_width, color=entry["color"], label=f"{entry['delay']} - {entry['config']}")

ax.set_yscale("log")
ax.set_xticks(x)
ax.set_xticklabels(x_labels)
ax.set_xlabel("Item Count")
ax.set_ylabel("Execution Time (s)")
ax.set_title("Execution Time by Item Count, Delay, and Configuration")

handles, labels = ax.get_legend_handles_labels()
unique = dict(zip(labels, handles))
ax.legend(unique.values(), unique.keys(), title="Delay - Config")

plt.tight_layout()
plt.grid(True, which="both", axis="y", linestyle="--", linewidth=0.5)
plt.savefig(res_dir / "execution_time_by_item_count.pdf", dpi=300)