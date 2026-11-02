"""Plot per-op wall time across the cumulative optimisation steps, print the table.

Usage: python plot_cumulative.py <logs_dir> <out_prefix>
Reads step<k>_<n>.log, writes <out_prefix>.png and .pdf.
"""

import glob
import re
import statistics
import sys

import matplotlib.pyplot as plt

logs, out = sys.argv[1], sys.argv[2]

STEPS = ["baseline", "+ broadcast", "+ write once,\ncache O2", "+ clean once", "+ coalesce\nO2 cache"]
OPS = ["M2", "M3", "O2", "O4"]
# validated categorical slots 1-4 (dataviz palette)
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, MUTED = "#1f1f1e", "#6b6b66"


def walls(path):
    """Wall time per profiled block of one run."""
    lines = open(path).read().splitlines()
    # the last profiler table is the complete one
    start = max(i for i, l in enumerate(lines) if l.startswith("run "))
    result = {}
    for l in lines[start + 1 :]:
        m = re.match(r"^(clean|M1|M2|M3|O1|O2|O4)-features:\S*.*?\.\.\.\s+(\S+)", l)
        if m:
            result[m.group(1)] = float(m.group(2))
    return result


runs = {}
for path in glob.glob(f"{logs}/step*_*.log"):
    step = int(re.search(r"step(\d+)_\d+\.log$", path).group(1))
    runs.setdefault(step, []).append(walls(path))

blocks = ["clean", "M1", "M2", "M3", "O1", "O2", "O4"]
med = {
    s: {b: statistics.median(r.get(b, 0.0) for r in runs[s]) for b in blocks}
    for s in sorted(runs)
}
totals = {s: [sum(r.values()) for r in runs[s]] for s in sorted(runs)}

print("step " + " ".join(blocks) + " total[min-max] runs")
for s in sorted(med):
    t = totals[s]
    print(
        f"{s} "
        + " ".join(f"{med[s][b]:.1f}" for b in blocks)
        + f" {statistics.median(t):.1f}[{min(t):.1f}-{max(t):.1f}] {len(t)}"
    )

fig, ax = plt.subplots(figsize=(6.4, 3.4), dpi=200)
width = 0.2
steps = sorted(med)
for i, (op, color) in enumerate(zip(OPS, COLORS)):
    xs = [s - 1 + (i - 1.5) * width for s in steps]
    ys = [med[s][op] for s in steps]
    ax.bar(xs, ys, width * 0.9, color=color, label=op, zorder=2)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
for side in ("left", "bottom"):
    ax.spines[side].set_color(MUTED)
ax.tick_params(colors=MUTED, labelcolor=INK, labelsize=8)
ax.grid(axis="y", color="#e4e4e0", linewidth=0.8, zorder=0)
ax.set_xticks([s - 1 for s in steps])
ax.set_xticklabels([STEPS[s - 1] for s in steps])
ax.set_ylabel("wall time (s), median of 3", color=INK, fontsize=9)
ax.legend(frameon=False, fontsize=8, labelcolor=INK, ncols=4, loc="upper right")
fig.tight_layout()
fig.savefig(f"{out}.png")
fig.savefig(f"{out}.pdf")
