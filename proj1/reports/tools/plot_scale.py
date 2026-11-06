"""Plot `all` wall time vs dataset fraction from the scalability logs, print the table.

Usage: python plot_scale.py <logs_dir> <out_prefix>
Reads sample_f<f>_<n>.log and months_m<k>_<n>.log (local), or w3_s<f>_<n>.log, w3_m<k>_<n>.log
and w3_opt_<n>.log as the full point (Docker session), writes <out_prefix>.png and .pdf.
"""

import glob
import re
import statistics
import sys

import matplotlib.pyplot as plt

logs, out = sys.argv[1], sys.argv[2]
MONTHS = 8  # months in the full local dataset

# validated categorical slots 1-2 (dataviz palette), gray for the reference line
BLUE, ORANGE, GRAY, INK, MUTED = "#2a78d6", "#eb6834", "#8a8a85", "#1f1f1e", "#6b6b66"


def run_total(path):
    """Sum of the wall times of every profiled block (clean write + ops) of one run."""
    lines = open(path).read().splitlines()
    # the last profiler table is the complete one
    start = max(i for i, l in enumerate(lines) if l.startswith("run "))
    walls = {}
    for l in lines[start + 1 :]:
        m = re.match(r"^(clean|M1|M2|M3|O1|O2|O4)-features:\S*.*?\.\.\.\s+(\S+)", l)
        if m:
            walls[m.group(1)] = float(m.group(2))
    return sum(walls.values()), walls.get("clean", 0.0)


def series(pattern, fraction_of):
    points = {}
    for path in glob.glob(f"{logs}/{pattern}"):
        key = re.search(r"_([fs][\d.]+|m\d+|opt)_\d+\.log$", path).group(1)
        points.setdefault(fraction_of(key), []).append(run_total(path))
    rows = []
    for f in sorted(points):
        totals = [t for t, _ in points[f]]
        cleans = [c for _, c in points[f]]
        rows.append(
            (
                f,
                statistics.median(totals),
                min(totals),
                max(totals),
                statistics.median(cleans),
                len(totals),
            )
        )
    return rows


months = series("*_m[0-9]*_*.log", lambda k: int(k[1:]) / MONTHS)
sample = series("*_[fs]0.*_*.log", lambda k: float(k[1:]))
if not any(r[0] == 1.0 for r in months):
    # Docker session: the full-data runs are <prefix>_opt_<n>.log
    months += series("*_opt_*.log", lambda k: 1.0)
full = next(r for r in months if r[0] == 1.0)
sample.append(full)  # the full dataset is fraction 1.0 of both series

print("series fraction median_s min_s max_s clean_s runs time/full")
for name, rows in (("months", months), ("sample", sample)):
    for f, med, lo, hi, clean, n in rows:
        print(
            f"{name} {f:.3f} {med:.1f} {lo:.1f} {hi:.1f} {clean:.1f} {n} {med / full[1]:.2f}"
        )

fig, ax = plt.subplots(figsize=(6.4, 3.8), dpi=200)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
for side in ("left", "bottom"):
    ax.spines[side].set_color(MUTED)
ax.tick_params(colors=MUTED, labelcolor=INK, labelsize=9)
ax.grid(axis="y", color="#e4e4e0", linewidth=0.8)
ax.set_axisbelow(True)

ax.plot(
    [0, 1],
    [0, full[1]],
    linestyle="--",
    color=GRAY,
    linewidth=1.5,
    label="linear scaling (proportional to full)",
)
for rows, color, marker, label in (
    (months, BLUE, "o", "fewer month files (input shrinks)"),
    (sample, ORANGE, "s", "sample fraction (all files still scanned)"),
):
    xs = [r[0] for r in rows]
    ys = [r[1] for r in rows]
    err = [[r[1] - r[2] for r in rows], [r[3] - r[1] for r in rows]]
    ax.errorbar(
        xs,
        ys,
        yerr=err,
        color=color,
        marker=marker,
        markersize=6,
        linewidth=2,
        capsize=3,
        label=label,
    )

ax.set_xlim(0, 1.05)
ax.set_ylim(0, full[1] * 1.15)
ax.set_xlabel(
    "fraction of the 2026-01..08 dataset (29.7 M records)", color=INK, fontsize=9
)
ax.set_ylabel("wall time of all operations (s), median of 3", color=INK, fontsize=9)
ax.legend(frameon=False, fontsize=8, labelcolor=INK, loc="upper left")
fig.tight_layout()
fig.savefig(f"{out}.png")
fig.savefig(f"{out}.pdf")
