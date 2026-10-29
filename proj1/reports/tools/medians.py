"""Print per-op metrics of each run and the median wall time from profiler report tables."""
import re
import statistics
import sys
from collections import defaultdict

COLS = ["wall_time_s", "num_jobs", "num_stages", "num_tasks", "executor_run_time_ms",
        "executor_cpu_time_ms", "input_bytes", "output_bytes", "shuffle_read_bytes",
        "shuffle_write_bytes", "memory_spilled_bytes", "disk_spilled_bytes"]

runs = defaultdict(list)
for path in sys.argv[1:]:
    lines = open(path).read().splitlines()
    # the last profiler table is the complete one
    start = max(i for i, l in enumerate(lines) if l.startswith("run "))
    for l in lines[start + 1:]:
        m = re.match(r"^(M1|M2|M3|O1|O2|O4)-features:\S*.*?\.\.\.\s+(.*)$", l)
        if not m:
            continue
        values = m.group(2).split()
        runs[m.group(1)].append((path.rsplit("/", 1)[-1], dict(zip(COLS, values))))

for op, rows in runs.items():
    print(f"== {op}")
    for name, r in rows:
        print(f"  {name}: " + " ".join(f"{k}={r[k]}" for k in COLS))
    walls = [float(r["wall_time_s"]) for _, r in rows]
    print(f"  median wall={statistics.median(walls):.2f} min={min(walls)} max={max(walls)}")
