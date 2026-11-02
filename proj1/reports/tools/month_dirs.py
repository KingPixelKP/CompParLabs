"""Create data dirs with the first 1, 2, 4 and 8 month files, for the dataset-scale series.

Usage: python month_dirs.py <data_dir> <out_dir>
Writes <out_dir>/data_m<k>/ with the zone table and the first k trip files (symlinks, or
copies where symlinks are not allowed, e.g. Windows without developer mode).
"""

import pathlib
import shutil
import sys

data, out = pathlib.Path(sys.argv[1]).resolve(), pathlib.Path(sys.argv[2])
trips = sorted(data.glob("yellow_tripdata_*.parquet"))
zones = data / "taxi_zone_lookup.csv"

for k in (1, 2, 4, 8):
    target = out / f"data_m{k}"
    target.mkdir(parents=True, exist_ok=True)
    for src in [zones, *trips[:k]]:
        dst = target / src.name
        if dst.exists():
            continue
        try:
            dst.symlink_to(src)
        except OSError:
            shutil.copy2(src, dst)
    print(target, len(list(target.iterdir())), "files")
