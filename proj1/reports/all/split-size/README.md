# All ops: smaller scan splits (`--max-partition-mb`)

- Date: 2026-10-30
- Data: 2026-01..08 (29.7 M records, 20.95 M after cleaning), local machine (see [baseline](../../baseline/README.md#experimental-setup))
- Before: `--broadcast-join --cache-dataframe` (cache limited to O2 after [reuse-results](../reuse-results/README.md)), `--max-partition-mb 128` (Spark default)
- After: the same with `--max-partition-mb 64 / 32 / 16 / 8`, commit TODO
- Result: **no gain, not adopted**

## Bottleneck observed

After [broadcast-join](../broadcast-join/README.md) the 18-task scan stage holds most of every op's task time, and its tasks are uneven. M3 stage 11 has 81.5 s of task time, with tasks of 66 ms (min), 4.6 s (median) and 9.1 s (max) ([sweep_stages_M3_128](./logs/sweep_stages_M3_128.txt)). With 8 cores, 18 tasks run in 3 waves, and the near-empty tasks suggest some cores have nothing to do.

## Hypothesis

Spark splits each parquet file into byte ranges of at most `min(maxPartitionBytes, max(openCostInBytes, totalBytes / defaultParallelism))`. Here that is 487 MiB / 8 ≈ 61 MiB, not the 128 MiB default. A split only reads the row groups whose midpoint falls inside it. The 8 files hold only **32 row groups** (4 per file, ~24 MiB uncompressed each), so splits that contain no midpoint read nothing (the 66 ms tasks), while others read 2 row groups.

Smaller splits should give one task per row group: about 32 even tasks, 4 full waves, and a shorter scan.

## Change

New option `--max-partition-mb` sets `spark.sql.files.maxPartitionBytes` before the trips are read (`proj1/src/proj1/__init__.py`, `main`). The default of 128 keeps Spark's default, so earlier runs are unaffected.

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", f"{features.max_partition_mb}m")
```

## Before / after

Sweep, **1 run per value** (`all` run: [sweep_128](./logs/sweep_128.log), [64](./logs/sweep_64.log), [32](./logs/sweep_32.log), [16](./logs/sweep_16.log), [8](./logs/sweep_8.log)). Wall time in seconds; the tasks column counts the tasks of the whole op.

| max-partition-mb | M2 | M3 | O2 | O4 | M1 | M3 tasks |
| ---------------- | -- | -- | -- | -- | -- | -------- |
| 128 (before) | 20.5 | 10.1 | 18.5 | 12.7 | 12.2 | 58 |
| 64 | 21.3 | 12.0 | 22.5 | 14.4 | 13.2 | 58 |
| 32 | 22.4 | 11.8 | 20.0 | 14.6 | 13.2 | 64 |
| 16 | 23.2 | 11.9 | 21.2 | 14.9 | 15.0 | 103 |
| 8 | 23.7 | 12.2 | 17.3 | 13.7 | 11.9 | 193 |

M3 scan stage per value ([sweep_stages_M3_*](./logs/)):

| max-partition-mb | Scan tasks | Task time total | Task time min / median / p75 / max |
| ---------------- | ---------- | --------------- | ---------------------------------- |
| 128 | 18 | 81.5 s | 0.07 / 4.6 / 8.2 / 9.1 s |
| 64 | 18 | 86.8 s | 0.05 / 5.5 / 8.3 / 9.6 s |
| 32 | 20 | 73.9 s | 0.07 / 3.9 / 7.2 / 7.9 s |
| 16 | 33 | 96.8 s | 0.05 / 2.8 / 4.1 / 7.1 s |
| 8 | 63 | 85.3 s | 0.03 / 0.1 / 2.5 / 6.0 s |

- **128 and 64 behave the same** (18 tasks), as expected: the effective split was already ~61 MiB.
- **Smaller splits add tasks but not useful parallelism.** At 8 MiB there are 63 tasks, but the median task takes 0.1 s: most splits contain no row-group midpoint and read nothing. The slowest task still takes 6.0 s, because a row group cannot be split.
- **Total task time stays at 74-97 s** whatever the split. The scan is CPU-bound (decode + filter of 21 M rows), and that work does not shrink. Wall time does not improve for any op, and is at best within noise. M1 varies from 11.9 to 15.0 s without any plan change, so differences under ~3 s are noise.

## Conclusion

The hypothesis is refuted. The unit of scan parallelism is the **parquet row group**, not the split. With 32 row groups on 8 cores the scan is already about as parallel as it can get, and smaller splits only add empty tasks and scheduling overhead. `--max-partition-mb` stays at 128.

What would help instead is more, smaller row groups: rewrite the input once with a smaller `parquet.block.size`. That is the backlog row "rewrite files once", which would also remove the schema `Union`. On the cluster, with more cores than row groups, this limit matters more: 32 row groups cap the scan at 32 concurrent tasks.

Single runs per value are enough for this result: no value comes out ahead across all ops, and every difference is within the run-to-run spread seen in the controls.

## Reproducing

```bash
for v in 128 64 32 16 8; do
  uv run --package proj1 proj1 all --broadcast-join --cache-dataframe --max-partition-mb $v --sink noop --explain-plan -v > proj1/reports/all/split-size/logs/sweep_$v.log 2>&1
  uv run python proj1/reports/tools/stages.py M3 proj1/reports/all/split-size/logs/sweep_stages_M3_$v.txt --broadcast-join --cache-dataframe --max-partition-mb $v
done
# row groups per file
uv run python -c "import pyarrow.parquet as pq,glob;[print(f,pq.ParquetFile(f).metadata.num_row_groups) for f in sorted(glob.glob('proj1/data/*.parquet'))]"
```
