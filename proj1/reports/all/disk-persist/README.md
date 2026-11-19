# Cleaned trips: parquet copy vs `DISK_ONLY` persist

- Date: 2026-11-03
- Data: 2026-01..08 (29.7 M records), local machine
- Common flags: `--broadcast-join --cache-dataframe --write-once --coalesce-cache`
- Compared: `--materialise-clean` (write cleaned trips as parquet, read back) vs `--persist-clean-disk` (same 9 columns, `persist(StorageLevel.DISK_ONLY)`, built with `count()` in the same `clean` profiler block)
- Runs: `all`, 3 each, interleaved, each started at load average < 1.5

Question: [materialise-clean](../materialise-clean/README.md) chose a parquet copy over a cache. The in-memory cache was already measured slower ([reuse-results](../reuse-results/README.md)); this checks a disk-only cache.

## Result

Median of 3 ([parquet_1](./logs/parquet_1.log), [parquet_2](./logs/parquet_2.log), [parquet_3](./logs/parquet_3.log), [disk_1](./logs/disk_1.log), [disk_2](./logs/disk_2.log), [disk_3](./logs/disk_3.log)):

| | Parquet copy | `DISK_ONLY` persist |
| --- | --- | --- |
| One-time build (`clean` block) | 17.0 s | 21.4 s |
| M2 | 9.0 s | 9.9 s |
| M3 | 7.2 s | 6.7 s |
| O2 | 10.7 s | 10.5 s |
| O4 | 8.0 s | 8.8 s |
| All blocks | 83.4 s [79.3-86.9] | 88.6 s [83.9-93.2] |
| Task time | 516 s | 573 s |

Outputs identical (M3 same rows; O4 floats within 7e-15).

- **Building is slower for the persist** (+4.4 s): Spark serialises full row blocks to its block manager, while parquet writes compressed columns (324 MiB on disk).
- **Reads are within noise** (differences ≤ 0.9 s either way): at this size both are dominated by decoding 21 M rows.
- **Parquet still wins overall**, and has two advantages this single-run comparison does not show: it survives the application (later runs skip the clean step, 72.5 s in [materialise-clean](../materialise-clean/README.md)), and each reader prunes to its own columns.

## Reproducing

```bash
B="--broadcast-join --cache-dataframe --write-once --coalesce-cache --sink noop --explain-plan -v"
uv run --package proj1 proj1 all $B --materialise-clean > proj1/reports/all/disk-persist/logs/parquet_<n>.log 2>&1
uv run --package proj1 proj1 all $B --persist-clean-disk > proj1/reports/all/disk-persist/logs/disk_<n>.log 2>&1
```
