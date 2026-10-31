# All ops: clean once, read many (`--materialise-clean`)

- Date: 2026-10-31
- Data: 2026-01..08 (29.7 M records, 20.95 M after cleaning), local machine (see [baseline](../../baseline/README.md#experimental-setup))
- Before: `--broadcast-join --cache-dataframe --write-once` (optimised config so far)
- After: the same plus `--materialise-clean`, commit TODO
- Runs: `all`, before and after interleaved, 3 each

## Bottleneck observed

After the earlier optimisations, each op is one parquet scan plus small stages, and the scan is the cost (M3 scan stage: 79.3 s of 80 s task time, [stages_M3_before](./logs/stages_M3_before.txt)). Every op starts with the same subplan:

```text
Union
:- Project (try_cast to TRIP_SCHEMA) <- Filter (13 cleaning predicates) <- Scan parquet (files of schema group 1)
+- Project (try_cast to TRIP_SCHEMA) <- Filter (13 cleaning predicates) <- Scan parquet (files of schema group 2)
```

One `all` run executes this 9 times (M1, M2, M3, O1, O2 ×3 plans, O4 ×2). Each time it reads the raw files, casts them and cleans the same 21 M rows. It must also read `passenger_count` and `DOLocationID` in ops that never use them, only to evaluate the cleaning rules.

## Hypothesis

Writing the cleaned, typed trips once to parquet and running M2-O4 on that copy should:

- remove the `Union`, the cleaning `Filter` and the casts from every op's plan
- let each op read only its own columns
- add scan parallelism: the raw files have only 32 row groups, see [split-size](../split-size/README.md)

The one-time cost is a scan, a clean and a write. In memory caching was rejected, because the [O4 cache](../reuse-results/README.md) cost more than a rescan and would not fit at 2016-2020 scale.

## Change

`--materialise-clean` (`proj1/src/proj1/__init__.py`). Before the ops run, `main` writes `get_clean(trip_df)` to `out/clean`. It keeps 9 columns (`tip_amount` and `total_amount` are read by no op) and uses 8 MiB row groups. This step runs in its own profiler block (`clean`), so its cost shows up in the logs. M2-O4 then read `spark.read.parquet("out/clean")` with `use_cleansed` off, so they do not filter again. M1 still reads the raw files, because its job is to count what cleaning removes.

```python
def materialise_clean(trip_df: DataFrame, path: str):
    get_clean(trip_df).select(*CLEAN_COLUMNS).write.mode("overwrite").option(
        "parquet.block.size", CLEAN_ROW_GROUP_BYTES
    ).parquet(path)
```

The cleaned copy:

- 20 952 103 rows (equal to M1's kept count)
- 15 files, 43 row groups, 324 MiB
- the scanned columns of the raw files: 383 MiB in 32 row groups

At 32 MiB, `parquet.block.size` gave 1 row group per file, because the setting counts uncompressed bytes, so 8 MiB was used.

The outputs are unchanged:

- M3 and O4 (`--sink show`): same rows, floats within 7e-15.
- M2 parquet: same 967 rows.
- O2: same rows. Only the display order of tied routes (`trip_volume = 1`) differs, which was never deterministic.

## Before / after

Plan nodes, all formatted plans of one `all` run ([before_1](./logs/before_1.log), [after_1](./logs/after_1.log)):

| Node | Before | After |
| ---- | ------ | ----- |
| `Union` | 9 | 1 (M1, raw files on purpose) |
| `Scan parquet` | 18 | 10 |
| `Filter` | 27 | 22 (the rest are join `isnotnull` filters) |

In the M2 plan after the change, the trip scan reads 5 columns from `out/clean` with only `IsNotNull(PULocationID)` pushed down. Before, it read 7 columns with 13 pushed predicates.

Median of 3 runs. Before: [before_1](./logs/before_1.log), [before_2](./logs/before_2.log), [before_3](./logs/before_3.log). After: [after_1](./logs/after_1.log), [after_2](./logs/after_2.log), [after_3](./logs/after_3.log).

| Op | Wall time (s) before → after | Task time (s) before → after | Tasks |
| -- | ---------------------------- | ---------------------------- | ----- |
| clean (one-time write) | – → 19.4 | – → 130.6 | – → 18 |
| M2 | 13.8 → 9.0 (1.5×) | 85.0 → 50.3 (−41 %) | 45 → 29 |
| M3 | 13.4 → 6.8 (2.0×) | 85.2 → 33.8 (−60 %) | 58 → 34 |
| O2 | 19.0 → 16.3 (1.2×) | 104.9 → 85.1 (−19 %) | 2 547 → 2 443 |
| O4 | 13.9 → 7.9 (1.8×) | 92.1 → 47.6 (−48 %) | 113 → 65 |
| O1 (outside the report scope) | 24.9 → 22.4 | 173.7 → 154.7 | 162 → 114 |
| M1 (control, raw files both times) | 11.5 → 9.5 | 68.9 → 59.2 | 76 → 76 |
| **`all` total, including the write** | **96.2 → 92.3** | 610 → 561 | |
| `all` without the one-time write | 96.2 → 72.5 | 610 → 431 | |

Runs were stable: every op's 3 wall times are within 1.5 s of each other, and the "before" total (96.2 s) matches the sum of the earlier reports' medians (~95 s). A first set of runs, taken while the PC was under heavy unrelated load, was discarded.

Per stage ([stages_M3_before](./logs/stages_M3_before.txt), [stages_M3_after](./logs/stages_M3_after.txt)): M3's scan went from 18 tasks with 68.2 s of task time to 10 tasks with 40.0 s, and the slowest task from 6.9 s to 5.0 s.

M1 runs the same plan on both sides but is 2 s faster after the change. In the after runs it follows the `clean` write, which has just read the raw files, so the OS file cache is warm. Differences of about 2 s are therefore within noise. The savings for M2, M3 and O4 (4.8-6.6 s of wall time, 35-51 s of task time each) are well above that.

## Result

- **Every reported op gets cheaper.** Task time drops 19-60 % and wall time 14-49 %, because the scan no longer casts, cleans or reads the cleaning-only columns.
- **It pays off within a single `all` run.** The one-time write costs 19.4 s of wall time. The ops then run 23.7 s faster, so even counting the write, `all` drops from 96.2 to 92.3 s. Every later run gets the full −25 % (72.5 s), since the cleaned copy already exists. In a profile → change → measure loop the pipeline runs many times.
- **The benefit grows with scale.** At 2016-2020 (~500 M rows) the raw files have many schema groups (more `Union` branches) and much more data per cleaning pass, while the cleaned copy is written only once. On the cluster, its smaller row groups also give more scan parallelism.
- **O1 is unchanged**, because its cost is the window over all trips, not the scan (outside the report scope).

## Reproducing

```bash
# before
uv run --package proj1 proj1 all --broadcast-join --cache-dataframe --write-once --sink noop --explain-plan -v > proj1/reports/all/materialise-clean/logs/before_<n>.log 2>&1
# after
uv run --package proj1 proj1 all --broadcast-join --cache-dataframe --write-once --materialise-clean --sink noop --explain-plan -v > proj1/reports/all/materialise-clean/logs/after_<n>.log 2>&1
uv run python proj1/reports/tools/stages.py M3 proj1/reports/all/materialise-clean/logs/stages_M3_<side>.txt --broadcast-join --cache-dataframe --write-once [--materialise-clean]
uv run python proj1/reports/tools/medians.py proj1/reports/all/materialise-clean/logs/after_{1,2,3}.log
```
