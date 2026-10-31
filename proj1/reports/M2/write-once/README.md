# M2: compute the result once (`--write-once`)

- Date: 2026-10-31
- Data: 2026-01..08 (29.7 M records, 20.95 M after cleaning), local machine (see [baseline](../../baseline/README.md#experimental-setup))
- Before: `--broadcast-join --cache-dataframe` (current optimised config)
- After: the same plus `--write-once`, commit TODO
- Runs: `M2` alone, before and after interleaved (before 1, after 1, before 2, ...) so that drift on the laptop hits both sides equally

## Bottleneck observed

M2 runs two actions on the same DataFrame: `emit()` (the noop sink, or `show` when printing) and the parquet write. Spark computes the full pipeline for each one: scan, clean, broadcast join, aggregate. This was seen in the [baseline](../../baseline/README.md#m2-is-computed-twice) (stages 11/14 rerun as 20/23). After [broadcast-join](../../all/broadcast-join/README.md) it is still there as two 18-task scan stages ([stages_before](./logs/stages_before.txt)):

| Stage | Tasks | Task time | Role |
| ----- | ----- | --------- | ---- |
| 11 | 18 | 82.3 s | scan + clean + join + partial aggregate, for `emit` |
| 15 | 18 | 49.1 s | the same again, for the parquet write |

The second scan is about 36 % of M2's task time.

Caching the result did not fix this ([reuse-results](../../all/reuse-results/README.md)). The cache keeps the 200 shuffle partitions, so the write ran 200 tasks and wrote 663 files, and M2 ended up slower.

## Hypothesis

The result is tiny: 967 rows, 6 parquet files. If the write runs first, the second action can read the written files instead of recomputing from the trips. Reading back costs a few KB of I/O, which removes one full scan.

## Change

`--write-once` (`proj1/src/proj1/__init__.py`, `run_M2`) swaps the order of the two actions and emits from the written parquet:

```python
if features.write_once:
    dated_df.write.mode("overwrite").partitionBy("Borough").parquet(temporal_dir)
    emit(dated_df.sparkSession.read.parquet(temporal_dir), sink)
else:
    emit(dated_df, sink)
    dated_df.write.mode("overwrite").partitionBy("Borough").parquet(temporal_dir)
```

The written output is identical: same 967 rows, keys, counts and means, still 6 files. The shown rows are the same but can come in a different order, since M2 does not sort its result.

## Before / after

Median of 3 runs. Before: [before_1](./logs/before_1.log), [before_2](./logs/before_2.log), [before_3](./logs/before_3.log). After: [after_1](./logs/after_1.log), [after_2](./logs/after_2.log), [after_3](./logs/after_3.log). The spread is small on both sides (before 22.7-23.2 s, after 14.9-15.7 s).

| Metric | Before | After | Change |
| ------ | ------ | ----- | ------ |
| Wall time (median) | 23.0 s | 15.3 s | 1.5× faster |
| Task time (median) | 135.3 s | 85.5 s | −37 % |
| Jobs / stages / tasks | 6 / 8 / 76 | 5 / 6 / 45 | one 18-task scan stage less |
| Shuffle read / write | 1.5 MB | 0.8 MB | halved (one partial aggregate less) |
| Spill | 0 | 0 | – |

Per stage ([stages_before](./logs/stages_before.txt), [stages_after](./logs/stages_after.txt)): after the change only one scan stage remains (stage 11, 86.2 s of task time). It is followed by a 1-task aggregate and write (0.5 s) and a 6-task stage that reads the written files back for `emit` (0.4 s in total). The task time saved (≈ 50 s) matches the second scan stage that disappeared (49.1 s).

## Takeaway

Caching and writing first fix the same symptom, but only writing first worked here. For a result this small, the written output is itself the cheapest reusable form: Spark already has to produce it, and it lands as 6 files, not 200 cached partitions. Caching only paid off for O2, where 3 actions share a larger intermediate result.

## Reproducing

```bash
# before
uv run --package proj1 proj1 M2 --broadcast-join --cache-dataframe --sink noop --explain-plan -v > proj1/reports/M2/write-once/logs/before_<n>.log 2>&1
# after
uv run --package proj1 proj1 M2 --broadcast-join --cache-dataframe --write-once --sink noop --explain-plan -v > proj1/reports/M2/write-once/logs/after_<n>.log 2>&1
uv run python proj1/reports/tools/stages.py M2 proj1/reports/M2/write-once/logs/stages_<side>.txt --broadcast-join --cache-dataframe [--write-once]
uv run python proj1/reports/tools/medians.py proj1/reports/M2/write-once/logs/after_{1,2,3}.log
```
