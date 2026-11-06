# All ops: broadcast the small side of the joins

- Date: 2026-10-29
- Data: 2026-01..08 (29.7 M records, 20.95 M after cleaning), local machine (see [baseline](../../baseline/README.md#experimental-setup))
- Before: baseline, commit `7c81125` (flag off; the code path without the flag is unchanged since)
- After: `--broadcast-join`, commit `d679ef3` (flag on; the code path with the flag is unchanged since)

## Bottleneck observed

In the baseline every operation joins ~21 M cleaned trips with a tiny table as a `SortMergeJoin`: the 265-row zone table (M2, M3, O2) or the ≤ 6 312-row bucket table in the O4 join back. Both sides get an `Exchange`, so the whole trip side is hashed, written to shuffle files, read back and sorted ([baseline](../../baseline/README.md#where-the-time-goes-the-trip-side-shuffle-of-the-zone-join), [M2 DAG](../../baseline/images/query0M2.svg)).

The stage after that shuffle is the most expensive one of each op ([stages_M2](../../baseline/logs/stages_M2.txt), [stages_M3](../../baseline/logs/stages_M3.txt), [stages_O4](../../baseline/logs/stages_O4.txt)):

| Op | Stage | Shuffle read | Spill memory / disk | Task time total | Tasks |
| -- | ----- | ------------ | ------------------- | --------------- | ----- |
| M2 | 14 | 476.4 MB | 896 MB / 392 MB | 99.1 s | 9 |
| M3 | 14 | 318.4 MB | 768 MB / 261 MB | 101.1 s | 9 |
| O4 | 14 | 222.3 MB | 704 MB / 107 MB | 73.7 s | 9 |

## Hypothesis

The small side fits easily in every executor's memory (265 rows, ≤ 6 312 rows). If it is broadcast, Spark can run a `BroadcastHashJoin` inside the scan stage: each scan task probes a hash table of the small side, so the trip rows are never shuffled or sorted. The aggregation that follows then runs as a partial aggregate in the scan tasks, and only the few thousand partial groups are shuffled. This should remove the trip-side `Exchange`, the sort and its spill, and the 9-task reduce stage that ran in 2 waves on 8 cores.

`spark.sql.autoBroadcastJoinThreshold=-1` stays set, so the explicit hint is the only difference between the two runs.

## Change

`--broadcast-join` wraps the small side in `sf.broadcast` (`proj1/src/proj1/__init__.py`). M2, M3 and O2 already did this for `zone_df`. This report adds the O4 bucket table:

```python
buckets = sf.broadcast(buckets) if features.broadcast_join else buckets

zone_type_stats = (
    trip_df.join(buckets, ["PULocationID", "hour_of_day"])
```

The results are unchanged:

- M3 and O4 (`--sink show`) give the same rows. Floats differ by at most 2e-13 relative, from a different summation order.
- The M2 parquet output has the same 967 keys and trip counts. Its means agree to 8e-15.

## Before / after

Median of 3 runs. Before: [all_1](../../baseline/logs/all_1.log), [all_2](../../baseline/logs/all_2.log), [all_3](../../baseline/logs/all_3.log). After: [broadcast_1](./logs/broadcast_1.log), [broadcast_2](./logs/broadcast_2.log), [broadcast_3](./logs/broadcast_3.log). Jobs, stages, tasks and shuffle bytes were identical in the 3 after runs.

| Op | Wall time (s) before → after | Speedup | Stages | Tasks | Shuffle read | Spill memory / disk |
| -- | ---------------------------- | ------- | ------ | ----- | ------------ | ------------------- |
| M2 | 45.7 → 20.9 | 2.2× | 18 → 8 | 152 → 76 | 953.8 MB → 1.5 MB | 1.9 GB / 832.6 MB → 0 |
| M3 | 21.1 → 11.8 | 1.8× | 9 → 7 | 76 → 58 | 318.5 MB → 0.7 MB | 768.0 MB / 261.3 MB → 0 |
| O2 | 20.3 → 21.7 | ≈ 1× (noise) | 81 → 45 | 515 → 393 | 53.9 MB → 47.9 MB | 0 → 0 |
| O4 | 22.1 → 15.6 | 1.4× | 18 → 11 | 220 → 113 | 222.4 MB → 1.8 MB | 704.0 MB / 106.8 MB → 0 |
| M1 (control, no join) | 14.5 → 12.0 | – | 8 → 8 | 76 → 76 | 5.4 KB → 5.4 KB | 0 → 0 |

M1 has no join and runs the same plan in both cases, so its 2.5 s difference is run-to-run noise. Differences of that size (O2) are not significant.

Plan nodes per query (formatted plans in the logs):

| Op | Before | After |
| -- | ------ | ----- |
| M2 | 1 `SortMergeJoin`, 3 `Exchange` | 1 `BroadcastHashJoin`, 1 `Exchange` + 1 `BroadcastExchange` |
| M3 | 1 `SortMergeJoin`, 3 `Exchange` | 1 `BroadcastHashJoin`, 2 `Exchange` + 1 `BroadcastExchange` |
| O2 (per output) | 2 `SortMergeJoin`, 6 `Exchange` | 2 `BroadcastHashJoin`, 2-3 `Exchange` + 2 `BroadcastExchange` |
| O4 | 1 `SortMergeJoin`, 4 `Exchange` | 1 `BroadcastHashJoin`, 3 `Exchange` + 1 `BroadcastExchange` |

After the change, the M2 plan is a scan, a broadcast join and a partial aggregate in one stage, followed by a single small `Exchange`:

```text
HashAggregate
+- Exchange
   +- HashAggregate
      +- Project
         +- BroadcastHashJoin Inner BuildRight
            :- Union (scan + filter of the trip files)
            +- BroadcastExchange
               +- Filter
                  +- Scan csv
```

Per stage ([stages_M2](./logs/stages_M2.txt), [stages_M3](./logs/stages_M3.txt), [stages_O2](./logs/stages_O2.txt), [stages_O4](./logs/stages_O4.txt)): the 9-task reduce stages are gone. What remains after each scan stage is a 1-task stage that reads 0.6-1.7 MB of partial aggregates in under 1 s. All the work is now in the 18-task scan stages: M2 stage 11 has 80.4 s of task time and writes 0.8 MB of shuffle.

O2 barely changes in wall time. Its joins run on monthly route aggregates (≤ 263 × 263 rows per month), not on trips, so the shuffles they remove were small (≤ 6.5 MB each). It still loses 36 stages.

Screenshots: not captured; the plan change is in the logs (`explain("formatted")`) and in Figure 1 of the report.

## What the profile shows next

With the shuffle gone, the scans dominate, and the ops that run several actions still scan the trips several times:

- M2: stages 11 and 15, 80.4 s + 60.2 s task time.
- O2: stages 11, 25 and 45.
- O4: stages 10 and 13.

That is the next optimisation (reuse of results, see [reuse-results](../reuse-results/README.md)). O1, which is outside the report scope, still spills 1.4 GB in its window over all trips.

## Reproducing

```bash
# before (baseline)
uv run --package proj1 proj1 all --sink noop --explain-plan -v > proj1/reports/baseline/logs/all_<n>.log 2>&1
# after
uv run --package proj1 proj1 all --broadcast-join --sink noop --explain-plan -v > proj1/reports/all/broadcast-join/logs/broadcast_<n>.log 2>&1
uv run python proj1/reports/tools/stages.py <CMD> proj1/reports/all/broadcast-join/logs/stages_<CMD>.txt --broadcast-join
uv run python proj1/reports/tools/medians.py proj1/reports/all/broadcast-join/logs/broadcast_{1,2,3}.log
```
