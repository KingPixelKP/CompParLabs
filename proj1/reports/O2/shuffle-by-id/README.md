# O2: shuffle zone ids instead of zone names

> [!NOTE]
> **Not implemented, not part of the report.** Kept as a documented idea: the `--string-to-id` flag exists but has no effect, and the TODO cells below were never measured. The only numbers are the historical preliminary run at the end.

- Date: first measured 2026-10-10 (preliminary only)
- Data: 2026-01..08 (29.7 M records)
- Status: hypothesis from a preliminary measurement, not remeasured with the [measurement protocol](../../README.md#measurement-protocol)

## Bottleneck observed

O2 resolves zone names and boroughs **before** the top-20 and Q1/Q3 aggregations, so the `groupBy`, the `row_number` window and the `pivot` shuffle rows keyed by up to 4 strings (`pickup_zone`, `dropoff_zone` and both boroughs) instead of 2 integers.

Baseline evidence ([baseline](../../baseline/README.md#o2-recomputes-the-route-aggregation-for-each-output), [stages_O2](../../baseline/logs/stages_O2.txt)): every shuffle after the monthly route aggregation is ≤ 6.5 MB, and O2 never spills. The gain from shuffling ids is expected to be small at this data size. O2's larger cost is the route aggregation running once per output (3 times), see the backlog.

## Hypothesis

String keys make shuffled rows wider (zone names average ~20 bytes vs 4 bytes for an `int`) and make hashing and sorting slower. Grouping by `PULocationID`/`DOLocationID` and joining the names only on the final, small results should lower shuffle bytes and time. The gain is probably small for O2: after the monthly aggregation there are at most 263 × 263 × months rows.

## Change

TODO: wire `--string-to-id` in `run_O2` (`proj1/src/proj1/__init__.py`). With the flag, top-20 and the Q1/Q3 pivot group by ids and resolve names with the zone joins at the end. Without it, the current code (by name) runs unchanged.

## Before / after

| Metric | Before (by name) | After (by id) | Change |
| ------ | ---------------- | ------------- | ------ |
| Median wall time (s) | TODO | TODO | TODO |
| Stages / tasks | TODO | TODO | TODO |
| Shuffle read / write | TODO | TODO | TODO |
| Spill (memory / disk) | TODO | TODO | TODO |
| `Exchange` nodes | TODO | TODO | TODO |

Logs: before `logs/by_name_{1,2,3}.log`, after `logs/by_id_{1,2,3}.log`. Screenshots: `images/`.

### Preliminary measurement (historical)

Measured on pre-`7c81125` code with the `show` sink, one run each, so not comparable with the table above:

- By name: [logs/baseline.log](./logs/baseline.log), commit [`5dcc723`](https://github.com/KingPixelKP/CompParLabs/commit/5dcc723d22f695dddfd54d69b839ff03ab5ecc66)
- By id: [logs/baseline_by_id_1.log](./logs/baseline_by_id_1.log), commit [`ebcbea1`](https://github.com/KingPixelKP/CompParLabs/commit/ebcbea19253ca93423a0dcd840845c22a0c9d257)
- Shuffle read/write about 4 MB lower by id, wall time about 3 s lower. The only plan difference was one fewer `Project` node.

## Reproducing

```bash
# before
uv run --package proj1 proj1 O2 --sink noop --explain-plan -v > proj1/reports/O2/shuffle-by-id/logs/by_name_<n>.log 2>&1
# after
uv run --package proj1 proj1 O2 --string-to-id --sink noop --explain-plan -v > proj1/reports/O2/shuffle-by-id/logs/by_id_<n>.log 2>&1
```

Commit: TODO
