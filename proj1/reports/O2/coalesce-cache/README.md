# O2: coalesce the cached routes (`--coalesce-cache`)

- Date: 2026-11-01
- Data: 2026-01..08 (29.7 M records, 20.95 M after cleaning), local machine (see [baseline](../../baseline/README.md#experimental-setup))
- Before: `--broadcast-join --cache-dataframe --write-once --materialise-clean` (final config so far)
- After: the same plus `--coalesce-cache`, commit TODO
- Runs: `O2` alone, before and after interleaved, each started once the 1-minute load average was below 1.5

## Bottleneck observed

O2 caches its monthly routes so its 3 outputs scan the trips only once ([reuse-results](../../all/reuse-results/README.md)). But the outputs read that 4.8 MiB cache in **200-task stages** ([stages_before](./logs/stages_before.txt)):

| Stage | Tasks | Task time | Task time min / median / max |
| ----- | ----- | --------- | ---------------------------- |
| 18 | 200 | 13.0 s | 21 / 52 / 371 ms |
| 21 | 200 | 4.8 s | 4 / 12 / 227 ms |
| 24 | 200 | 7.1 s | 10 / 31 / 132 ms |
| 31 | 200 | 21.1 s | 36 / 90 / 267 ms |
| 54 | 200 | 28.2 s | 50 / 132 / 391 ms |

These 5 stages hold 74 s of O2's 128 s of task time, spread over 1 000 tasks of tens of milliseconds each. The scale runs show the same thing as a fixed cost: O2 takes 10.6 s on 1 month and 15.8 s on 8 months ([scalability](../../scalability/README.md#local-2026-11-01)).

## Hypothesis

`routes` comes out of a window whose `Exchange` has `spark.sql.shuffle.partitions = 200`. The cache stores those 200 partitions as they are. AQE normally coalesces small shuffle partitions when it reads a shuffle, but it does not repartition a cached relation. So every action on the cache gets 200 tasks, and with ~25 KB per partition, task scheduling and setup cost far more than the work. This is the same mechanism that made M2's cache slower in [reuse-results](../../all/reuse-results/README.md). Coalescing to the default parallelism (8) before caching should leave 8 tasks per stage that each do real work.

## Change

`--coalesce-cache` (`proj1/src/proj1/__init__.py`, `run_O2`). `coalesce` merges partitions without a shuffle:

```python
if features.coalesce_cache:
    routes = routes.coalesce(
        routes.sparkSession.sparkContext.defaultParallelism
    )
routes = routes.cache()
```

Same 123 output rows with and without the flag (`--sink show`, compared as sorted rows).

## Before / after

Median of 3 runs. Before: [before_1](./logs/before_1.log), [before_2](./logs/before_2.log), [before_3](./logs/before_3.log). After: [after_1](./logs/after_1.log), [after_2](./logs/after_2.log), [after_3](./logs/after_3.log).

| Metric | Before | After | Change |
| ------ | ------ | ----- | ------ |
| Wall time (median [min-max]) | 24.2 s [22.6-25.5] | 14.8 s [12.5-16.3] | 1.64× faster |
| Task time (median) | 127.6 s | 81.3 s | −36 % |
| Tasks | 2 443 | 370 | −85 % |
| Jobs / stages | 14 / 47 | 15 / 52 | +1 job (the coalesced cache is materialised in its own stages) |

Per stage ([stages_after](./logs/stages_after.txt)): the five 200-task stages become 8-task stages with 39 s of task time instead of 74 s. Their tasks now take 0.3-1.7 s each, so each one does real work. The scan stage (95 s of task time) is unchanged.

O2 run on its own is slower than inside `all` (24.2 s here vs 16.3 s in [materialise-clean](../../all/materialise-clean/README.md)): run alone, it also pays for JVM warm-up and JIT compilation, which in `all` the earlier ops have already done. Before and after were both run alone, so the comparison is fair.

## Takeaway

Caching changes the physical partitioning that later stages inherit: the cache freezes whatever partition count the plan had when it was materialised, and AQE cannot fix it on read. For a small cached result, choose the partition count explicitly. This removes most of O2's fixed cost, which was the main remaining bottleneck after the other optimisations.

## Reproducing

```bash
# before
uv run --package proj1 proj1 O2 --broadcast-join --cache-dataframe --write-once --materialise-clean --sink noop --explain-plan -v > proj1/reports/O2/coalesce-cache/logs/before_<n>.log 2>&1
# after
uv run --package proj1 proj1 O2 --broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache --sink noop --explain-plan -v > proj1/reports/O2/coalesce-cache/logs/after_<n>.log 2>&1
uv run python proj1/reports/tools/stages.py O2 proj1/reports/O2/coalesce-cache/logs/stages_<side>.txt --broadcast-join --cache-dataframe --write-once --materialise-clean [--coalesce-cache]
```
