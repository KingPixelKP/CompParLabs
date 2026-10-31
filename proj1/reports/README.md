# Reports

Performance engineering notes for Project 1. They are the source material for the final LaTeX report, so every directory maps to a report section.

| Directory / file                       | Content                                                                       | LaTeX section                |
| -------------------------------------- | ----------------------------------------------------------------------------- | ---------------------------- |
| [pipeline](./pipeline/README.md)       | Pipeline stages, design decisions, one annotated physical plan                | 1. Introduction, 2. Pipeline |
| [baseline](./baseline/README.md)       | Setup, baseline measurements, bottlenecks seen in plans and the Spark UI      | 3. Profiling and Baseline    |
| `<OP>/<short-name>/`                   | One optimisation each (e.g. [O2/shuffle-by-id](./O2/shuffle-by-id/README.md)) | 4. Optimisation Study        |
| [Summary table](#optimisation-summary) | One row per applied optimisation                                              | 4. (required table)          |
| [scalability](./scalability/README.md) | Dataset scale and cluster size runs                                           | 5. Analysis                  |
| [discussion](./discussion.md)          | Remaining bottlenecks, limitations, future work                               | 6. Discussion                |

The report covers M1, M2, M3, O2 and O4. O1 is implemented but is not part of the report.

## Measurement protocol

Every number in a report must come from a run that follows these rules, so before/after pairs are comparable.

- **Data**: the 2026-01..08 files (29.7 M records) until the 2016-2020 dataset is processed. State the data used in every report.
- **Sink**: always `--sink noop`. `show()` only computes the rows it prints, so its timings understate the work.
- **Plans**: always `--explain-plan`, so the log holds the physical (`explain("formatted")`) and logical (`explain(True)`) plans.
- **Repetitions**: 3 runs per configuration, report the median wall time. Wall time on a laptop is noisy (M3 once took 317 s inside `all` and 24 s alone).
- **Command**:

  ```bash
  uv run --package proj1 proj1 <CMD> [--<feature>] --sink noop --explain-plan -v > proj1/reports/<OP>/<name>/logs/<config>_<n>.log 2>&1
  ```

- **Screenshots**: rerun once with `--keep-ui` and capture from <http://localhost:4040> the Jobs timeline, the SQL DAG and the Stages task summary (min/median/max task time, used for skew).
- **One change at a time**: the before and after runs differ only in the change being measured. Record the commit hash of both.
- **Input size caveat**: Spark's input metric (profiler and Spark UI) undercounts parquet scans here (6.2 MiB reported vs 383.2 MiB of column chunks for M3). Do not cite it; use the parquet footer sizes (see [baseline](./baseline/README.md#baseline-measurements)).

Logs under the repository root `logs/` were produced before commit `7c81125` (old timestamp handling and cleaning rules). They are historical and must not be used as baselines.

## Optimisation report structure

Each optimisation lives in `proj1/reports/<OP>/<short-name>/README.md` with `logs/` and `images/` next to it. Sections follow the LaTeX optimisation subsection:

1. **Date and data** used.
2. **Bottleneck observed**: the metric and where it is (stage id, plan node), with a screenshot or plan excerpt from the baseline.
3. **Hypothesis**: why that node or stage costs what it does.
4. **Change**: the feature flag and/or a short code snippet, with the commit hash.
5. **Before / after**: table with median wall time, stages, tasks, shuffle read/write and spill, plus the plan difference (e.g. `SortMergeJoin` to `BroadcastHashJoin`, number of `Exchange` nodes). Link the 3 log files of each side and the screenshots.
6. **Reproducing**: exact commands for before and after.

When done, add a row to the [summary table](#optimisation-summary) and update the backlog row status.

An example of the layout can be found under [Example Report](./example/found-example/README.md).

## Optimisation summary

Median of 3 runs, `--sink noop`, 2026-01..08 data, local machine (see [baseline](./baseline/README.md)).

| Op  | Optimisation | Bottleneck | Wall time before → after | Shuffle before → after | Spill before → after | Report |
| --- | ------------ | ---------- | ------------------------ | ---------------------- | -------------------- | ------ |
| M2, M3, O2, O4 | Broadcast the zone / bucket table (`--broadcast-join`) | Trip-side shuffle + sort of the `SortMergeJoin`, spill in the 9-task reduce stage | M2 45.7 → 20.9 s, M3 21.1 → 11.8 s, O4 22.1 → 15.6 s, O2 ≈ unchanged | M2 953.8 → 1.5 MB, M3 318.5 → 0.7 MB, O4 222.4 → 1.8 MB | M2 1.9 GB, M3 768 MB, O4 704 MB → 0 | [all/broadcast-join](./all/broadcast-join/README.md) |
| M2, O2, O4 | Persist results reused by several actions (`--cache-dataframe`) | Trips scanned once per action (M2 2×, O2 3×, O4 2×) | O2 21.7 → 17.0 s; M2 20.9 → 23.2 s and O4 15.6 → 19.9 s (slower) | ≈ unchanged (≤ 55 MB) | 0 → 0 | [all/reuse-results](./all/reuse-results/README.md) |
| all | Smaller scan splits (`--max-partition-mb` 64/32/16/8) | Uneven 18-task scan stage, near-empty tasks | no gain (within noise), not adopted | – | – | [all/split-size](./all/split-size/README.md) |

## Optimisation backlog

Possible optimisations, **not applied**. Each one must first be confirmed by profiling the baseline, then applied and measured in its own report. Status: `to check`, `confirmed` (baseline evidence found), `not confirmed`, `done`, `tried, slower` (applied and measured, made things worse).

| Op             | Suspected bottleneck                                      | Evidence to look for                             | Candidate change                                                                                          | Status                                                                                                  |
| -------------- | --------------------------------------------------------- | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| M2, M3, O1, O2 | SortMergeJoin against the 265 row zone table              | `SortMergeJoin` with an `Exchange` on both sides | Broadcast the zone table (`--broadcast-join`)                                                             | done ([all/broadcast-join](./all/broadcast-join/README.md)) |
| M3, O1         | Zone strings (`Borough`, `Zone`) carried through shuffles | Shuffle bytes per stage                          | Join zones after aggregation/window, shuffle ids only                                                     | to check                                                                                                |
| O2             | String keys in groupBy/window shuffles                    | Shuffle write bytes                              | Group by ids, resolve names last (`--string-to-id`, see [O2/shuffle-by-id](./O2/shuffle-by-id/README.md)) | weak (O2 shuffles ≤ 6.5 MB per stage)                                                                   |
| O1             | Window over the full trip table to get per bucket mean/σ  | `Window` after an `Exchange` of every trip row   | Aggregate per bucket, broadcast join stats back                                                           | out of report scope                                                                                     |
| O4             | Join back of the classification to the full trip table    | `SortMergeJoin` shuffling every trip row         | Broadcast the 263×24 bucket table                                                                         | done ([all/broadcast-join](./all/broadcast-join/README.md)) |
| O4             | Classification labels as strings                          | Shuffle bytes of the final aggregation           | Integer class codes, map to labels at the end                                                             | to check                                                                                                |
| O1, O4         | Skew on hot pickup zones (132 JFK, 161, 237)              | Task duration max ≫ median in bucket stages      | AQE skew handling, salting, `--repartition-pickup`                                                        | not confirmed (max/median ≤ 1.1), recheck at full scale                                                 |
| M2             | Result computed twice (`show` then parquet write)         | Two jobs with the same stages                    | Write once, read back / cache                                                                             | cache: tried, slower ([reuse-results](./all/reuse-results/README.md): cache keeps 200 partitions, write makes 663 files); write once + read back still to try |
| O2             | Route aggregation recomputed for each of the 3 outputs    | Scan stage repeated per action                   | Persist `routes` (`--cache-dataframe`)                                                                    | done ([reuse-results](./all/reuse-results/README.md)) |
| O4             | Trip table scanned and cleaned twice                      | Two `Union` branches in the plan                 | Persist the cleaned trips                                                                                 | tried, slower ([reuse-results](./all/reuse-results/README.md): building the 390 MiB cache costs more than the rescan) |
| all            | Cleaned trips recomputed by every command                 | Repeated `FileScan` + `Filter` stages            | Persist the cleaned DataFrame (`--cache-dataframe`)                                                       | to check in `all` (unlikely to pay off: caching the O4 trips was slower) |
| all            | AQE coalesces 200 shuffle partitions to 9                 | 9 tasks on 8 cores (2 waves), spill per task     | `spark.sql.adaptive.advisoryPartitionSizeInBytes`, shuffle partitions, memory                             | resolved by [broadcast-join](./all/broadcast-join/README.md) (9-task reduce stages gone), recheck at full scale |
| load           | One scan per schema group joined by `Union`               | Several `FileScan` nodes                         | Rewrite files once to a common schema                                                                     | to check                                                                                                |
| all            | Uneven scan tasks, some read nothing                      | Scan task min ≪ median                           | Smaller splits (`--max-partition-mb`)                                                                     | tried, no gain ([split-size](./all/split-size/README.md): parallelism capped by 32 row groups) |
| load           | Only 32 parquet row groups cap scan parallelism           | Scan tasks ≤ row groups; slowest task = 1 row group | Rewrite input with smaller row groups (`parquet.block.size`), together with the common-schema rewrite   | to check (matters more on the cluster) |
