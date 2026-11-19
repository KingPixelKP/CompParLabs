# all: fewer shuffle partitions (`--shuffle-partitions 8`)

- Date: 2026-11-06 22:29-22:48, one session, `local[*]` (8 threads, 4 cores)
- Data: 2026-01..08 (29.7 M records)
- Runs: `all --sink noop --explain-plan`, final configuration (`--broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache`) with and without `--shuffle-partitions 8`, 3 interleaved rounds, each started after a 20 s pause once the 1-minute load was below 1.0
- Result: **not adopted**, no gain beyond noise

## Bottleneck observed

After the other optimisations every operation is one scan plus small stages. The remaining fixed cost is per-stage and per-task scheduling, and `spark.sql.shuffle.partitions` is 200 on an 8-thread machine.

## Hypothesis

Starting every shuffle with 8 partitions (one per thread) instead of 200 removes tiny tasks and lowers that fixed cost.

## Change

`--shuffle-partitions N` sets `spark.sql.shuffle.partitions` (default 200).

## Before / after

Median of 3 (wall s, tasks), logs `final_<n>.log` and `sp8_<n>.log`:

| Op | final (200) | `--shuffle-partitions 8` |
| --- | --- | --- |
| clean write (no shuffle) | 18.6 s, 18 | 14.0 s, 18 |
| M2 | 8.9 s, 29 | 8.1 s, 29 |
| M3 | 7.1 s, 34 | 7.2 s, 34 |
| O2 | 10.5 s, 370 | 9.2 s, 341 |
| O4 | 8.4 s, 65 | 8.7 s, 65 |
| O1 (outside report scope) | 23.2 s, 114 | 24.4 s, 109 |

AQE already coalesces the 200 shuffle partitions into a few, so task counts change only in O2 (370 → 341) and O1. O2's 1.3 s gain is below the ~2 s noise band of this machine; the `clean` write, which has no shuffle at all, varied by 4.6 s between the two configurations in the same session.

## Reproducing

`zsh proj1/reports/all/shuffle-partitions/run.zsh`, summary: `uv run python proj1/reports/tools/medians.py proj1/reports/all/shuffle-partitions/logs/<final|sp8>_*.log`.
