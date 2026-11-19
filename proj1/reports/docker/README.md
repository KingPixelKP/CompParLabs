# Baseline vs optimised on the Docker cluster

> [!NOTE]
> Superseded by [docker-session](../docker-session/README.md) (2026-11-05), which re-measures this table in one session together with dataset scale and cluster size. The report uses those numbers; this series is kept for reference.

- Date: 2026-11-02
- Data: 2026-01..08 (29.7 M records), Docker standalone cluster on the laptop: 1 master, 3 workers of 2 cores / 2 GiB (`spark.executor.memory=1536m`), driver in the `jupyter` container (see [scalability](../scalability/README.md#docker-standalone-cluster-on-the-laptop-2026-11-01-supplement) for the topology)
- Configs: baseline (no flags) and final (`--broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache`)
- Runs: `all`, 3 per config, interleaved, each started once the 1-minute load average was below 1.5

Why: the assignment asks for a baseline on a cluster (wall-clock time, per-stage duration). The DI cluster was not available, so this is the closest substitute. Unlike `local[*]`, shuffles here go between executor JVMs in different containers, and memory is split over 3 executors.

## Wall time

Median of 3 runs. Logs: [baseline_1](./logs/baseline_1.log), [baseline_2](./logs/baseline_2.log), [baseline_3](./logs/baseline_3.log), [optimised_1](./logs/optimised_1.log), [optimised_2](./logs/optimised_2.log), [optimised_3](./logs/optimised_3.log).

| Op | Baseline (s) | Optimised (s) | Speedup | Local, same steps ([cumulative](../cumulative/logs/)) |
| -- | ------------ | ------------- | ------- | ----------------------------------------------------- |
| M2 | 42.9 | 12.5 | 3.4× | 38.3 → 7.7 (5.0×) |
| M3 | 19.2 | 10.2 | 1.9× | 18.2 → 7.4 (2.5×) |
| O2 | 26.0 | 17.2 | 1.5× | 18.4 → 9.8 (1.9×) |
| O4 | 32.3 | 9.9 | 3.3× | 21.0 → 8.6 (2.4×) |
| clean (one-time write) | – | 22.9 | | 14.3 |
| all ops (sum per run) | 168.2 [163.9-200.2] | 113.5 [112.9-120.0] | 1.5× | 136.8 → 76.9 (1.8×) |

The shuffle and spill numbers match the local runs: M2 reads 953.6 MB of shuffle in the baseline and 0.5 MB optimised, and O4 goes from 221.8 MB with 1.0 GB of spill to 1.1 MB.

## Per-stage duration

The stages with the longest wall time in one `all` run per config ([stages_baseline](./logs/stages_baseline.txt), [stages_optimised](./logs/stages_optimised.txt); `wall_s` = submission to completion, `run_s` = summed task time):

| Config | Stage (role) | Tasks | Wall (s) | Task time (s) | Shuffle | Spill mem / disk |
| ------ | ------------ | ----- | -------- | ------------- | ------- | ---------------- |
| baseline | 49, O1 window (outside the report scope) | 7 | 21.5 | 121.4 | read 363.1 MB | 2 704 MB / 413 MB |
| baseline | 22, M2 SMJ reduce | 9 | 12.8 | 66.6 | read 476.3 MB | 0 |
| baseline | 151, O4 join-side scan | 12 | 10.4 | 52.0 | write 220.4 MB | 0 |
| baseline | 155, O4 SMJ reduce | 7 | 10.0 | 57.6 | read 221.7 MB | 1 056 MB / 161 MB |
| optimised | 10, scan + clean + write of the cleaned copy | 12 | 21.2 | 114.3 | – | 0 |
| optimised | 36, O1 window | 7 | 10.2 | 58.9 | read 227.8 MB | 1 344 MB / 167 MB |
| optimised | 12, first scan of the cleaned copy | 12 | 9.2 | 48.8 | – | 0 |

## Observations

- **The same bottlenecks as locally.** In the baseline, the costly stages are the sort-merge-join reduce stages and the trip-side shuffle that feeds them. After the optimisations, the one-time write of the cleaned copy is the longest stage, and no trip-side shuffle remains in the reported operations.
- **Spill depends on the executor layout.** The M2 reduce stage does not spill on Docker, while locally it spilled 896 MB / 392 MB. Its 9 tasks are spread over 3 executor JVMs of 1.5 GiB each, instead of 8 tasks sharing one 2 GiB JVM. O4's reduce stage still spills (1.0 GB), with 7 partitions here.
- **Smaller gains than locally (1.5× vs 1.8× for all ops).** The optimised run has a larger fixed share on this cluster. The cleaned-copy write takes 22.9 s (14.3 s locally), and O2's coalesced cache is read across containers in 17.2 s (9.8 s locally). Both are scan- and task-bound on 6 executor cores that share 4 physical cores with the driver and master (see the cluster-size analysis). M2 and O4, where the broadcast removes a shuffle between executors, keep the largest speedups (3.4× and 3.3×).

## Reproducing

```bash
cd docker-spark-env && docker compose up -d --scale spark-worker=3 && cd ..
C="--master docker --data-dir /repo/proj1/data --output-dir /repo/out/docker"
docker compose -f docker-spark-env/docker-compose.yml exec -T -w /repo \
  -e PYTHONPATH=/repo/proj1/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
  jupyter python3 -m proj1 all $C [<final flags>] --sink noop --explain-plan -v > proj1/reports/docker/logs/<config>_<n>.log 2>&1
uv run python proj1/reports/tools/stages.py all proj1/reports/docker/logs/stages_<config>.txt $C [<final flags>]
```
