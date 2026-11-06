# Comp Par Project 1

> [!WARNING]
> **Run every command in this README from the repository root**, not from `proj1/`: `proj1` is a member of the uv workspace defined by the root `pyproject.toml` and `uv.lock`, and all paths below are relative to the root.
>
> ```bash
> git clone https://github.com/KingPixelKP/CompParLabs.git
> cd CompParLabs
> ```

## Report

- **PDF: [latex/main.pdf](latex/main.pdf)**, source [latex/main.tex](latex/main.tex). Rebuild with `cd proj1/latex && latexmk -pdf main.tex`.
- Logs, plans, charts and screenshots behind every number: [reports/](reports/README.md). The report's footnotes and captions name the folder for each result.

## Setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

Data is downloaded with [fetch](./src/proj1/fetch.py). **Every measurement in the report uses 2026-01..08**; fetch exactly those files with:

```bash
uv run --package proj1 proj1fetch --data-dir proj1/data/ --years 2026 --months 1 2 3 4 5 6 7 8
```

Without `--months` all 12 months of each year are tried (unpublished months are skipped), so a 2026 fetch made later would also include September onwards and change every number. The assignment's full range is `--years 2016 2017 2018 2019 2020` (~12 GB); it was not used because no cluster was available.

### Dataset

- Trip records: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` ([NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page))
- Zone lookup: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Version used: downloaded 2026-10-08; 8 files, 488 MB, 29.7 M records (20.95 M after cleaning). TLC republishes files, so check the SHA-256 prefixes (`sha256sum proj1/data/*`):

  | File                              | SHA-256 (first 16 hex) |
  | --------------------------------- | ---------------------- |
  | `yellow_tripdata_2026-01.parquet` | `8b3933fe6f0d7b6d`     |
  | `yellow_tripdata_2026-02.parquet` | `5352ca800f29c40a`     |
  | `yellow_tripdata_2026-03.parquet` | `d7af794a7ac06cbb`     |
  | `yellow_tripdata_2026-04.parquet` | `e03b82d5722a6b21`     |
  | `yellow_tripdata_2026-05.parquet` | `9aa5a1609e2bf07d`     |
  | `yellow_tripdata_2026-06.parquet` | `f340c62a18885c56`     |
  | `yellow_tripdata_2026-07.parquet` | `3260bb629c2f3340`     |
  | `yellow_tripdata_2026-08.parquet` | `6683a025443afcbb`     |
  | `taxi_zone_lookup.csv`            | `1a99e105092230f8`     |

The dataset must not be committed, it stays under `proj1/data/` (git ignored).

## Running

### Local with uv

In dev environments it is much faster and easier to use uv.

Run a command (`M1`, `M2`, `M3`, `O1`, `O2`, `O4`, or `all` for the full pipeline) with:

```bash
uv run --package proj1 proj1 all -v
```

Useful options (see `uv run --package proj1 proj1 --help` for all of them):

| Option                                                                     | Meaning                                                                                                |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `--data-dir DIR`                                                           | Directory with the parquet files and `taxi_zone_lookup.csv` (default `./proj1/data`)                   |
| `--years 2016 2017`                                                        | Only load these years (default: every file in `--data-dir`)                                            |
| `--sample-fraction 0.1`                                                    | Run on a random 10% of the records (seed 42)                                                           |
| `--sink noop`                                                              | Compute every row without printing, use it for timing runs (`show` only computes ~20 rows)             |
| `--explain-plan`                                                           | Print the physical and logical plans                                                                   |
| `--keep-ui`                                                                | Wait for Enter before exiting so the Spark WebUI stays up                                              |
| `--transit-threshold`, `--commercial-threshold`, `--residential-threshold` | O4 classification thresholds                                                                           |
| `--[no-]<feature>`                                                         | Optimisation toggles (`broadcast-join`, `prune-column`, ...), all off by default except `use-cleansed` |

### Reproducibility

- Spark: 4.2.0 (PySpark from `uv.lock`), Python 3.14 locally, 3.10 in the Docker image (both work)
- Non-default Spark configuration, set in `create_spark_session`:
  - `spark.driver.memory=2g`, `spark.executor.memory=1536m`
  - `spark.sql.autoBroadcastJoinThreshold=-1` (no automatic broadcast, so the baseline shows every join as SortMergeJoin)
  - `spark.sql.session.timeZone=UTC` (TLC timestamps are NYC wall-clock times, this keeps hour/day of week independent of the host time zone)
- `spark.sql.files.maxPartitionBytes` is set from `--max-partition-mb` (default 128 MiB, Spark's default)
- Optimised configuration (all reports applied): `--broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache`
- Cluster: the DI cluster was not available; cluster runs use the Docker standalone cluster below (1 master, N workers of 2 cores / 2 GiB, Spark 4.2.0, driver in the `jupyter` container)
- `spark-submit` (from the `jupyter` container, entry point [submit.py](submit.py)):

  ```bash
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 proj1/submit.py all --master docker \
    --data-dir /repo/proj1/data --output-dir /repo/out/docker \
    --broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache --sink noop -v
  ```

### Reproducing the report

Every number in [the report](latex/main.tex) comes from a log under [reports/](reports/README.md). Measurement rules: `--sink noop`, 3 runs per configuration, median reported, machine idle (load average < 1.5) before each run. Local runs need nothing but uv; the Docker rows need the cluster below.

Shorthand: `P="uv run --package proj1 proj1"`, and `OPT="--broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache"` (final configuration).

| Report element                                                | Command (run 3 times)                                                                                                                                                                                                                                                                                                                                                       | Logs and details                                                                                                                                                                         |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Table 1, baseline                                             | `$P all --sink noop --explain-plan -v`                                                                                                                                                                                                                                                                                                                                      | [baseline](reports/baseline/README.md)                                                                                                                                                   |
| Per-stage numbers (§3, §4)                                    | `uv run python proj1/reports/tools/stages.py <OP> <out.txt> [flags]` (runs one op with the Spark UI up and dumps stage metrics from its REST API)                                                                                                                                                                                                                           | `logs/stages_*.txt` in each report                                                                                                                                                       |
| §4.1 broadcast joins                                          | `$P all --broadcast-join --sink noop --explain-plan -v`                                                                                                                                                                                                                                                                                                                     | [all/broadcast-join](reports/all/broadcast-join/README.md)                                                                                                                               |
| §4.2 cache vs write once                                      | add `--cache-dataframe`, then `--write-once` (M2)                                                                                                                                                                                                                                                                                                                           | [all/reuse-results](reports/all/reuse-results/README.md), [M2/write-once](reports/M2/write-once/README.md)                                                                               |
| §4.3 clean once                                               | add `--materialise-clean`                                                                                                                                                                                                                                                                                                                                                   | [all/materialise-clean](reports/all/materialise-clean/README.md)                                                                                                                         |
| §4.4 coalesce O2 cache                                        | `$P O2 $OPT --sink noop --explain-plan -v` with and without `--coalesce-cache`                                                                                                                                                                                                                                                                                              | [O2/coalesce-cache](reports/O2/coalesce-cache/README.md)                                                                                                                                 |
| Split size (Table 2 row)                                      | `$P all --broadcast-join --cache-dataframe --max-partition-mb <128\|64\|32\|16\|8> --sink noop -v`                                                                                                                                                                                                                                                                          | [all/split-size](reports/all/split-size/README.md)                                                                                                                                       |
| Table 2 last row, Figure 2                                    | `$P all <flags of each cumulative step> --sink noop -v`, steps: none; `--broadcast-join`; `+ --write-once --cache-dataframe`; `+ --materialise-clean`; `+ --coalesce-cache`                                                                                                                                                                                                 | [cumulative](reports/cumulative/logs/), chart: `uv run python proj1/reports/tools/plot_cumulative.py proj1/reports/cumulative/logs proj1/reports/cumulative/images/cumulative`           |
| Figure 3, Table 3, Table 4 (all cluster numbers, one session) | `zsh proj1/reports/docker-session/docker_session.zsh` (starts the Docker cluster, builds the month directories in the container, runs baseline, final, samples 0.1/0.2/0.5 and months 1/2/4 on 3 workers, the stage dumps, then 1/2/3 workers; about 2.5 h). Without zsh, run the same `docker compose exec ... python3 -m proj1 all --master docker ...` commands it lists | [docker-session](reports/docker-session/README.md), chart: `uv run python proj1/reports/tools/plot_scale.py proj1/reports/docker-session/logs proj1/reports/docker-session/images/scale` |
| §5 local supplement (dataset scale on `local[*]`)             | samples: `$P all $OPT --sample-fraction <0.1\|0.2\|0.5> --sink noop -v`; months: `uv run python proj1/reports/tools/month_dirs.py proj1/data <dir>`, then `$P all $OPT --data-dir <dir>/data_m<1\|2\|4\|8> --sink noop -v`                                                                                                                                                  | [scalability](reports/scalability/README.md)                                                                                                                                             |

`uv run python proj1/reports/tools/medians.py <logs...>` prints per-run metrics and medians from the profiler tables. The local dataset-scale series (§5 supplement) was measured before `--coalesce-cache` existed, so it uses `OPT` without it; the Docker session uses the full `OPT`.

The PDF is built with `latexmk -pdf main.tex` in [latex/](latex) (pdflatex, bibtex and makeglossaries).

### Docker

The standalone cluster in [docker-spark-env](../docker-spark-env/docker-compose.yml) runs Spark 4.2.0 with Python 3.10. It has 1 master and N workers of 2 cores / 2 GiB each. The driver runs in the `jupyter` container. Workers and driver mount the repository at `/repo`, so data and output paths are the same in every container. Workers run as root so executors can write outputs into the bind mount. Docker runs therefore write to `out/docker` (root-owned on the host), apart from the local `out/`.

```bash
cd docker-spark-env && docker compose up -d --scale spark-worker=3
docker compose exec -T -w /repo \
  -e PYTHONPATH=/repo/proj1/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
  jupyter python3 -m proj1 all --master docker --data-dir /repo/proj1/data --output-dir /repo/out/docker \
  --broadcast-join --cache-dataframe --write-once --materialise-clean --sink noop -v
```

Spark master UI: <http://localhost:8080>, application UI: <http://localhost:4040>. Clean up the root-owned output with `docker compose exec jupyter rm -rf /repo/out/docker`.

#### Setup

- Run [boot.sh](../boot.sh)
- Reload VsCode Window
- Select jupyter kernel in top right

> [!NOTE]
> Reminder that master in the notebooks must be changed to `spark://spark-master:7077`

#### Adding pip packages

- Add packages to the [Dockerfile](../docker-spark-env/Dockerfile)
- Run [build.sh](../build.sh)
- Restart the containers
