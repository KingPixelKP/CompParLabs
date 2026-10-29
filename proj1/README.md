# Comp Par Project 1

## Setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

To fetch any data to be used by the project the python script [fetch](./src/proj1/fetch.py) is provided you can run it with:

```bash
uv run --package proj1 proj1fetch --data-dir proj1/data/ --years 2016 2017 2018 2019 2020
```

It downloads every month of the given years (default 2016-2020, as in the assignment) plus the zone lookup table. Months not yet published are skipped.

### Dataset

- Trip records: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` ([NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page))
- Zone lookup: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Version: TODO record the download date, TLC republishes files

The dataset must not be committed, it stays under `proj1/data/` (git ignored).

## Running

### Local with uv

In dev environments it is much faster and easier to use uv.

Run a command (`M1`, `M2`, `M3`, `O1`, `O2`, `O4`, or `all` for the full pipeline) with:

```bash
uv run --package proj1 proj1 all -v
```

Useful options (see `uv run --package proj1 proj1 --help` for all of them):

| Option | Meaning |
| --- | --- |
| `--data-dir DIR` | Directory with the parquet files and `taxi_zone_lookup.csv` (default `./proj1/data`) |
| `--years 2016 2017` | Only load these years (default: every file in `--data-dir`) |
| `--sample-fraction 0.1` | Run on a random 10% of the records (seed 42) |
| `--sink noop` | Compute every row without printing, use it for timing runs (`show` only computes ~20 rows) |
| `--explain-plan` | Print the physical and logical plans |
| `--keep-ui` | Wait for Enter before exiting so the Spark WebUI stays up |
| `--transit-threshold`, `--commercial-threshold`, `--residential-threshold` | O4 classification thresholds |
| `--[no-]<feature>` | Optimisation toggles (`broadcast-join`, `prune-column`, ...), all off by default except `use-cleansed` |

### Reproducibility

- Spark: 4.2.0 (PySpark from `uv.lock`), Python 3.14
- Non-default Spark configuration, set in `create_spark_session`:
  - `spark.driver.memory=2g`, `spark.executor.memory=1536m`
  - `spark.sql.autoBroadcastJoinThreshold=-1` (no automatic broadcast, so the baseline shows every join as SortMergeJoin)
  - `spark.sql.session.timeZone=UTC` (TLC timestamps are NYC wall-clock times, this keeps hour/day of week independent of the host time zone)
- TODO: DI cluster topology (workers, cores, memory per worker) and `spark-submit` command

### Docker

TODO: Check if docker can use uv too
TODO: Update docker to include data file

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
