# Profiling & Optimizing PySpark Computations

This lab is a continuation of `compar-lab02-spark-dataframes`. Where lab02
was about *expressing* computations with the DataFrame API, this lab is
about *understanding what Spark actually does when it runs one*, and how to
make it faster: reading a job's profile, spotting the bottleneck, applying a
targeted fix, and measuring the impact.

The lab uses the `compar-spark-docker` environment (1 Spark master + N Spark
workers + JupyterLab) so that partitioning and parallelism decisions have a
real, visible effect across multiple worker containers.

## 🛠️ Setup

1. Update the **Docker Compose** file (see `compar-spark-docker`) with the following configurations:
   * **Shared Folders:** Map shared folders for both `apps` and `notebooks`. 
     * *Note:* This lab reuses the data from `lab02` (`movies.csv.gz`, `ratings1.csv.gz`, and `ratings2.csv.gz`) to avoid duplication. The notebooks are configured to point to `../../compar-lab02-spark-dataframes/data/` (or `/data/lab02/` inside the Docker container). 
     * If you already set up a data folder for `lab02`, no further action is needed. Otherwise, create a data folder for this lab and update the notebook paths accordingly.
   * **Port Mapping:** Map ports `4040` to `4050` to the host machine in the `jupyter` service configuration:
     ```yaml
     ports:
       - "8888:8888"
       - "4040-4050:4040-4050"
     ```
   
2. Add the **pandas** package to the `Dockerfile` (if you have not already done so for `lab02`):
   ```dockerfile
   RUN pip install --no-cache-dir \
       jupyterlab \
       py4j \
       pandas
   ```

3. **Rebuild and start** the Docker cluster (refer to `compar-spark-docker/README.md`), specifying the desired number of workers. For example:

```
cd path-to-compar-spark-docker/docker-spark-env
docker compose up -d --scale spark-worker=3  --remove-orphans --build
```

4. Open the **Spark Master UI** at **http://localhost:8080**, and once
   a notebook has an active `SparkSession`, the **Spark Application UI** at
   **http://localhost:4040** (the port increments to 4041, 4042, ... if a
   previous session's UI is still bound). Keep this tab open throughout the
   lab - it's the visual counterpart to the numbers the profiler prints.

You can also run everything locally (same Python environment as lab01/lab02)
by leaving `master = "local[*]"` in the notebooks - the profiling method is
the same, but the partitioning exercises are far less interesting with a
single JVM playing both master and worker.



## 🔎 The profiler

`notebooks/spark_profiler.py` is a small, dependency-free helper: it tags
the Spark jobs launched inside a `with profiler.profile("name"):` block and
pulls their stage metrics (wall time, task count, shuffle read/write,
memory/disk spill, executor run/CPU time) from the Spark Application UI's
REST API. No extra JVM jar or `pip install` is required - it is built only
on `SparkContext.setJobGroup` and Spark's own monitoring API.

```python
from spark_profiler import SparkProfiler
profiler = SparkProfiler(spark)

with profiler.profile("baseline"):
    ratings.groupBy("movieId").count().collect()

# ... apply a fix ...

with profiler.profile("optimized"):
    ratings.repartition(12).groupBy("movieId").count().collect()

profiler.report()                       # every run so far, one row each
profiler.compare("baseline", "optimized")   # before/after + % change
```

## 🎬 Showcase

`profiling_showcase.ipynb` walks through four worked examples on the
MovieLens data, each following profile → diagnose → fix → profile → compare:

1. **Partitioning** - why `ratings1.csv.gz` (non-splittable gzip) reads into
   just 1 or 2 partitions no matter how many workers you have, and what
   `repartition()` fixes.
2. **Schema & column pruning** - the cost of `inferSchema`, and of carrying
   unused columns through a shuffle.
3. **Join strategy** - shuffle (sort-merge) join vs broadcast join between
   the large `ratings` table and the tiny `movies` table.
4. **Adaptive Query Execution (AQE)** - how automatic shuffle-partition
   coalescing changes the task count (not the bytes shuffled) of a group-by
   on a small cluster, with `spark.sql.adaptive.enabled` toggled off/on.

Examples 2 and 3 reduce *how much* gets shuffled (narrower rows, or no
shuffle at all for the join); example 1 is about parallelism, not shuffle
reduction - `repartition()` is itself a shuffle, paid for deliberately to
fix under-parallelization; example 4 reduces the *overhead* of a shuffle
without changing its size.

## 📝 Work to develop

`profiling_exercises.ipynb`: starting from the naive, unoptimized version of
the *"top rated movie of every year since 2000"* pipeline (the query left
unfinished in lab02's `movies.ipynb`), profile the baseline and then work
through:

* Exercise A - fix the partitioning problem.
* Exercise B - explicit schema + column pruning.
* Exercise C - check and, if needed, force the right join strategy.
* Exercise E - push a late filter (`year >= 2000`) back before the join,
  instead of after it.
* Exercise D - decide whether a reused DataFrame is worth caching.
* Final - combine every fix, profile the tuned pipeline end to end, and
  quantify the overall improvement against the baseline.
* Stretch goal (optional) - re-run the baseline and the tuned pipeline
  against different worker counts (`--scale spark-worker=N`) and see which
  one actually benefits from more workers.

`apps/run_tuned_pipeline.py` is a standalone `spark-submit`-ready version of
the final pipeline, useful for the stretch goal so you don't have to keep a
notebook kernel alive across cluster restarts:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /apps/lab03/run_tuned_pipeline.py --partitions 12
```

## 📚 Supporting Documentation

* [Spark Monitoring and Instrumentation (Web UI & REST API)](https://spark.apache.org/docs/latest/monitoring.html)
* [Spark SQL Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
* [Spark Configuration Reference](https://spark.apache.org/docs/latest/configuration.html)
* [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programmingguide.html)