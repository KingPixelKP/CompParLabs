"""
run_tuned_pipeline.py
----------------------
Standalone, spark-submit-ready version of the "final" tuned pipeline from
profiling_exercises.ipynb (top rated movie of every year since 2000).

It exists for the optional "does it scale?" stretch goal: comparing how the
baseline vs. the tuned pipeline behave as the number of Docker workers
changes is much easier from a script you can spark-submit repeatedly than
from a notebook kernel you'd have to restart every time.

Usage (from inside the spark-master container, see README.md):

    spark-submit --master spark://spark-master:7077 \\
        /apps/lab03/run_tuned_pipeline.py \\
        --data /data/lab02/ --partitions 12

Or locally:

    spark-submit run_tuned_pipeline.py \\
        --data ../../compar-lab02-spark-dataframes/data/ --partitions 8
"""

import argparse
import time

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType


def build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="../../compar-lab02-spark-dataframes/data/",
                         help="Folder containing movies.csv.gz / ratings*.csv.gz")
    parser.add_argument("--partitions", type=int, default=None,
                         help="Target partition count for `ratings` after read "
                              "(defaults to 2x the driver's default parallelism)")
    return parser


def run(spark, data_folder, target_partitions):
    ratings_schema = StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", DoubleType(), False),
        StructField("timestamp", LongType(), False),
    ])

    movies = (spark.read.option("header", "true").option("inferSchema", "true")
              .csv(data_folder + "movies.csv.gz")
              .select("movieId", "title"))

    ratings = (spark.read.option("header", "true").schema(ratings_schema)
               .csv(data_folder + "ratings*.csv.gz")
               .repartition(target_partitions)
               .select("movieId", "rating"))

    movies_year = (movies
                   .withColumn("year_str", sf.regexp_extract("title", r'^(.*) \(([0-9 \-]*)\)$', 2))
                   .withColumn("year", sf.when(sf.col("year_str").rlike(r'^[0-9]+$'), sf.col("year_str").cast(IntegerType())))
                   .drop("year_str")
                   .filter("year IS NOT NULL")
                   .select("movieId", "title", "year")
                   .cache())
    movies_year.count()  # materialize the cache before it's used twice below

    avg_ratings = (movies_year.join(sf.broadcast(ratings), on="movieId")
                   .groupBy("year", "title")
                   .agg(sf.round(sf.avg("rating"), 2).alias("avg_rating"),
                        sf.count("*").alias("num_ratings"))
                   .filter("num_ratings > 100"))

    w = Window.partitionBy("year").orderBy(sf.desc("avg_rating"))
    top_per_year = (avg_ratings
                    .withColumn("rank", sf.row_number().over(w))
                    .filter("rank = 1 and year >= 2000")
                    .orderBy("year")
                    .select("year", "title", "avg_rating", "num_ratings"))

    top_per_year.show(30, truncate=False)
    movies_year.unpersist()


def main():
    args = build_arg_parser().parse_args()

    spark = (SparkSession.builder
             .appName("spark-profiling-tuned-pipeline")
             .config("spark.driver.memory", "2g")
             .config("spark.executor.memory", "1536m")   # stay under the worker's --memory 2G cap
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())

    target_partitions = args.partitions or spark.sparkContext.defaultParallelism * 2

    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"Target partitions for ratings: {target_partitions}")

    start = time.perf_counter()
    run(spark, args.data, target_partitions)
    elapsed = time.perf_counter() - start
    print(f"Wall time: {elapsed:.2f}s")

    spark.stop()


if __name__ == "__main__":
    main()
