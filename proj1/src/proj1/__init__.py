import argparse
import pathlib
from enum import Enum
import logging
import functools

import pyspark.sql.functions as sf
from pyspark.sql import (
    SparkSession,
    Window,
    DataFrame,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    StringType,
)

import proj1.spark_profiler

logger = None


class Command(Enum):
    M1 = "M1"
    M2 = "M2"
    M3 = "M3"

    def __str__(self):
        return self.value


def build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        type=Command,
        choices=list(Command),
        help="Command to run",
    )
    parser.add_argument(
        "-m",
        "--master",
        type=str,
        default="local[*]",
        help="Spark master to use",
    )
    parser.add_argument(
        "--data-dir",
        type=pathlib.Path,
        default="./proj1/data",
        help="Data dir to retrieve data from, if not given it'll be infered from the master",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        help="Target partition count for `ratings` after read "
        "(defaults to 2x the driver's default parallelism)",
    )
    parser.add_argument(
        "-od",
        "--output-dir",
        type=pathlib.Path,
        default="out",
        help="If the chosen command has an output it'll be redirected here",
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Print lots of debugging statements",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.WARNING,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Be verbose",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
    )
    return parser


def create_spark_session():
    return (
        SparkSession.builder.appName("spark-profiling-tuned-pipeline")
        .config("spark.driver.memory", "2g")
        .config(
            "spark.executor.memory", "1536m"
        )  # stay under the worker's --memory 2G cap
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def aggregate_files(data_dir, suffix) -> list[pathlib.Path]:
    logger.info(f"Aggregating {suffix} files in {data_dir}")
    files = [
        str(path)
        for path in pathlib.Path(data_dir).iterdir()
        if path.is_file() and path.suffix == suffix
    ]

    logger.info(f"Found {files}")

    return files


def run_M1(trip_data: DataFrame):

    not_null = functools.reduce(
        lambda a, b: a & b, [sf.col(c).isNotNull() for c in trip_data.columns]
    )

    valid_df = trip_data.filter(
        not_null
        & (sf.col("fare_amount") > 0)
        & (sf.col("trip_distance") > 0)
        & sf.col("passenger_count").between(1, 6)
        & (sf.col("tpep_dropoff_datetime") > sf.col("tpep_pickup_datetime"))
        & sf.col("PULocationID").between(1, 263)
        & sf.col("DOLocationID").between(1, 263)
    )

    print(f"Cleansed: {trip_data.count() - valid_df.count()} rows")


def run_M2(trip_data: DataFrame, zone_data: DataFrame, output_dir: pathlib.Path):
    pickup_ts = sf.timestamp_micros("tpep_pickup_datetime")

    dated_df = (
        trip_data.withColumn("day_of_week", sf.dayofweek(pickup_ts))
        .withColumn("hour_of_day", sf.hour(pickup_ts))
        .withColumn(
            "trip_duration_minutes",
            (sf.col("tpep_dropoff_datetime") - sf.col("tpep_pickup_datetime"))
            / 60_000_000,
        )
        .join(sf.broadcast(zone_data), trip_data.PULocationID == zone_data.LocationID)
        .groupBy("Borough", "day_of_week", "hour_of_day")
        .agg(
            sf.count("*").alias("total_trip_count"),
            sf.mean("fare_amount").alias("mean_fare_amount"),
            sf.mean("trip_duration_minutes").alias("mean_trip_duration_minutes"),
            sf.mean("trip_distance").alias("mean_trip_distance"),
        )
        .withColumnRenamed("Borough", "pickup_borough")
    )
    dated_df.show()

    dated_df.write.mode("overwrite").partitionBy("pickup_borough").parquet(
        str(output_dir / "temporal")
    )


def main() -> None:
    args = build_arg_parser().parse_args()
    logging.basicConfig(level=args.loglevel)

    global logger
    logger = logging.getLogger(__name__)

    logger.debug("Creating Spark Session")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.debug("Spark Session Created")

    target_partitions = args.partitions or spark.sparkContext.defaultParallelism * 2

    logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    logger.info(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    logger.info(f"Target partitions for ratings: {target_partitions}")

    parquet_schema = StructType(
        [
            StructField("VendorID", IntegerType(), nullable=True),
            StructField("tpep_pickup_datetime", LongType(), nullable=True),
            StructField("tpep_dropoff_datetime", LongType(), nullable=True),
            StructField("passenger_count", LongType(), nullable=True),
            StructField("trip_distance", DoubleType(), nullable=True),
            StructField("RatecodeID", LongType(), nullable=True),
            StructField("store_and_fwd_flag", StringType(), nullable=True),
            StructField("PULocationID", IntegerType(), nullable=True),
            StructField("DOLocationID", IntegerType(), nullable=True),
            StructField("payment_type", LongType(), nullable=True),
            StructField("fare_amount", DoubleType(), nullable=True),
            StructField("extra", DoubleType(), nullable=True),
            StructField("mta_tax", DoubleType(), nullable=True),
            StructField("tip_amount", DoubleType(), nullable=True),
            StructField("tolls_amount", DoubleType(), nullable=True),
            StructField("improvement_surcharge", DoubleType(), nullable=True),
            StructField("total_amount", DoubleType(), nullable=True),
            StructField("congestion_surcharge", DoubleType(), nullable=True),
            StructField("Airport_fee", DoubleType(), nullable=True),
            StructField("cbd_congestion_fee", DoubleType(), nullable=True),
        ]
    )

    zone_schema = StructType(
        [
            StructField("LocationID", IntegerType(), nullable=True),
            StructField("Borough", StringType(), nullable=True),
            StructField("Zone", StringType(), nullable=True),
            StructField("service_zone", StringType(), nullable=True),
        ]
    )

    parquet_df = spark.read.schema(parquet_schema).parquet(
        *aggregate_files(args.data_dir, ".parquet")
    )
    zone_df = spark.read.schema(zone_schema).csv(
        *aggregate_files(args.data_dir, ".csv")
    )

    if args.command == Command.M1:
        run_M1(parquet_df)
    elif args.command == Command.M2:
        run_M2(parquet_df, zone_df, args.output_dir)
    else:
        print("Fuck you")
