import argparse
import pathlib
from enum import Enum
import logging

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

    not_null = ""

    for col in trip_data.columns:
        not_null += f" and {col} is not null"

    m1_df = trip_data.filter(f"""fare_amount <= 0
    and trip_distance <= 0
    and (passenger_count = 0 or passenger_count > 6)
    and not (tpep_pickup_datetime = tpep_dropoff_datetime)
    and (PULocationID >= 1 and PULocationID <= 263)
    and (DOLocationID >= 1 and DOLocationID <= 263){not_null}""")

    print(f"Cleansed: {trip_data.count() - m1_df.count()} rows")


def run_M2(trip_data: DataFrame, zone_data: DataFrame):
    dated_df = (
        (
            trip_data.withColumn(
                "day_of_week",
                sf.dayofweek(sf.timestamp_micros(trip_data.tpep_pickup_datetime)),
            ).withColumn(
                "hour_of_day",
                sf.hour(sf.timestamp_micros(trip_data.tpep_pickup_datetime)),
            )
            # Broadcast the zone_data frame (it's small)
            .join(
                zone_data.hint("merge"), trip_data.PULocationID == zone_data.LocationID
            )
        )
        .groupBy("day_of_week", "hour_of_day", "LocationID")
        .agg(
            sf.count("fare_amount").alias("count"),
            sf.mean("fare_amount").alias("mean_fare"),
            sf.mean("trip_distance").alias("mean_distance"),
        )
        .sort("count", ascending=False)
    )
    dated_df.show(dated_df.count(), truncate=False)


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
        run_M2(parquet_df, zone_df)
    else:
        print("Fuck you")
