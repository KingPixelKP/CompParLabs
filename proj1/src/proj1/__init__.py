import argparse
import pathlib
from enum import Enum
import logging
import functools
from typing import get_type_hints
import pandas as pd
from dataclasses import dataclass, field, fields

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
from pyspark import StorageLevel

import proj1.spark_profiler

logger = None


class Command(Enum):
    M1 = "M1"
    M2 = "M2"
    M3 = "M3"
    OX1 = "OX1"
    OX2 = "OX2"

    def __str__(self):
        return self.value


class Master(Enum):
    Local = ("local", "local[*]")
    Docker = ("docker", "spark://spark-master:7077")

    def __init__(self, value: str, url: str):
        self._value_ = value
        self.url = url

    def __str__(self):
        return self.value


@dataclass(frozen=True)
class Features:
    explain_plan: bool = field(
        default=False,
        metadata={"help": "Emit physical and logical plans."},
    )
    use_cleansed: bool = field(
        default=False,
        metadata={
            "help": "Use the output DataFrame of M1 in all other commands (No op on M1)"
        },
    )
    prune_column: bool = field(
        default=True,
        metadata={"help": "Remove unused columns before processing."},
    )
    broadcast_join: bool = field(
        default=True,
        metadata={"help": "Broadcast the zone DataFrame during joins."},
    )
    cache_dataframe: bool = field(
        default=False,
        metadata={"help": "Cache intermediate DataFrames for reuse."},
    )


def add_dataclass_arguments(parser: argparse.ArgumentParser, cls: type) -> None:
    hints = get_type_hints(cls)

    for f in fields(cls):
        help_text = f.metadata.get("help", "")
        option = f"--{f.name.replace('_', '-')}"

        if hints[f.name] is bool:
            parser.add_argument(
                option,
                action=argparse.BooleanOptionalAction,
                default=f.default,
                help=help_text,
            )
        else:
            parser.add_argument(
                option,
                type=hints[f.name],
                default=f.default,
                help=help_text,
            )


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "command",
        type=Command,
        choices=list(Command),
        help="Command to run",
    )
    parser.add_argument(
        "-m",
        "--master",
        type=Master,
        default=Master.Local,
        help=f"Spark master to use (available: {', '.join(m.value for m in Master)})",
    )
    parser.add_argument(
        "--data-dir",
        type=pathlib.Path,
        default="./proj1/data",
        help="Data dir to retrieve data from, if not given it'll be inferred from the master",
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
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Be verbose",
    )
    add_dataclass_arguments(parser, Features)
    return parser


def create_spark_session(master: Master):
    return (
        SparkSession.builder.appName("proj1")
        .master(master.url)
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


def get_clean(trip_df: DataFrame):
    not_null = functools.reduce(
        lambda a, b: a & b, [sf.col(c).isNotNull() for c in trip_df.columns]
    )

    valid_df = trip_df.filter(
        not_null
        & (sf.col("fare_amount") > 0)
        & (sf.col("trip_distance") > 0)
        & sf.col("passenger_count").between(1, 6)
        & (sf.col("tpep_dropoff_datetime") > sf.col("tpep_pickup_datetime"))
        & sf.col("PULocationID").between(1, 263)
        & sf.col("DOLocationID").between(1, 263)
    )

    return valid_df


def explain_df(df: DataFrame, features: Features):

    if not features.explain_plan:
        return

    df.explain("formatted")
    df.explain(True)


def run_M1(trip_df: DataFrame, features: Features):

    clean = get_clean(trip_df)

    explain_df(clean, features)

    print(f"Cleansed: {trip_df.count() - clean.count()} rows")


def run_M2(
    trip_df: DataFrame, zone_df: DataFrame, features: Features, output_dir: pathlib.Path
):
    pickup_ts = sf.timestamp_micros("tpep_pickup_datetime")

    if features.use_cleansed:
        trip_df = get_clean(trip_df)

    if features.prune_column:
        zone_df = zone_df.select("Zone", "Borough", "LocationID")
        trip_df = trip_df.select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "fare_amount",
        )

    zone_to_join = sf.broadcast(zone_df) if features.broadcast_join else zone_df

    dated_df = (
        trip_df.join(
            zone_to_join,
            trip_df.PULocationID == zone_df.LocationID,
        )
        .withColumn("day_of_week", sf.dayofweek(pickup_ts))
        .withColumn("hour_of_day", sf.hour(pickup_ts))
        .withColumn(
            "trip_duration_minutes",
            (sf.col("tpep_dropoff_datetime") - sf.col("tpep_pickup_datetime"))
            / 60_000_000,
        )
        .groupBy("Borough", "day_of_week", "hour_of_day")
        .agg(
            sf.count("*").alias("total_trip_count"),
            sf.mean("fare_amount").alias("mean_fare_amount"),
            sf.mean("trip_duration_minutes").alias("mean_trip_duration_minutes"),
            sf.mean("trip_distance").alias("mean_trip_distance"),
        )
    )

    explain_df(dated_df, features)

    dated_df.show()

    dated_df.write.mode("overwrite").partitionBy("Borough").parquet(
        str(output_dir / "temporal")
    )


def run_M3(trip_df: DataFrame, zone_df: DataFrame, features: Features):
    pickup_ts = sf.timestamp_micros("tpep_pickup_datetime")

    if features.use_cleansed:
        trip_df = get_clean(trip_df)

    if features.prune_column:
        zone_df = zone_df.select(zone_df.Zone, zone_df.Borough, zone_df.LocationID)
        trip_df = trip_df.select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "fare_amount",
        )

    zone_to_join = sf.broadcast(zone_df) if features.broadcast_join else zone_df

    base = (
        trip_df.join(
            zone_to_join,
            trip_df.PULocationID == zone_df.LocationID,
        )
        .withColumn("pickup_month", sf.month(pickup_ts))
        .groupBy("Borough", "Zone", "pickup_month")
        .agg(
            sf.count("*").alias("trip_volume"),
            sf.mean("fare_amount").alias("mean_fare"),
            sf.stddev("fare_amount").alias("stddev_fare"),
            sf.mean("trip_distance").alias("mean_distance"),
            sf.stddev("trip_distance").alias("stddev_distance"),
        )
    )

    # every window's partition keys include Borough
    rank_w = Window.partitionBy("Borough", "pickup_month").orderBy(
        sf.col("trip_volume").desc()
    )
    zone_w = Window.partitionBy("Borough", "Zone").orderBy("pickup_month")
    roll_3 = zone_w.rowsBetween(-2, 0)
    hist_w = zone_w.rowsBetween(-2, -1)
    boro_w = Window.partitionBy("Borough").orderBy("pickup_month").rangeBetween(-2, 0)

    monthly = (
        base.withColumn("volume_rank", sf.dense_rank().over(rank_w))
        .withColumn("rolling_3m_volume", sf.sum("trip_volume").over(roll_3))
        .withColumn("rolling_mean_volume", sf.mean("trip_volume").over(hist_w))
        .withColumn("rolling_stddev_volume", sf.stddev("trip_volume").over(hist_w))
        .withColumn("borough_rolling_3m_volume", sf.sum("trip_volume").over(boro_w))
        .withColumn(
            "rolling_3m_trip_share",
            sf.col("rolling_3m_volume") / sf.col("borough_rolling_3m_volume"),
        )
        .withColumn(
            "volume_anomaly",
            sf.abs(sf.col("trip_volume") - sf.col("rolling_mean_volume"))
            > 1.5 * sf.col("rolling_stddev_volume"),
        )
    )

    explain_df(monthly, features)

    monthly.show()


def main() -> None:
    args = build_arg_parser().parse_args()
    from dataclasses import fields

    feature_names = {f.name for f in fields(Features)}
    features = Features(**{k: v for k, v in vars(args).items() if k in feature_names})

    logging.basicConfig(
        level=(
            (
                logging.DEBUG
                if args.debug
                else logging.INFO if args.verbose else logging.WARNING
            ),
        ),
    )

    global logger
    logger = logging.getLogger(__name__)

    logger.debug("Creating Spark Session")
    spark = create_spark_session(args.master)
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

    profiler = proj1.spark_profiler.SparkProfiler(spark)
    with profiler.profile(f"{args.command}"):
        if args.command == Command.M1:
            run_M1(
                parquet_df,
                features,
            )
        elif args.command == Command.M2:
            run_M2(
                parquet_df,
                zone_df,
                features,
                args.output_dir,
            )
        elif args.command == Command.M3:
            run_M3(
                parquet_df,
                zone_df,
                features,
            )
        else:
            raise NotImplementedError("Nope!!!")

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.expand_frame_repr",
        False,
    ):
        print(profiler.report())

    logger.info("Done")

    input("Keeping WebUI Up, Hit Enter to Exit!")
