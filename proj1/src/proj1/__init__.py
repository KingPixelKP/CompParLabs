import argparse
import pathlib
from enum import Enum
import logging
import functools
from typing import get_type_hints
import pandas as pd
from dataclasses import dataclass, field, fields
import sys

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
    O1 = "O1"
    O2 = "O2"

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
        default=False,
        metadata={"help": "Remove unused columns before processing."},
    )
    broadcast_join: bool = field(
        default=False,
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
        .config(
            "spark.sql.autoBroadcastJoinThreshold", "-1"
        )  # NOTE: This is for profiling purposes
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

    zone_df = sf.broadcast(zone_df) if features.broadcast_join else zone_df

    dated_df = (
        trip_df.join(
            zone_df,
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


def run_O1(trip_df: DataFrame, zone_df: DataFrame, features: Features):
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
            "payment_type",
        )

    zone_df = sf.broadcast(zone_df) if features.broadcast_join else zone_df

    base = (
        trip_df.join(
            zone_df,
            trip_df.PULocationID == zone_df.LocationID,
        )
        .withColumn(
            "per_km_fare",
            sf.try_divide(sf.col("fare_amount"), sf.col("trip_distance")),
        )
        .withColumn("hour_of_day", sf.hour(pickup_ts))
    )

    bucket_window = Window.partitionBy("PULocationID", "hour_of_day")

    base = (
        base.withColumn("mean_per_km", sf.mean("per_km_fare").over(bucket_window))
        .withColumn("stddev_per_km", sf.stddev("per_km_fare").over(bucket_window))
        .withColumn(
            "is_anomaly",
            sf.col("per_km_fare")
            > (sf.col("mean_per_km") + 3 * sf.col("stddev_per_km")),
        )
    )

    # What does this mean? TODO: compare weekday vs. weekend anomaly rates broken down by payment_type and pickup
    # borough.

    explain_df(base, features)

    base.show()


def run_O2(trip_df: DataFrame, zone_df: DataFrame, features: Features):
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
            "DOLocationID",
            "fare_amount",
            "payment_type",
        )

    zone_df = sf.broadcast(zone_df) if features.broadcast_join else zone_df

    if features.cache_dataframe:
        zone_df = zone_df.cache()  # This isn't worth it here it just adds more stages

    # 2. Count trips per route per month
    pickup_ts = sf.timestamp_micros("tpep_pickup_datetime")

    monthly_routes = (
        trip_df.withColumn("pickup_month", sf.month(pickup_ts))
        .groupBy(
            "PULocationID",
            "DOLocationID",
            "pickup_month",
        )
        .agg(sf.count("*").alias("trip_volume"))
    )

    # 3. Calculate total outbound trips and route shares
    outbound_w = Window.partitionBy(
        "PULocationID",
        "pickup_month",
    )

    monthly_routes = monthly_routes.withColumn(
        "month_volume",
        sf.sum("trip_volume").over(outbound_w),
    ).withColumn(
        "monthly_share",
        sf.col("trip_volume") / sf.col("month_volume"),
    )

    # 4. Resolve pickup and dropoff zone names and boroughs
    pickup_zones = zone_df.select(
        sf.col("LocationID").alias("pickup_id"),
        sf.col("Borough").alias("pickup_borough"),
    )

    dropoff_zones = zone_df.select(
        sf.col("LocationID").alias("dropoff_id"),
        sf.col("Borough").alias("dropoff_borough"),
    )

    routes = (
        monthly_routes.join(
            pickup_zones,
            monthly_routes.PULocationID == pickup_zones.pickup_id,
        )
        .join(
            dropoff_zones,
            monthly_routes.DOLocationID == dropoff_zones.dropoff_id,
        )
    )

    # 5. Find the top 20 routes per pickup borough
    borough_routes = routes.groupBy(
        "pickup_borough",
        "pickup_id",
        "dropoff_borough",
        "dropoff_id",
    ).agg(
        sf.sum("trip_volume").alias("total_trips"),
    )

    top_w = Window.partitionBy("pickup_borough").orderBy(
        sf.desc("total_trips"),
        sf.asc("pickup_id"),
        sf.asc("dropoff_id"),
    )

    top20 = borough_routes.withColumn(
        "route_rank",
        sf.row_number().over(top_w),
    ).filter(sf.col("route_rank") <= 20)

    # 6. Compare route shares between Q1 and Q3
    # Compare Jan -> Mar against Jul -> Sep.
    route_trends = (
        routes.groupBy(
            "PULocationID",
            "DOLocationID",
            "pickup_borough",
            "pickup_id",
            "dropoff_borough",
            "dropoff_id",
        )
        .pivot("pickup_month", list(range(1, 13)))
        .agg(sf.first("monthly_share"))
    )

    route_trends = route_trends.withColumn(
        "q1_change",
        sf.col("3") - sf.col("1"),
    ).withColumn(
        "q3_change",
        sf.col("9") - sf.col("7"),
    )

    reversals = route_trends.filter(
        ((sf.col("q1_change") > 0) & (sf.col("q3_change") < 0))
        | ((sf.col("q1_change") < 0) & (sf.col("q3_change") > 0))
    )

    # 7. Explain and display the results
    explain_df(routes, features)
    explain_df(top20, features)
    explain_df(reversals, features)

    print("Monthly route counts and shares:")
    routes.orderBy(
        "PULocationID",
        "pickup_month",
        sf.desc("trip_volume"),
    ).show(20, truncate=False)

    print("Top 20 routes per pickup borough:")
    top20.orderBy(
        "pickup_borough",
        "route_rank",
    ).show(100, truncate=False)

    print("Routes with Q1/Q3 share reversals:")
    reversals.select(
        "PULocationID",
        "DOLocationID",
        "pickup_id",
        "dropoff_id",
        "pickup_borough",
        "dropoff_borough",
        "1",
        "3",
        "7",
        "9",
        "q1_change",
        "q3_change",
    ).show(100, truncate=False)


def main() -> None:
    args = build_arg_parser().parse_args()
    from dataclasses import fields

    feature_names = {f.name for f in fields(Features)}
    features = Features(**{k: v for k, v in vars(args).items() if k in feature_names})

    logging.basicConfig(
        level=(
            logging.DEBUG
            if args.debug
            else logging.INFO if args.verbose else logging.WARNING
        ),
        stream=sys.stdout,
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

    logger.info(
        "autoBroadcastJoinThreshold=%s",
        spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
    )

    for f in fields(features):
        logger.info(f"{f.name}={getattr(features, f.name)}")

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

    trip_df = spark.read.schema(parquet_schema).parquet(
        *aggregate_files(args.data_dir, ".parquet")
    )
    zone_df = spark.read.schema(zone_schema).csv(
        *aggregate_files(args.data_dir, ".csv")
    )

    profiler = proj1.spark_profiler.SparkProfiler(spark)
    feature_str = ", ".join(
        f"{f.name}={getattr(features, f.name)}" for f in fields(features)
    )

    with profiler.profile(f"{args.command}-features:{feature_str}"):
        if args.command == Command.M1:
            run_M1(
                trip_df,
                features,
            )
        elif args.command == Command.M2:
            run_M2(
                trip_df,
                zone_df,
                features,
                args.output_dir,
            )
        elif args.command == Command.M3:
            run_M3(
                trip_df,
                zone_df,
                features,
            )
        elif args.command == Command.O1:
            run_O1(
                trip_df,
                zone_df,
                features,
            )
        elif args.command == Command.O2:
            run_O2(
                trip_df,
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
