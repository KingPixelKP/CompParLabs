import argparse
import operator
import pathlib
import re
from enum import Enum
import logging
import functools
from typing import get_type_hints
import pandas as pd
from dataclasses import dataclass, field, fields
import sys

import pyspark.sql.functions as sf
from pyspark.sql import (
    Column,
    SparkSession,
    Window,
    DataFrame,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    StringType,
    TimestampNTZType,
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
    O4 = "O4"
    All = "all"

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


class Sink(Enum):
    Show = "show"
    Noop = "noop"  # Computes every row without printing, use for timing runs

    def __str__(self):
        return self.value


@dataclass(frozen=True)
class Features:
    explain_plan: bool = field(
        default=False,
        metadata={"help": "Emit physical and logical plans."},
    )
    use_cleansed: bool = field(
        default=True,
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
        metadata={
            "help": "Broadcast the small side of joins (zone table, O4 bucket table)."
        },
    )
    cache_dataframe: bool = field(
        default=False,
        metadata={
            "help": "Persist the O2 routes reused by its 3 outputs (M2/O4 caches were slower, see reports/all/reuse-results)."
        },
    )
    string_to_id: bool = field(
        default=False,
        metadata={
            "help": "Convert string columns to integer IDs for joins."
        },  # TODO: Implement this feature
    )
    repartition_pickup: bool = field(
        default=False,
        metadata={"help": "Repartition trips by PULocationID before O4 bucketing."},
    )
    max_partition_mb: int = field(
        default=128,
        metadata={
            "help": "spark.sql.files.maxPartitionBytes in MiB, upper bound of a scan split (Spark default 128)."
        },
    )


@dataclass(frozen=True)
class O4Thresholds:
    transit_threshold: float = field(
        default=0.2,
        metadata={
            "help": "O4: min fraction of airport trips (RatecodeID in {2, 3}) for a transit bucket."
        },
    )
    commercial_threshold: float = field(
        default=0.01,
        metadata={
            "help": "O4: min fraction of negotiated fares (RatecodeID = 5) for a commercial bucket."
        },
    )
    residential_threshold: float = field(
        default=0.1,
        metadata={
            "help": "O4: min fraction of group trips (passenger_count >= 4) for a residential bucket."
        },
    )


# Columns of the assignment's schema table, with their documented types
TRIP_SCHEMA = StructType(
    [
        StructField("tpep_pickup_datetime", TimestampNTZType(), nullable=True),
        StructField("tpep_dropoff_datetime", TimestampNTZType(), nullable=True),
        StructField("PULocationID", IntegerType(), nullable=True),
        StructField("DOLocationID", IntegerType(), nullable=True),
        StructField("passenger_count", IntegerType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("RatecodeID", IntegerType(), nullable=True),
        StructField("payment_type", IntegerType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
    ]
)

# Columns every operation relies on, a null in any of them invalidates the record
REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
]


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


def dataclass_from_args(cls: type, args: argparse.Namespace):
    names = {f.name for f in fields(cls)}
    return cls(**{k: v for k, v in vars(args).items() if k in names})


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "command",
        type=Command,
        choices=list(Command),
        help="Command to run (all runs every command in order)",
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
        help="Directory holding the yellow_tripdata_YYYY-MM.parquet files and taxi_zone_lookup.csv",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help="Only load trip files of these years (default: every file in --data-dir)",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Run on a random fraction (0-1] of the trip records",
    )
    parser.add_argument(
        "--sink",
        type=Sink,
        choices=list(Sink),
        default=Sink.Show,
        help="show prints the first rows, noop computes every row without printing (timing runs)",
    )
    parser.add_argument(
        "-od",
        "--output-dir",
        type=pathlib.Path,
        default="out",
        help="If the chosen command has an output it'll be redirected here",
    )
    parser.add_argument(
        "--keep-ui",
        action="store_true",
        help="Wait for Enter before exiting to keep the Spark WebUI up",
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
    add_dataclass_arguments(parser, O4Thresholds)
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
        .config(
            "spark.sql.session.timeZone", "UTC"
        )  # TLC timestamps are NYC wall-clock times, keep hour/day independent of the host zone
        .getOrCreate()
    )


def aggregate_files(data_dir, suffix) -> list[str]:
    logger.info(f"Aggregating {suffix} files in {data_dir}")
    files = [
        str(path)
        for path in pathlib.Path(data_dir).iterdir()
        if path.is_file() and path.suffix == suffix
    ]

    logger.info(f"Found {files}")

    return files


def file_year(path: str) -> int:
    return int(re.search(r"(\d{4})-\d{2}\.parquet$", path).group(1))


def load_trips(spark: SparkSession, files: list[str]) -> DataFrame:
    # Older TLC files store some columns with other physical types (e.g.
    # passenger_count as double), so files are read in groups of equal schema and
    # each group is cast to TRIP_SCHEMA before the union.
    groups: dict[str, list[str]] = {}
    for path in files:
        schema = spark.read.parquet(path).schema
        groups.setdefault(schema.simpleString(), []).append(path)

    def conform(df: DataFrame) -> DataFrame:
        present = {c.lower() for c in df.columns}
        return df.select(
            *[
                (
                    sf.col(f.name).try_cast(f.dataType)
                    if f.name.lower() in present
                    else sf.lit(None).cast(f.dataType)
                ).alias(f.name)
                for f in TRIP_SCHEMA.fields
            ]
        )

    return functools.reduce(
        DataFrame.unionByName,
        [conform(spark.read.parquet(*paths)) for paths in groups.values()],
    )


def validity_rules() -> dict[str, Column]:
    """M1 cleaning criteria, a record is kept only if every rule holds.

    Keys name the criterion a record is removed for when its rule fails.
    """
    return {
        "null_field": functools.reduce(
            operator.and_, [sf.col(c).isNotNull() for c in REQUIRED_COLUMNS]
        ),
        "fare_le_0": sf.col("fare_amount") > 0,
        "distance_le_0": sf.col("trip_distance") > 0,
        "passengers_out_of_range": sf.col("passenger_count").between(1, 6),
        "dropoff_le_pickup": sf.col("tpep_dropoff_datetime")
        > sf.col("tpep_pickup_datetime"),
        "location_out_of_range": sf.col("PULocationID").between(1, 263)
        & sf.col("DOLocationID").between(1, 263),
    }


def get_clean(trip_df: DataFrame):
    return trip_df.filter(functools.reduce(operator.and_, validity_rules().values()))


def explain_df(df: DataFrame, features: Features):

    if not features.explain_plan:
        return

    df.explain("formatted")
    df.explain(True)


def emit(df: DataFrame, sink: Sink, rows: int = 20):
    if sink == Sink.Noop:
        df.write.format("noop").mode("overwrite").save()
    else:
        df.show(rows, truncate=False)


def run_M1(trip_df: DataFrame, features: Features, sink: Sink):
    rules = validity_rules()
    keep = functools.reduce(operator.and_, rules.values())

    # A record failing several criteria is counted under each of them, removed
    # counts it once
    stats = (
        trip_df.groupBy(sf.year("tpep_pickup_datetime").alias("year"))
        .agg(
            sf.count("*").alias("records"),
            *[sf.count_if(~rule).alias(name) for name, rule in rules.items()],
            sf.count_if(~sf.coalesce(keep, sf.lit(False))).alias("removed"),
        )
        .orderBy("year")
    )

    explain_df(stats, features)

    emit(stats, sink, rows=100)


def run_M2(
    trip_df: DataFrame,
    zone_df: DataFrame,
    features: Features,
    sink: Sink,
    output_dir: pathlib.Path,
):
    pickup_ts = sf.col("tpep_pickup_datetime")

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
            sf.timestamp_diff(
                "SECOND", "tpep_pickup_datetime", "tpep_dropoff_datetime"
            )
            / 60,
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

    emit(dated_df, sink)

    dated_df.write.mode("overwrite").partitionBy("Borough").parquet(
        str(output_dir / "temporal")
    )


def run_M3(trip_df: DataFrame, zone_df: DataFrame, features: Features, sink: Sink):
    pickup_ts = sf.col("tpep_pickup_datetime")

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

    # Zone names are not unique (e.g. Corona is LocationID 56 and 57), group by id
    base = (
        trip_df.join(
            zone_to_join,
            trip_df.PULocationID == zone_df.LocationID,
        )
        .withColumn("year", sf.year(pickup_ts))
        .withColumn("month", sf.month(pickup_ts))
        .groupBy("Borough", "PULocationID", "Zone", "year", "month")
        .agg(
            sf.count("*").alias("trip_volume"),
            sf.mean("fare_amount").alias("mean_fare"),
            sf.stddev("fare_amount").alias("stddev_fare"),
            sf.mean("trip_distance").alias("mean_distance"),
            sf.stddev("trip_distance").alias("stddev_distance"),
        )
        # consecutive months differ by 1, so range frames span calendar months
        .withColumn("month_index", sf.col("year") * 12 + sf.col("month"))
    )

    # every window's partition keys include Borough
    rank_w = Window.partitionBy("Borough", "month_index").orderBy(
        sf.col("trip_volume").desc()
    )
    zone_w = Window.partitionBy("Borough", "PULocationID").orderBy("month_index")
    roll_3 = zone_w.rangeBetween(-2, 0)
    hist_w = zone_w.rangeBetween(-2, -1)
    boro_w = Window.partitionBy("Borough").orderBy("month_index").rangeBetween(-2, 0)

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

    emit(monthly, sink)


def run_O1(trip_df: DataFrame, zone_df: DataFrame, features: Features, sink: Sink):
    pickup_ts = sf.col("tpep_pickup_datetime")

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
        .withColumn(
            "day_type",
            sf.when(sf.dayofweek(pickup_ts).isin(1, 7), "weekend").otherwise(
                "weekday"
            ),
        )
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

    anomaly_rates = (
        base.groupBy("Borough", "payment_type")
        .pivot("day_type", ["weekday", "weekend"])
        .agg(
            sf.avg(sf.col("is_anomaly").cast("int")).alias("anomaly_rate"),
            sf.count(sf.lit(1)).alias("trips"),  # pivot rejects count("*")
        )
        .withColumn(
            "weekend_minus_weekday_rate",
            sf.col("weekend_anomaly_rate") - sf.col("weekday_anomaly_rate"),
        )
        .orderBy("Borough", "payment_type")
    )

    explain_df(anomaly_rates, features)

    emit(anomaly_rates, sink, rows=100)


def run_O2(trip_df: DataFrame, zone_df: DataFrame, features: Features, sink: Sink):
    pickup_ts = sf.col("tpep_pickup_datetime")

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

    # 2. Count trips per route per month
    monthly_routes = (
        trip_df.withColumn("year", sf.year(pickup_ts))
        .withColumn("month", sf.month(pickup_ts))
        .groupBy(
            "PULocationID",
            "DOLocationID",
            "year",
            "month",
        )
        .agg(sf.count("*").alias("trip_volume"))
    )

    # 3. Calculate total outbound trips and route shares
    outbound_w = Window.partitionBy(
        "PULocationID",
        "year",
        "month",
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
        sf.col("Zone").alias("pickup_zone"),
        sf.col("Borough").alias("pickup_borough"),
    )

    dropoff_zones = zone_df.select(
        sf.col("LocationID").alias("dropoff_id"),
        sf.col("Zone").alias("dropoff_zone"),
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
        .drop("pickup_id", "dropoff_id")
    )

    # all 3 outputs are built from routes, without a cache each one rescans the trips
    if features.cache_dataframe:
        routes = routes.cache()

    # 5. Find the top 20 routes per pickup borough
    # ids are kept since zone names are not unique (e.g. Corona is 56 and 57)
    borough_routes = routes.groupBy(
        "PULocationID",
        "DOLocationID",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
    ).agg(
        sf.sum("trip_volume").alias("total_trips"),
    )

    top_w = Window.partitionBy("pickup_borough").orderBy(
        sf.desc("total_trips"),
        sf.asc("PULocationID"),
        sf.asc("DOLocationID"),
    )

    top20 = borough_routes.withColumn(
        "route_rank",
        sf.row_number().over(top_w),
    ).filter(sf.col("route_rank") <= 20)

    # 6. Compare route shares between Q1 and Q3 of each year
    # Compare Jan -> Mar against Jul -> Sep.
    route_trends = (
        routes.groupBy(
            "PULocationID",
            "DOLocationID",
            "pickup_borough",
            "pickup_zone",
            "dropoff_borough",
            "dropoff_zone",
            "year",
        )
        .pivot("month", list(range(1, 13)))
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
    emit(
        routes.orderBy(
            "PULocationID",
            "year",
            "month",
            sf.desc("trip_volume"),
        ),
        sink,
    )

    print("Top 20 routes per pickup borough:")
    emit(
        top20.orderBy(
            "pickup_borough",
            "route_rank",
        ),
        sink,
        rows=100,
    )

    print("Routes with Q1/Q3 share reversals:")
    emit(
        reversals.select(
            "PULocationID",
            "DOLocationID",
            "pickup_zone",
            "dropoff_zone",
            "pickup_borough",
            "dropoff_borough",
            "year",
            "1",
            "3",
            "7",
            "9",
            "q1_change",
            "q3_change",
        ),
        sink,
        rows=100,
    )

    if features.cache_dataframe:
        routes.unpersist()


def run_O4(
    trip_df: DataFrame,
    zone_df: DataFrame,
    features: Features,
    sink: Sink,
    thresholds: O4Thresholds,
):
    pickup_ts = sf.col("tpep_pickup_datetime")

    if features.use_cleansed:
        trip_df = get_clean(trip_df)

    trip_df = (
        trip_df.withColumn("hour_of_day", sf.hour(pickup_ts))
        .withColumn("year", sf.year(pickup_ts))
        .withColumn("month", sf.month(pickup_ts))
    )

    used_columns = [
        "PULocationID",
        "RatecodeID",
        "passenger_count",
        "year",
        "month",
        "hour_of_day",
        "fare_amount",
        "trip_distance",
    ]

    if features.prune_column:
        trip_df = trip_df.select(*used_columns)

    if features.repartition_pickup:
        trip_df = trip_df.repartition(
            "PULocationID"
        )  # TODO: Write a report on the performance impact of this repartitioning relevant logs under ./logs/O4/repartition and ./logs/O4/baseline.log

    # NOTE: Possible optimization use numbers instead of strings to identify each class
    buckets = (
        trip_df.groupBy("PULocationID", "hour_of_day")
        .agg(
            sf.avg(sf.when(sf.col("RatecodeID").isin(2, 3), 1).otherwise(0)).alias(
                "airport_fraction"
            ),
            sf.avg(sf.when(sf.col("RatecodeID") == 5, 1).otherwise(0)).alias(
                "negotiated_fraction"
            ),
            sf.avg(sf.when(sf.col("passenger_count") >= 4, 1).otherwise(0)).alias(
                "group_fraction"
            ),
        )
        .withColumn(
            "zone_type",
            sf.when(
                sf.col("airport_fraction") >= thresholds.transit_threshold, "transit"
            )
            .when(
                sf.col("negotiated_fraction") >= thresholds.commercial_threshold,
                "commercial",
            )
            .when(
                sf.col("group_fraction") >= thresholds.residential_threshold,
                "residential",
            )
            .otherwise("mixed"),
        )
    )

    buckets = sf.broadcast(buckets) if features.broadcast_join else buckets

    zone_type_stats = (
        trip_df.join(buckets, ["PULocationID", "hour_of_day"])
        .groupBy("zone_type", "year", "month")
        .agg(
            sf.count("*").alias("trips"),
            sf.mean("fare_amount").alias("mean_fare"),
            sf.stddev("fare_amount").alias("stddev_fare"),
            sf.mean("trip_distance").alias("mean_distance"),
            sf.stddev("trip_distance").alias("stddev_distance"),
        )
        .orderBy("zone_type", "year", "month")
    )

    explain_df(zone_type_stats, features)

    emit(zone_type_stats, sink, rows=100)


def main() -> None:
    args = build_arg_parser().parse_args()

    features = dataclass_from_args(Features, args)
    thresholds = dataclass_from_args(O4Thresholds, args)

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

    logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    logger.info(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

    logger.info(
        "autoBroadcastJoinThreshold=%s",
        spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
    )

    spark.conf.set("spark.sql.files.maxPartitionBytes", f"{features.max_partition_mb}m")

    for f in fields(features):
        logger.info(f"{f.name}={getattr(features, f.name)}")

    zone_schema = StructType(
        [
            StructField("LocationID", IntegerType(), nullable=True),
            StructField("Borough", StringType(), nullable=True),
            StructField("Zone", StringType(), nullable=True),
            StructField("service_zone", StringType(), nullable=True),
        ]
    )

    trip_files = [
        f
        for f in aggregate_files(args.data_dir, ".parquet")
        if args.years is None or file_year(f) in args.years
    ]
    if not trip_files:
        sys.exit(f"No trip files for years {args.years} in {args.data_dir}")

    trip_df = load_trips(spark, trip_files)
    if args.sample_fraction is not None:
        trip_df = trip_df.sample(fraction=args.sample_fraction, seed=42)

    zone_df = spark.read.schema(zone_schema).csv(
        *aggregate_files(args.data_dir, ".csv"), header=True
    )

    runners = {
        Command.M1: lambda: run_M1(trip_df, features, args.sink),
        Command.M2: lambda: run_M2(
            trip_df, zone_df, features, args.sink, args.output_dir
        ),
        Command.M3: lambda: run_M3(trip_df, zone_df, features, args.sink),
        Command.O1: lambda: run_O1(trip_df, zone_df, features, args.sink),
        Command.O2: lambda: run_O2(trip_df, zone_df, features, args.sink),
        Command.O4: lambda: run_O4(trip_df, zone_df, features, args.sink, thresholds),
    }
    commands = list(runners) if args.command == Command.All else [args.command]

    profiler = proj1.spark_profiler.SparkProfiler(spark)
    feature_str = ", ".join(
        f"{f.name}={getattr(features, f.name)}" for f in fields(features)
    )

    for command in commands:
        with profiler.profile(f"{command}-features:{feature_str}"):
            runners[command]()

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

    if args.keep_ui:
        input("Keeping WebUI Up, Hit Enter to Exit!")
