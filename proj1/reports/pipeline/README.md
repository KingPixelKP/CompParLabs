# Pipeline

Source for LaTeX sections 1 (Introduction) and 2 (Pipeline). Keep the final text to at most one page.

## Dataset

- NYC TLC Yellow Taxi trip records, one parquet file per month, plus `taxi_zone_lookup.csv` (265 zones). Download URLs are in the [project README](../../README.md#dataset).
- Development data: 2026-01..08, 29.7 M records, ~500 MB of parquet. Target data: 2016-2020 (~500 M records, ~12 GB).
- Operations reported: M1, M2, M3 (mandatory), O2 and O4 (optional).

## Why the workload is interesting for performance

Every operation except M1 joins the large trip table with the small zone table and then aggregates or windows on keys with very uneven sizes (a few Manhattan zones and JFK hold a large share of trips). This exposes the classic distributed costs:

- shuffle volume (joins, `groupBy`, windows),
- join strategy (sort-merge vs broadcast against a 265-row table),
- skewed partitions (hot pickup zones),
- recomputation (every operation re-reads and re-cleans the same data).

## Stages

All operations share the load and clean steps:

1. **Load** (`load_trips`): files are grouped by physical parquet schema (2026 has 2 variants, one adds `request_source`). Each group is cast to the documented schema (`TRIP_SCHEMA`) and the groups are combined with `unionByName`. Older years store e.g. `passenger_count` as double, so a single fixed read schema fails on them.
2. **Clean** (`get_clean`, M1 rules): drop records with a null in a used column, `fare_amount ≤ 0`, `trip_distance ≤ 0`, `passenger_count` outside 1-6, dropoff ≤ pickup, or a location id outside 1-263.

| Op | What it computes                                                                                                                           | Main Spark operators                                                       |
| -- | ------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| M1 | Records removed per criterion and per year                                                                                                 | Scan, one `groupBy(year)` with conditional counts                          |
| M2 | Trips, mean fare, duration and distance per (pickup borough, day of week, hour); written as parquet partitioned by borough                 | Join with zones, `groupBy`, partitioned write                              |
| M3 | Per pickup zone and month: mean/σ of fare and distance, volume rank in borough, rolling 3-month share, volume anomaly flag (> 1.5σ)        | Join with zones, `groupBy`, 4 windows (rank, rolling zone, rolling borough) |
| O2 | Monthly route counts and outbound share, top-20 routes per pickup borough, routes whose share trend reverses between Q1 and Q3             | `groupBy`, window, 2 joins with zones, `row_number`, `pivot`               |
| O4 | Airport / negotiated / group trip fractions per (pickup zone, hour), one label per bucket, joined back to every trip, fare and distance stats per label and month | `groupBy`, join back to the trip table, `groupBy`                          |

## Design decisions

- **DataFrame API everywhere, no Spark SQL strings**: the operations are built step by step from shared pieces (cleaning rules, zone joins, feature flags that switch a join to broadcast). This is easier to compose in Python than SQL strings. Both compile to the same Catalyst plans, so performance does not depend on this choice.
- **No UDFs**: every expression is a built-in function, so Catalyst can optimise it and nothing goes through Python workers.
- **Timestamps**: TLC timestamps are NYC wall-clock times without a zone. They are read as `timestamp_ntz` and the session zone is fixed to UTC, so `hour()`/`dayofweek()` give the same result on any machine.
- **Keys**: M3 and O2 group by `PULocationID`/`DOLocationID`, not zone names, because names are not unique (Corona is 56 and 57). Months are keyed by (year, month) so multi-year data does not merge January 2016 with January 2017.
- **Baseline configuration**: `spark.sql.autoBroadcastJoinThreshold=-1`, so every join starts as a sort-merge join and broadcasting is an explicit, measured optimisation. Optimisations are feature flags (`--broadcast-join`, `--prune-column`, ...), all off in the baseline.

## Physical plan: M2

From [baseline/logs/all_1.log](../baseline/logs/all_1.log) (`explain(True)`, before AQE runs):

```text
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[Borough, day_of_week, hour_of_day], functions=[count(1), avg(...) x3])     ← final aggregate
   +- Exchange hashpartitioning(Borough, day_of_week, hour_of_day, 200)                             ← shuffle 2: partial aggregates only
      +- HashAggregate(keys=[...], functions=[partial_count(1), partial_avg(...) x3])                ← map-side partial aggregation
         +- Project [trip_distance, fare_amount, Borough, dayofweek(...), hour(...), timestampdiff(...)]
            +- SortMergeJoin [PULocationID], [LocationID], Inner                                     ← join against a 265-row table
               :- Sort [PULocationID]
               :  +- Exchange hashpartitioning(PULocationID, 200)                                    ← shuffle 1: every cleaned trip row
               :     +- Union                                                                        ← one branch per schema group
               :        :- Project / Filter (cleaning rules, pushed down)
               :        :  +- FileScan parquet [7 of 20 columns], PushedFilters: [IsNotNull(...), ...]
               :        +- Project / Filter
               :           +- FileScan parquet [7 of 20 columns]
               +- Sort [LocationID]
                  +- Exchange hashpartitioning(LocationID, 200)                                      ← shuffle of the zone table
                     +- Filter / FileScan csv [LocationID, Borough]
```

What it shows:

- Column pruning and filter pushdown already happen without any flag: the scans read 7 of the 20 columns and push the null checks into the parquet reader.
- The expensive part is **shuffle 1**. Every cleaned trip row (~21 M) is hashed by `PULocationID` and sorted, only to join against 265 rows. This is the main candidate for a broadcast join (see the [baseline](../baseline/README.md)).
- Shuffle 2 is cheap because the partial aggregation shrinks the data to at most 6 boroughs × 7 days × 24 hours per partition.
