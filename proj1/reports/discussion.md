# Discussion

Source for LaTeX section 6. Fill in as the optimisation study progresses.

## Remaining bottlenecks

Backlog rows still `confirmed` but not `done` when the study ends (see [backlog](./README.md#optimisation-backlog)), with the evidence and why they were not addressed.

- TODO

## Limitations of the methodology

- Local baselines run on a single laptop in `local[*]` mode, so shuffles never cross the network. Shuffle cost is underestimated compared to the cluster.
- Wall time on a laptop is noisy (thermal throttling, background load). Mitigated with the median of 3 runs.
- Development data is 2026-01..08 (29.7 M records), not the 2016-2020 dataset (~500 M records). Bottlenecks that only appear at scale (e.g. spills, skew) may be under-represented.
- Spark's input metric (profiler and Spark UI alike) reports 6-19 MiB for scans that read ~383 MiB of parquet column chunks, so input volume comes from the parquet footers, not from Spark.
- A small number of records have pickup years outside the data period (2001, 2008, 2009, 2025). The assignment's cleaning rules keep them, so they appear as extra months in M3, O2 and O4.

## With more time

- TODO
