# Scalability

Source for LaTeX section 5 (Analysis). Runs use the **optimised** pipeline, so this is filled in after the optimisation study.

## Dataset scale

Question: does execution time grow sub-linearly, linearly or worse with the data size, and why?

Runs: `all` with `--sample-fraction 0.1`, `--sample-fraction 0.2` and the full data, 3 runs each, `--sink noop`.

```bash
uv run --package proj1 proj1 all --sample-fraction 0.1 --sink noop -v > proj1/reports/scalability/logs/local_f0.1_<n>.log 2>&1
```

| Environment | Data | Fraction | Records | Median wall time (s) | Shuffle | Spill |
| ----------- | ---- | -------- | ------- | -------------------- | ------- | ----- |
| local       | 2026-01..08 | 0.1 | TODO | TODO | TODO | TODO |
| local       | 2026-01..08 | 0.2 | TODO | TODO | TODO | TODO |
| local       | 2026-01..08 | 1.0 | 29.7 M | TODO | TODO | TODO |
| docker      | 2026-01..08 | 0.1 | TODO | TODO | TODO | TODO |
| DI cluster  | 2016-2020 | 0.1-0.2 | TODO | TODO | TODO | TODO |
| DI cluster  | 2016-2020 | 1.0 | TODO | TODO | TODO | TODO |

Note: `sample` still scans every file, so fixed costs (file listing, footer reads, task scheduling) stay constant across fractions. Separate them from the per-record cost when interpreting the curve.

Chart: execution time vs dataset fraction (required, at least one chart in section 5).

## Cluster size (TODO: DI cluster not available yet)

Runs: optimised `all` on 2 nodes, then on the full allocation. Report speedup S = T₂ / Tₙ and efficiency E = S / (n / 2), and explain deviations from linear speedup (shuffle over the network, skewed stages, driver-side work, too few partitions).

| Nodes | Cores | Median wall time (s) | Speedup | Efficiency |
| ----- | ----- | -------------------- | ------- | ---------- |
| 2     | TODO  | TODO                 | 1.0     | 1.0        |
| TODO  | TODO  | TODO                 | TODO    | TODO       |

Record the cluster topology (workers, cores and memory per worker) and the `spark-submit` command in the project README.
