# Example Report

- Date: 2026-10-10

## Findings

### Bottleneck

Not truly a bottleneck more of a presentation "optimization" instead if using zone names whilst calculating routes use the ids directly thus avoiding shuffling large strings.

### Plans

#### Physical

Not much changed only a drop wa removed thus removing a `Project` node.

#### Logical

Same as above.

### Profiling report

As seen by both profiles the amount of data read and written in shuffles in the post fix is reduced by about 4MBs not much, but made a 3 second difference in wall clock time.

## Pre Fix

### Evidence

#### Pipeline

TODO:

#### SQL

##### 0

TODO:

##### 1

TODO:

#### Logs

- [Baseline](./logs/baseline.log)

### Reproducing

**Git Commit Hash:** `[5dcc723d22f695dddfd54d69b839ff03ab5ecc66](https://github.com/KingPixelKP/CompParLabs/commit/5dcc723d22f695dddfd54d69b839ff03ab5ecc66)`
**UV Command:** `uv run proj1 O2 -v`

## Post Fix

TODO:

### Evidence

#### Pipeline

TODO:

#### SQL

##### 0

TODO:

#### Logs

- [Optimized](./logs/baseline_by_id_1.log)

### Reproducing

**Git Commit Hash:** `[ebcbea19253ca93423a0dcd840845c22a0c9d257](https://github.com/KingPixelKP/CompParLabs/commit/ebcbea19253ca93423a0dcd840845c22a0c9d257)`
**UV Command:** `uv run proj1 O2 -v`
