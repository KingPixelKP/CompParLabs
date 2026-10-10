# Reports

This directory is used to document findings and report any bottlenecks explain why the bottlenecks happen and how they can be improved from.

## Report Structure

Any report made on any command and/or operation should be located under a directory with the commands name and a directory with either an appropriate name for that report or date in which the report was made.

> [!NOTE]
> The report should be done on a `README.md` file.

A report should include the following

### Mandatory

- Date of report
- Findings section including
  - Bottleneck found
  - An association of the bottleneck and the plans
- Evidence Section
  - A screenshot of the SparkWebUI pipeline timeline
  - A screenshot of the any SQL query from SparkWebUI
  - A .log file of the run which must contain
    - `explain("format")` - **Physical Plan**
    - `explain(True)` - **Logical Plan**
- Reproducing Section
  - If possible a `git` commit hash or a python snippet showing the relevant piece of code
  - The `uv` command used to run the example

The **Evidence** and **Reproducing** sections should be made once for the pre and post fix og the bottleneck.

### Optional

- Parquet dataset used (files names or url)

## Example

An example of a report can be found under [Example Report](./example/README.md).

## M1

## M2

## M3

## OX1

## OX2
