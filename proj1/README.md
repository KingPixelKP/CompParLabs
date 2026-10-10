# Comp Par Project 1

## Setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/).

To fetch any data to be used by the project the python script [fetch](./src/proj1/fetch.py) is provided you can run it with:

```bash
uv run proj1fetch --data-dir proj1/data/
```

It'll fetch any of the `.parquet` and/or `.csv` provided in `links`

## Running

### Local with uv

In dev environments it is much faster and easier to use uv.

Run the main with:

```bash
uv run proj1
```

### Docker

TODO: Check if docker can use uv too
TODO: Update docker to include data file

#### Setup

- Run [boot.sh](../boot.sh)
- Reload VsCode Window
- Select jupyter kernel in top right

> [!NOTE]
> Reminder that master in the notebooks must be changed to `spark://spark-master:7077`

#### Adding pip packages

- Add packages to the [Dockerfile](../docker-spark-env/Dockerfile)
- Run [build.sh](../build.sh)
- Restart the containers
