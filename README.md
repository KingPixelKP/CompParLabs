# Comp Par Labs

Repository that holds code and docs related do Parallel Computing class labs.

## Local with uv

In dev environments it is much faster and easier to use uv.

### Setup

First we must create the python kernel with uv packages:

```bash
uv run ipython kernel install --user --env VIRTUAL_ENV $(pwd)/.venv --name=comp-par-labs
```

With the kernel created all we must do noww is select it as the kernel to be used in the top right of the notebook (assuming VsCode).

> [!NOTE]
> Reminder that master in the notebooks must be changed to `local[*]`

### Adding uv packages

To add uv packages use:

```bash
uv add <package-name>
```

Or manually introduce it in [pyproject.toml](./pyproject.toml)

After adding the package run the steps described in [Setup](#setup).

## Docker

### Setup

- Run [boot.sh](./boot.sh)
- Reload VsCode Window
- Select jupyter kernel in top right

> [!NOTE]
> Reminder that master in the notebooks must be changed to `spark://spark-master:7077`

### Adding pip packages

- Add packages to the [Dockerfile](./docker-spark-env/Dockerfile)
- Run [build.sh](./build.sh)
- Restart the containers
