# Comp Par Labs

Repository that holds code and docs related do Parallel Computing class labs and projects.

## Running

### Local with uv

In dev environments it is much faster and easier to use uv.

#### Python Script

To run a python script with uv use:

```bash
uv run python <script-name>.py
```

The project [uv.lock](./uv.lock) already has the dependencies needed to run the labs and projects, so you can just run the script without worrying about installing dependencies.

#### Jupyter

##### Setup

Here are the steps to setup a jupyter notebook kernel with uv packages in a virtual environment.
First we must create the python kernel with uv packages:

```bash
uv run ipython kernel install --user --env VIRTUAL_ENV $(pwd)/.venv --name=comp-par-labs
```

With the kernel created all we must do now is select it as the kernel to be used in the top right of the notebook (assuming VsCode).

> [!NOTE]
> Reminder that master in the notebooks must be changed to `local[*]`

#### Adding uv packages

To add uv packages use:

```bash
uv add <package-name>
```

Or manually introduce it in [pyproject.toml](./pyproject.toml)

After adding the package run the steps described in [Setup](#setup).

### Docker

Sometimes it is necessary to run the code in a docker environment, for example when testing cluster environments. In this case we can use the provided docker-compose file to run a spark cluster with jupyter.

> [!WARNING]
> Docker does not use uv, so you must install the dependencies in the docker image, check [Adding pip packages](#adding-pip-packages) for more information.

#### Setup

- Run [boot.sh](./boot.sh)
- Reload VsCode Window
- Select jupyter kernel in top right

> [!NOTE]
> Reminder that master in the notebooks must be changed to `spark://spark-master:7077`

#### Adding pip packages

- Add packages to the [Dockerfile](./docker-spark-env/Dockerfile)
- Run [build.sh](./build.sh)
- Restart the containers

## Projects

### Project 1

The first project is located at [proj1](./proj1/)
