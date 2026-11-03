"""Run one command with --keep-ui and dump per-stage metrics + task duration quantiles.

Usage: python stages.py <CMD> <out.txt> [extra proj1 args, e.g. --broadcast-join]
With `--master docker` among the extra args the command runs in the `jupyter` container of
docker-spark-env (paths in the args must then be container paths, e.g. /repo/proj1/data).
"""
import datetime
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.request

API = "http://localhost:4040/api/v1/applications"
TIMEOUT_S = 600

cmd, out, extra = sys.argv[1], sys.argv[2], sys.argv[3:]

PROJ1_ARGS = [cmd, "--sink", "noop", "--keep-ui", "-v", *extra]
if "docker" in extra and extra[extra.index("docker") - 1] in ("--master", "-m"):
    compose = pathlib.Path(__file__).resolve().parents[3] / "docker-spark-env" / "docker-compose.yml"
    pythonpath = "/repo/proj1/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
    launch = ["docker", "compose", "-f", str(compose), "exec", "-T", "-w", "/repo",
              "-e", f"PYTHONPATH={pythonpath}", "jupyter", "python3", "-m", "proj1", *PROJ1_ARGS]
else:
    launch = ["uv", "run", "--package", "proj1", "proj1", *PROJ1_ARGS]


def get(url):
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def mb(b):
    return f"{b / 2**20:.1f}MB"


def wall_s(stage):
    """Stage duration from submission to completion (times like 2026-11-01T10:13:51.123GMT)."""
    t = [datetime.datetime.strptime(stage[k], "%Y-%m-%dT%H:%M:%S.%f%Z")
         for k in ("submissionTime", "completionTime")]
    return round((t[1] - t[0]).total_seconds(), 1)


# another Spark app on 4040 would push ours to 4041 and we would read the wrong stages;
# probe the API, not the port: docker-spark-env maps 4040 even when no app is running
try:
    get(API)
    sys.exit("a Spark app already answers on port 4040 (another --keep-ui run?), close it first")
except (OSError, ValueError):
    pass

with tempfile.TemporaryFile("w+") as log:
    proc = subprocess.Popen(
        launch,
        stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT, text=True,
    )
    # --keep-ui logs "Done" then waits for Enter with the UI still up
    for _ in range(TIMEOUT_S):
        log.seek(0)
        if "INFO:proj1:Done" in log.read() or proc.poll() is not None:
            break
        time.sleep(1)
    if proc.poll() is not None:
        log.seek(0)
        sys.exit(f"proj1 {cmd} exited early:\n{log.read()[-2000:]}")

    try:
        app = get(API)[0]["id"]
        stages = get(f"{API}/{app}/stages")
        with open(out, "w") as f:
            print("stage attempt tasks status wall_s run_s shuffleR shuffleW memSpill diskSpill"
                  " task_ms[min,p50,p75,max] max/med name", file=f)
            for s in sorted(stages, key=lambda s: s["stageId"]):
                if s["status"] != "COMPLETE":
                    continue
                url = (f"{API}/{app}/stages/{s['stageId']}/{s['attemptId']}"
                       "/taskSummary?quantiles=0,0.5,0.75,1")
                q = get(url)["executorRunTime"]
                ratio = q[3] / q[1] if q[1] else float("nan")
                print(s["stageId"], s["attemptId"], s["numTasks"], s["status"], wall_s(s),
                      round(s["executorRunTime"] / 1000, 1),
                      mb(s["shuffleReadBytes"]), mb(s["shuffleWriteBytes"]),
                      mb(s["memoryBytesSpilled"]), mb(s["diskBytesSpilled"]),
                      [int(x) for x in q], f"{ratio:.1f}", s["name"][:60], file=f)
    finally:
        # answer the "Hit Enter to Exit" prompt so Spark shuts down cleanly
        proc.communicate("\n", timeout=60)
