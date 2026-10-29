"""Run one command with --keep-ui and dump per-stage metrics + task duration quantiles.

Usage: python stages.py <CMD> <out.txt> [extra proj1 args, e.g. --broadcast-join]
"""
import json
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

API = "http://localhost:4040/api/v1/applications"
TIMEOUT_S = 600

cmd, out, extra = sys.argv[1], sys.argv[2], sys.argv[3:]


def get(url):
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def mb(b):
    return f"{b / 2**20:.1f}MB"


# another Spark app on 4040 would push ours to 4041 and we would read the wrong stages
with socket.socket() as sock:
    if sock.connect_ex(("localhost", 4040)) == 0:
        sys.exit("port 4040 is in use (another Spark app with --keep-ui?), close it first")

with tempfile.TemporaryFile("w+") as log:
    proc = subprocess.Popen(
        ["uv", "run", "--package", "proj1", "proj1", cmd, "--sink", "noop", "--keep-ui", "-v", *extra],
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
            print("stage attempt tasks status run_s shuffleR shuffleW memSpill diskSpill"
                  " task_ms[min,p50,p75,max] max/med name", file=f)
            for s in sorted(stages, key=lambda s: s["stageId"]):
                if s["status"] != "COMPLETE":
                    continue
                url = (f"{API}/{app}/stages/{s['stageId']}/{s['attemptId']}"
                       "/taskSummary?quantiles=0,0.5,0.75,1")
                q = get(url)["executorRunTime"]
                ratio = q[3] / q[1] if q[1] else float("nan")
                print(s["stageId"], s["attemptId"], s["numTasks"], s["status"],
                      round(s["executorRunTime"] / 1000, 1),
                      mb(s["shuffleReadBytes"]), mb(s["shuffleWriteBytes"]),
                      mb(s["memoryBytesSpilled"]), mb(s["diskBytesSpilled"]),
                      [int(x) for x in q], f"{ratio:.1f}", s["name"][:60], file=f)
    finally:
        # answer the "Hit Enter to Exit" prompt so Spark shuts down cleanly
        proc.communicate("\n", timeout=60)
