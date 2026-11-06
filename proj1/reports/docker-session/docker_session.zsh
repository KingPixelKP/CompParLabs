#!/usr/bin/env zsh
# One Docker session for every cluster number in the report.
set -u
REPO=${0:A:h}/../../..  # repo root (script lives in proj1/reports/docker-session)
cd $REPO
L=proj1/reports/docker-session/logs
mkdir -p $L
DC=(docker compose -f docker-spark-env/docker-compose.yml)
OPT=(--broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache)
C=(--master docker --output-dir /repo/out/docker)
EXEC=($DC exec -T -w /repo -e PYTHONPATH=/repo/proj1/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip jupyter)

waitidle() { sleep 20; while (( $(cut -d' ' -f1 /proc/loadavg | tr , .) > 1.0 )); do sleep 10; done; }

alive() { curl -s localhost:8080/json/ | python3 -c 'import json,sys; print(json.load(sys.stdin)["aliveworkers"])' 2>/dev/null || echo 0; }

scale() {
  $DC up -d --scale spark-worker=$1 > /dev/null 2>&1
  for i in {1..60}; do [[ $(alive) == $1 ]] && { echo "workers=$1 $(date +%T)"; return 0; }; sleep 5; done
  echo "workers=$1 NOT READY (alive=$(alive))"; return 1
}

run() { # name, data-dir, extra args...
  local name=$1 dir=$2; shift 2
  waitidle
  $EXEC python3 -m proj1 all $C --data-dir $dir "$@" --sink noop -v > $L/$name.log 2>&1
  echo "$name $? $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
}

scale 3 || exit 1
$EXEC python3 proj1/reports/tools/month_dirs.py /repo/proj1/data /repo/out/docker/months

# 1. Table 4 and dataset scale, 3 workers
for n in 1 2 3; do
  run w3_baseline_$n /repo/proj1/data --explain-plan
  run w3_opt_$n /repo/proj1/data $OPT --explain-plan
  for f in 0.1 0.2 0.5; do run w3_s${f}_$n /repo/proj1/data $OPT --sample-fraction $f; done
  for k in 1 2 4; do run w3_m${k}_$n /repo/out/docker/months/data_m$k $OPT; done
done

# 2. per-stage durations
for cfg in baseline optimised; do
  waitidle
  extra=(); [[ $cfg == optimised ]] && extra=($OPT)
  uv run python proj1/reports/tools/stages.py all $L/stages_$cfg.txt --master docker --data-dir /repo/proj1/data --output-dir /repo/out/docker $extra > $L/stages_${cfg}_run.log 2>&1
  echo "stages_$cfg $? $(date +%T)"
done

# 3. cluster size, interleaved
for n in 1 2 3; do
  for w in 1 2 3; do
    scale $w || continue
    run size_w${w}_$n /repo/proj1/data $OPT
  done
done
echo DONE $(date +%T)
