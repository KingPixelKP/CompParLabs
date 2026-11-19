#!/usr/bin/env zsh
# final configuration vs final + --shuffle-partitions 8, local[*], 3 interleaved rounds
set -u
cd ${0:A:h}/../../../..
L=proj1/reports/all/shuffle-partitions/logs
OPT=(--broadcast-join --cache-dataframe --write-once --materialise-clean --coalesce-cache)
waitidle() { sleep 20; while (( $(cut -d' ' -f1 /proc/loadavg | tr , .) > 1.0 )); do sleep 10; done; }
for n in 1 2 3; do
  for cfg in final sp8; do
    extra=(); [[ $cfg == sp8 ]] && extra=(--shuffle-partitions 8)
    waitidle
    uv run --package proj1 proj1 all $OPT $extra --sink noop --explain-plan -v > $L/${cfg}_$n.log 2>&1
    echo "$cfg $n $? $(date +%T) load=$(cut -d' ' -f1 /proc/loadavg)"
  done
done
echo DONE
