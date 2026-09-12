#!/usr/bin/env bash
# Run playbooks sequentially in one "lane"; usage: scripts/run-lane.sh <lane-name> <playbook>...
# Each playbook gets logs/run-<name>.{out,log}; lane summary in logs/lane-<lane>.txt
lane=$1; shift
: > "logs/lane-$lane.txt"
for pb in "$@"; do
  name=$(basename "$pb" .yaml)
  start=$(date +%s)
  poetry run oko-test -v --log-file "logs/run-$name.log" run "$pb" > "logs/run-$name.out" 2>&1
  rc=$?
  echo "exit=$rc" >> "logs/run-$name.out"
  echo "$name rc=$rc $(( $(date +%s) - start ))s $(grep -m1 -E 'FAIL ' "logs/run-$name.out" | cut -c1-300)" >> "logs/lane-$lane.txt"
  # LANE_CLEANUP: only for a lane that runs ALONE (the 50-55 migration lane). Those playbooks replace the shared
  # operator and refuse to start while any operator CR exists (N23), so one failure that keeps its namespace
  # (cleanup_on_failure: false) fails every later playbook in the lane in 5s -- 4 of 6 were lost that way on
  # 2026-09-12. Never set it for concurrent lanes: cleanup deletes every oko-test namespace, including the other
  # lane's live cluster. Failure evidence is already written to logs/<pb>-<ts>/ before the run exits.
  [ -n "$LANE_CLEANUP" ] && poetry run oko-test cleanup >> "logs/run-$name.out" 2>&1
done
echo "LANE_DONE" >> "logs/lane-$lane.txt"
