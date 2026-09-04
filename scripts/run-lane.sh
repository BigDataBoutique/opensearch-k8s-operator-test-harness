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
done
echo "LANE_DONE" >> "logs/lane-$lane.txt"
