#!/usr/bin/env bash
# usage: scripts/chain.sh <wait-for-lane> <new-lane> <playbooks...>  — waits for logs/lane-<wait>.txt LANE_DONE, then runs the new lane
wait_lane=$1; new_lane=$2; shift 2
until grep -q LANE_DONE "logs/lane-$wait_lane.txt" 2>/dev/null; do sleep 30; done
exec scripts/run-lane.sh "$new_lane" "$@"
