#!/usr/bin/env bash
# usage: scripts/chain2.sh <laneA> <laneB> <new-lane> <playbooks...> — waits for BOTH lanes to finish, then runs the new lane
a=$1; b=$2; new=$3; shift 3
until grep -q LANE_DONE "logs/lane-$a.txt" 2>/dev/null && grep -q LANE_DONE "logs/lane-$b.txt" 2>/dev/null; do sleep 30; done
exec scripts/run-lane.sh "$new" "$@"
