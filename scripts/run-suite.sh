#!/usr/bin/env bash
# Full release-gate suite: 10-basic-3x alone first (builds/imports the operator image from ../opensearch-k8s-operator HEAD),
# then two lanes in parallel (S1, S2), then 50/51 operator upgrades alone. Results: logs/lane-S0|S1|S2|S3.txt
# usage: nohup scripts/run-suite.sh >/dev/null 2>&1 &
cd "$(dirname "$0")/.."
rm -f logs/lane-S0.txt logs/lane-S1.txt logs/lane-S2.txt logs/lane-S3.txt
p() { for n in "$@"; do ls playbooks/$n-*.yaml; done; }
scripts/run-lane.sh S0 $(p 10)
scripts/run-lane.sh S1 $(p 11 12 20 22 30 40 60 64 65 66 67 68 69 70 72 73 74 80 82 84 86 88) &
scripts/run-lane.sh S2 $(p 21 23 41 75 76 77 78 79 81 83 85 87 89 90 91) &
wait
scripts/run-lane.sh S3 $(p 50 51)
