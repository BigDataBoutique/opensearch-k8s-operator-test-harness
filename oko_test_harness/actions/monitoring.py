"""Diagnostics."""

import os
import select
import sys
import time

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.actions.cluster import OPERATOR_SELECTOR
from oko_test_harness.models.playbook import ActionResult


class CollectLogsAction(BaseAction):
    """Dump operator logs, the CR, pods, events and OpenSearch pod logs to a directory (also run automatically on failure)."""

    action_name = "collect_logs"
    params = {"output_dir", "since"}

    def execute(self, params):
        out = params.get("output_dir", f"./logs/{int(time.time())}")
        since = params.get("since", "2h")
        os.makedirs(out, exist_ok=True)
        ons = self.config.opensearch.operator_namespace
        ns = self.namespace
        dumps = {
            "operator-pods.txt": ["get", "pods", "-n", ons, "-o", "wide"],
            "cluster-cr.yaml": ["get", self.cr_resource(), "-n", ns, "-o", "yaml"],
            "cluster-pods.txt": ["get", "pods,pvc,svc,sts", "-n", ns, "-o", "wide"],
            "cluster-events.txt": ["get", "events", "-n", ns, "--sort-by=.lastTimestamp"],
            "operator-events.txt": ["get", "events", "-n", ons, "--sort-by=.lastTimestamp"],
            "nodes.txt": ["get", "nodes", "-o", "wide"],
        }
        for p in k8s.get_pods(ons, OPERATOR_SELECTOR):
            dumps[f"operator-{p['name']}.log"] = ["logs", p["name"], "-n", ons, f"--since={since}"]
            if p["restarts"]:
                dumps[f"operator-{p['name']}-previous.log"] = ["logs", p["name"], "-n", ons, "--previous"]
        for p in k8s.get_pods(ns):
            dumps[f"pod-{p['name']}.log"] = ["logs", p["name"], "-n", ns, "--all-containers", f"--since={since}"]
            dumps[f"pod-{p['name']}.yaml"] = ["get", "pod", p["name"], "-n", ns, "-o", "yaml"]
        for name, args in dumps.items():
            with open(os.path.join(out, name), "w") as f:
                f.write(k8s.kubectl(*args, check=False))
        return ActionResult(True, f"Collected {len(dumps)} files to {out}")


class DebugPauseAction(BaseAction):
    action_name = "debug_pause"
    params = {"message"}

    def execute(self, params):
        timeout = self.timeout("10m")
        print(f"\n=== DEBUG PAUSE: {params.get('message', '')} (Enter to continue, auto-continue in {timeout}s) ===", flush=True)
        if sys.stdin.isatty():
            select.select([sys.stdin], [], [], timeout)
        else:
            time.sleep(timeout)
        return ActionResult(True, "Resumed")
