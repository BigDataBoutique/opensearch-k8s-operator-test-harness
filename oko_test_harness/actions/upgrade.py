"""Upgrade actions: OpenSearch version upgrades driven through the CR, and operator upgrades."""

import json
import time

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.actions.cluster import operator_pods, InstallOperatorAction
from oko_test_harness.models.playbook import ActionResult


class UpgradeClusterAction(BaseAction):
    """Set spec.general.version and watch the operator perform the rolling upgrade.

    While it runs, an observer samples health, pod readiness and per-index document counts every few
    seconds; the step fails if health drops below `min_health`, more than `max_unready_pods` pods are
    down at once, or any index loses documents. `wait_for: stuck` instead waits for the upgrade to get
    stuck on an unpullable image (to test abort/revert handling)."""

    action_name = "upgrade_cluster"
    params = {"target_version", "min_health", "max_unready_pods", "wait_for", "indices", "expect_rolling"}

    def execute(self, params):
        target = str(params["target_version"])
        current = self.cr()["spec"]["general"]["version"]
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)  # baseline sample before the change
        k8s.kubectl("patch", self.cr_resource(), self.cluster, "-n", self.namespace, "--type", "merge", "-p", json.dumps({"spec": {"general": {"version": target}}}))
        timeout = self.timeout(self.config.timeouts.upgrade)
        if params.get("wait_for", "running") == "stuck":

            def stuck():
                bad = [p for p in self.pods() if p["waiting_reason"] in ("ImagePullBackOff", "ErrImagePull", "InvalidImageName")]
                return bad or None

            bad = k8s.wait_for("a pod stuck pulling the target image", stuck, timeout, 10)
            return self.finish_observed(obs, f"Upgrade {current} -> {target} is stuck as expected on {[p['name'] for p in bad]}", params.get("min_health", "yellow"), params.get("max_unready_pods", 1))
        saw_upgrading = False
        t0 = time.time()
        while time.time() - t0 < 120 and not saw_upgrading:
            if self.cr().get("status", {}).get("phase") == "UPGRADING":
                saw_upgrading = True
            time.sleep(5)
        self.wait_cluster_running(timeout, expect_version=target)
        with self.os_client() as c:
            versions = {n["name"]: n["version"] for n in c.nodes().values()}
        wrong = {n: v for n, v in versions.items() if v != target}
        summary_msg = f"Upgraded {current} -> {target} in {int(time.time() - t0)}s" + ("" if saw_upgrading else " (CR never reported UPGRADING phase)")
        if wrong:
            obs.stop()
            return ActionResult(False, f"{summary_msg}, but nodes report other versions: {wrong}")
        result = self.finish_observed(obs, summary_msg, params.get("min_health", "yellow"), params.get("max_unready_pods", 1))
        if result.success and params.get("expect_rolling", True) and current != target and obs.max_unready_pods == 0:
            return ActionResult(False, f"{summary_msg}, but no pod was ever observed restarting: the operator did not roll the pods", result.data)
        return result


class UpgradeOperatorAction(BaseAction):
    """Upgrade the operator (helm) while an OpenSearch cluster exists, then verify the cluster was not disturbed:
    same pod UIDs (no unexpected rolling restart), still RUNNING, no data loss, no operator crash."""

    action_name = "upgrade_operator"
    params = {"version", "values", "values_file", "expect_restart", "legacy_api", "build_ref", "wait_running"}

    def execute(self, params):
        before = {p["name"]: p["uid"] for p in self.pods()}
        obs = self.observer()
        time.sleep(obs.interval)
        install = InstallOperatorAction(self.config, self.variables)
        result = install.run({k: v for k, v in params.items() if k in ("version", "values", "values_file", "legacy_api", "build_ref")})
        if not result.success:
            obs.stop()
            return result
        # the migration controller / new operator may take a moment to adopt the cluster
        k8s.wait_for("new operator pod ready", lambda: all(p["ready"] for p in operator_pods(self.config.opensearch.operator_namespace)), 300, 5)
        if params.get("wait_running", True):
            self.wait_cluster_running(self.timeout(self.config.timeouts.recovery))
            time.sleep(60)  # let a couple of reconcile loops run with the new operator before judging
            self.wait_cluster_running(self.timeout(self.config.timeouts.recovery))
        else:
            time.sleep(60)  # wait_running: false when the cluster is deliberately not RUNNING (e.g. mid-upgrade); judge restarts only
        after = {p["name"]: p["uid"] for p in self.pods()}
        restarted = sorted(n for n in before if after.get(n) != before[n])
        if params.get("expect_restart") == "any":  # e.g. an OpenSearch upgrade was deliberately in flight
            return self.finish_observed(obs, f"{result.message}; cluster pods restarted: {restarted}", max_unready_pods=None)
        if restarted and not params.get("expect_restart", False):
            obs.stop()
            return ActionResult(False, f"Operator upgrade caused an unexpected restart of pods {restarted}")
        if not restarted and params.get("expect_restart", False):
            obs.stop()
            return ActionResult(False, "Operator upgrade was expected to restart pods but none restarted")
        return self.finish_observed(obs, f"{result.message}; cluster pods {'restarted: ' + str(restarted) if restarted else 'untouched'}", max_unready_pods=None if restarted else 0)
