"""Validation actions. These assert what the *operator* should have produced."""

import json
import time
from typing import Any, Dict, List

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.actions.cluster import operator_pods
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.opensearch import health_at_least


def operator_invariant_violations(client, cr: Dict[str, Any], pods: List[Dict[str, Any]]) -> List[str]:
    """Things that must be true whenever the operator says it is done with a cluster."""
    v = []
    enable = client.setting("cluster.routing.allocation.enable")
    if enable not in (None, "all"):
        v.append(f"cluster.routing.allocation.enable left at '{enable}' (operator must restore 'all' after rolling operations)")
    excluded = client.setting("cluster.routing.allocation.exclude._name")
    if excluded:
        v.append(f"cluster.routing.allocation.exclude._name left at '{excluded}'")
    status = cr.get("status", {})
    if status.get("phase") != "RUNNING":
        v.append(f"CR phase is {status.get('phase')}")
    if any(p["crash_loop"] for p in pods):
        v.append(f"crash looping pods: {[p['name'] for p in pods if p['crash_loop']]}")
    return v


class WaitForClusterReadyAction(BaseAction):
    """Wait for the operator to report RUNNING, all pods ready, health >= `health`, and operator invariants to hold."""

    action_name = "wait_for_cluster_ready"
    params = {"health", "expect_version", "check_invariants", "max_unassigned"}

    def execute(self, params):
        timeout = self.timeout(self.config.timeouts.deployment)
        deadline = time.time() + timeout
        cr = self.wait_cluster_running(timeout, params.get("expect_version"))
        target = params.get("health", "green")

        expected_nodes = sum(int(p.get("replicas", 0)) for p in self.cr()["spec"].get("nodePools", []))

        def healthy():
            with self.os_client() as c:
                h = c.health()
                # the bootstrap pod stays a cluster member (and often the elected manager) for a while after RUNNING;
                # requests issued while it leaves can hang in the election, so wait for the real membership
                if expected_nodes and h["number_of_nodes"] != expected_nodes:
                    raise Exception(f"{h['number_of_nodes']} cluster members, expected {expected_nodes} (bootstrap pod still a member?)")
                if not health_at_least(h["status"], target):
                    raise Exception(f"health {h['status']} (unassigned={h['unassigned_shards']}, relocating={h['relocating_shards']})")
                if h["relocating_shards"] or h["initializing_shards"]:
                    raise Exception(f"shards still moving: relocating={h['relocating_shards']} initializing={h['initializing_shards']}")
                if h["unassigned_shards"] > int(params.get("max_unassigned", 0)):
                    raise Exception(f"{h['unassigned_shards']} unassigned shards")
                return h

        h = k8s.wait_for(f"cluster health {target}", healthy, max(30, int(deadline - time.time())), 10)
        if params.get("check_invariants", True):
            # give the operator one reconcile loop to clean up after itself
            def invariants_ok():
                with self.os_client() as c:
                    v = operator_invariant_violations(c, self.cr(), self.pods())
                if v:
                    raise Exception("; ".join(v))
                return True

            k8s.wait_for("operator invariants", invariants_ok, min(120, max(60, int(deadline - time.time()))), 10)
        return ActionResult(True, f"Cluster {self.cluster} RUNNING, health {h['status']}, {h['number_of_nodes']} nodes, version {cr.get('status', {}).get('version')}")


class ValidateClusterHealthAction(BaseAction):
    action_name = "validate_cluster_health"
    params = {"expected_status", "expected_nodes"}

    def execute(self, params):
        target = params.get("expected_status", "green")

        def check():
            with self.os_client() as c:
                h = c.health()
            if not health_at_least(h["status"], target):
                raise Exception(f"health is {h['status']}")
            if "expected_nodes" in params and h["number_of_nodes"] != int(params["expected_nodes"]):
                raise Exception(f"{h['number_of_nodes']} nodes, expected {params['expected_nodes']}")
            return h

        h = k8s.wait_for(f"health {target}", check, self.timeout("5m"), 10)
        return ActionResult(True, f"Health {h['status']}, {h['number_of_nodes']} nodes, {h['active_shards']} active shards")


class ValidateClusterVersionAction(BaseAction):
    """All nodes report the expected version via the API, and the CR status agrees."""

    action_name = "validate_cluster_version"
    params = {"expected_version"}

    def execute(self, params):
        expected = str(params["expected_version"])
        with self.os_client() as c:
            versions = {n["name"]: n["version"] for n in c.nodes().values()}
        wrong = {n: v for n, v in versions.items() if v != expected}
        if wrong:
            return ActionResult(False, f"Nodes not on {expected}: {wrong}")
        status_version = self.cr().get("status", {}).get("version")
        if status_version != expected:
            return ActionResult(False, f"All {len(versions)} nodes run {expected} but CR status.version is {status_version}")
        return ActionResult(True, f"All {len(versions)} nodes run {expected}; CR status agrees")


class ValidateClusterConfigurationAction(BaseAction):
    """Compare the live cluster against expectations: node counts per role, plugins, opensearch.yml settings, topology vs CR."""

    action_name = "validate_cluster_configuration"
    params = {"total_nodes", "roles", "plugins", "cluster_settings", "coordinator_nodes"}

    def execute(self, params):
        # the operator may still be rolling pods right after RUNNING (2.x operators expose no in-flight status for the
        # post-bootstrap restart), so the expectations are polled: they must hold, but not on the first sample
        last = [None]

        def check():
            r = self._check(params)
            last[0] = r
            if not r.success:
                raise Exception(r.message)
            return r

        try:
            return k8s.wait_for("cluster configuration", check, self.timeout("3m"), 10)
        except TimeoutError:
            return last[0]

    def _check(self, params):
        errors = []
        with self.os_client() as c:
            nodes = c.nodes()
            plugins = c.plugins()
            if params.get("cluster_settings"):
                current = c.get("/_cluster/settings?include_defaults=true&flat_settings=true")
        info = {n["name"]: n.get("roles", []) for n in nodes.values()}
        # OpenSearch reports 'master' and 'cluster_manager' as the same role depending on version; normalise.
        norm = lambda r: "cluster_manager" if r == "master" else r  # noqa: E731
        role_counts: Dict[str, int] = {}
        for roles in info.values():
            for r in {norm(r) for r in roles}:
                role_counts[r] = role_counts.get(r, 0) + 1
        if "total_nodes" in params and len(info) != int(params["total_nodes"]):
            errors.append(f"{len(info)} nodes, expected {params['total_nodes']}")
        for role, expected in (params.get("roles") or {}).items():
            if role_counts.get(norm(role), 0) != int(expected):
                errors.append(f"role {role}: {role_counts.get(norm(role), 0)} nodes, expected {expected}")
        if "coordinator_nodes" in params:
            coord = [n for n, r in info.items() if not r]
            if len(coord) != int(params["coordinator_nodes"]):
                errors.append(f"{len(coord)} coordinator-only nodes {coord}, expected {params['coordinator_nodes']}")
        for plugin in params.get("plugins") or []:
            missing = [n for n, ps in plugins.items() if plugin not in ps]
            if missing:
                errors.append(f"plugin {plugin} missing on {missing}")
        for key, expected in (params.get("cluster_settings") or {}).items():
            actual = current.get("transient", {}).get(key, current.get("persistent", {}).get(key, current.get("defaults", {}).get(key)))
            if str(actual) != str(expected):
                errors.append(f"setting {key}={actual!r}, expected {expected!r}")
        # topology must match the CR
        cr = self.cr()
        spec_total = sum(p["replicas"] for p in cr["spec"]["nodePools"])
        if spec_total != len(info):
            errors.append(f"CR declares {spec_total} nodes but cluster has {len(info)}")
        if errors:
            return ActionResult(False, "; ".join(errors), {"nodes": info, "role_counts": role_counts})
        return ActionResult(True, f"{len(info)} nodes, roles {role_counts}, plugins ok, settings ok", {"nodes": info})


class ValidateNodeConfigurationAction(BaseAction):
    """Every pod's resources, JVM options, image and PVC size match what the CR asked for."""

    action_name = "validate_node_configuration"
    params = set()

    def execute(self, params):
        cr = self.cr()
        errors = []
        pvcs = {i["metadata"]["name"]: i for i in k8s.get_json("pvc", "-n", self.namespace).get("items", [])}
        version = cr["spec"]["general"]["version"]
        for pool in cr["spec"]["nodePools"]:
            pods = self.pods(pool["component"])
            if len(pods) != pool["replicas"]:
                errors.append(f"pool {pool['component']}: {len(pods)} pods, expected {pool['replicas']}")
            for pod in pods:
                main = next((c for c in pod["containers"] if c["name"] == "opensearch"), pod["containers"][0])
                res = main.get("resources", {})
                want = pool.get("resources", {})
                for kind in ("requests", "limits"):
                    for key, val in want.get(kind, {}).items():
                        if str(res.get(kind, {}).get(key)) != str(val):
                            errors.append(f"{pod['name']}: {kind}.{key}={res.get(kind, {}).get(key)} expected {val}")
                if not pod["image"].endswith(":" + version):
                    errors.append(f"{pod['name']}: image {pod['image']} is not version {version}")
                if pool.get("jvm"):
                    env = {e["name"]: e.get("value") for e in main.get("env", [])}
                    if pool["jvm"] not in (env.get("OPENSEARCH_JAVA_OPTS") or ""):
                        errors.append(f"{pod['name']}: OPENSEARCH_JAVA_OPTS={env.get('OPENSEARCH_JAVA_OPTS')!r} lacks {pool['jvm']!r}")
                pvc = pvcs.get(f"data-{pod['name']}")
                if pvc and pool.get("diskSize") and pvc["spec"]["resources"]["requests"]["storage"] != pool["diskSize"]:
                    errors.append(f"{pod['name']}: PVC size {pvc['spec']['resources']['requests']['storage']} expected {pool['diskSize']}")
                if pool.get("roles") is not None:
                    want_master = any(r in ("master", "cluster_manager") for r in pool["roles"])
                    if want_master != (pod["labels"].get("opensearch.role") in ("master", "cluster_manager")):
                        errors.append(f"{pod['name']}: opensearch.role label {pod['labels'].get('opensearch.role')} vs roles {pool['roles']}")
        if errors:
            return ActionResult(False, "; ".join(errors))
        return ActionResult(True, f"Pod resources, JVM opts, images and PVCs match the CR for {len(cr['spec']['nodePools'])} pool(s)")


class ValidateOperatorStatusAction(BaseAction):
    """Operator pod is running, ready, and has not restarted (crashed) since the baseline."""

    action_name = "validate_operator_status"
    params = {"max_restarts"}

    def execute(self, params):
        ns = self.config.opensearch.operator_namespace
        pods = operator_pods(ns)
        if not pods:
            return ActionResult(False, f"No operator pods in {ns}")
        bad = [p for p in pods if p["phase"] != "Running" or not p["ready"] or p["crash_loop"]]
        if bad:
            return ActionResult(False, f"Operator pods unhealthy: {[(p['name'], p['phase'], p['ready']) for p in bad]}")
        restarts = sum(p["restarts"] for p in pods)
        baseline = self.config.opensearch.operator_restart_baseline
        if baseline is not None and len(pods) == 1:
            restarts = max(0, restarts - baseline)  # restarts during this playbook only (a new pod resets the count anyway)
        if restarts > int(params.get("max_restarts", 0)):
            logs = k8s.kubectl("logs", "-n", ns, pods[0]["name"], "--previous", "--tail=50", check=False)
            return ActionResult(False, f"Operator restarted {restarts} times. Last crash logs:\n{logs[-3000:]}")
        errors = k8s.kubectl("logs", "-n", ns, pods[0]["name"], "--since=30m", check=False)
        panics = []
        for line in errors.splitlines():
            if not ("panic" in line.lower() or "goroutine " in line):
                continue
            try:  # structured line: only count it if it concerns this cluster (or names no cluster at all)
                entry = json.loads(line)
                if entry.get("namespace") not in (None, self.namespace):
                    continue
            except ValueError:
                pass
            panics.append(line[:300])
        if panics:
            return ActionResult(False, f"Operator log contains panics for {self.namespace}: {panics[:3]}")
        return ActionResult(True, f"Operator {pods[0]['image']} running, {restarts} restarts during this run, no panics in log")


class ValidateDashboardsAction(BaseAction):
    """The operator deployed OpenSearch Dashboards for the cluster and it is serving."""

    action_name = "validate_dashboards"
    params = {"replicas"}

    def execute(self, params):
        name = f"{self.cluster}-dashboards"

        def ready():
            d = k8s.get_json("deployment", name, "-n", self.namespace)
            if not d:
                raise Exception(f"deployment {name} not found")
            want = int(params.get("replicas", d["spec"].get("replicas", 1)))
            got = d.get("status", {}).get("readyReplicas", 0)
            if got != want:
                raise Exception(f"{got}/{want} dashboards replicas ready")
            return d

        k8s.wait_for("dashboards ready", ready, self.timeout("10m"), 10)
        pod = k8s.get_pods(self.namespace, f"opensearch.cluster.dashboards={self.cluster}")
        return ActionResult(True, f"Dashboards deployment {name} ready ({len(pod)} pod(s))")
