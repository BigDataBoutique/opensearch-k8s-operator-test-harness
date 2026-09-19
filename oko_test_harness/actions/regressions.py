"""Regression actions for recent operator issues: quorum-safe cluster-manager removal (#1448), crash-loop recovery
during rolling restarts (#1453), parallel pod management (#1329), node-pool rename drains (#1476), emptyDir readiness
blips (#1455), securityconfig job retries (#1456) and TLS rotation reaching the nodes (#1451).

Everything that changes the cluster runs under the observer; the assertions are what the operator must guarantee."""

import datetime
import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction, parse_duration
from oko_test_harness.actions.cluster import DeployClusterAction, sh
from oko_test_harness.actions.features import _Placeholders
from oko_test_harness.actions.scaling import _ScaleBase, pool_node_names
from oko_test_harness.actions.validation import operator_invariant_violations
from oko_test_harness.models.playbook import ActionResult

MASTER_ROLES = ("cluster_manager", "master")


def voting_config_problems(client, manager_pods: List[str]) -> Tuple[List[str], Dict[str, Any]]:
    """The voting configuration must reference only live nodes, every manager pod must be a live cluster-manager-eligible
    node (and a voter when the pool size is odd), no voting-config exclusion may be left behind, and a cluster manager
    must be elected (#1448)."""
    coord = client.get("/_cluster/state/metadata?filter_path=metadata.cluster_coordination")["metadata"]["cluster_coordination"]
    nodes = client.get("/_nodes/_all/roles")["nodes"]
    live = {nid: n["name"] for nid, n in nodes.items()}
    eligible = {n["name"] for n in nodes.values() if any(r in n["roles"] for r in MASTER_ROLES)}
    committed = coord.get("last_committed_config", [])
    voters = sorted(live[nid] for nid in committed if nid in live)
    problems = []
    departed = [nid for nid in committed if nid not in live]
    if departed:
        problems.append(f"voting configuration still references departed nodes {departed} (live nodes: {sorted(live.values())})")
    missing = sorted(set(manager_pods) - eligible)
    if missing:
        problems.append(f"manager pods not in the cluster: {missing}")
    if manager_pods and len(manager_pods) % 2 == 1:
        non_voters = sorted(set(manager_pods) - set(voters))
        if non_voters:
            problems.append(f"manager pods missing from the voting configuration: {non_voters} (voters: {voters})")
    exclusions = coord.get("voting_config_exclusions", [])
    if exclusions:
        problems.append(f"voting_config_exclusions left behind: {[e.get('node_name') for e in exclusions]}")
    elected = client.get("/_cat/cluster_manager?format=json")[0]["node"]  # 503 when no manager is elected
    return problems, {"voters": voters, "elected": elected}


def green(client) -> bool:
    h = client.health()
    return h["status"] == "green" and not h["relocating_shards"]


def unreachable_for(obs) -> float:
    """Seconds the observer has been unable to reach the cluster API (trailing error samples)."""
    n = 0
    for s in reversed(obs.samples):
        if "error" not in s:
            break
        n += 1
    return n * obs.interval


def events_since(namespace: str, since: float, reason: str) -> List[Dict[str, Any]]:
    out = []
    for e in k8s.get_json("events", "-n", namespace).get("items", []):
        if e.get("reason") != reason:
            continue
        ts = e.get("lastTimestamp") or e.get("eventTime") or ""
        if ts and datetime.datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp() < since:
            continue
        out.append(e)
    return out


class _QuorumWatch(BaseAction):
    """Shared wait loop: fail fast when the API stays unreachable (quorum lost) instead of waiting for a timeout."""

    def wait_available(self, obs, expected_nodes: int, max_unreachable: int, timeout: int, what: str) -> Optional[ActionResult]:
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            down = unreachable_for(obs)
            if down > max_unreachable:
                obs.stop()
                return ActionResult(False, f"cluster API unreachable for {int(down)}s {what}: quorum lost (issue #1448); last error: {obs.samples[-1].get('error')}", obs.summary())
            try:
                self.wait_cluster_running(0)
                with self.os_client() as c:
                    h = c.health()
                if h["number_of_nodes"] == expected_nodes and h["status"] == "green" and not h["relocating_shards"]:
                    return None
                last = f"health {h['status']}, {h['number_of_nodes']}/{expected_nodes} nodes, relocating {h['relocating_shards']}"
            except k8s.StopWaiting:
                raise
            except Exception as e:  # noqa: BLE001
                last = str(e)[:300]
            time.sleep(10)
        obs.stop()
        return ActionResult(False, f"cluster did not settle within {timeout}s {what}: {last}", obs.summary())

    def voting_check(self, result: ActionResult, component: Optional[str]) -> ActionResult:
        pods = [p["name"] for p in self.pods(component) if p["labels"].get(k8s.NODEPOOL_LABEL)]

        def check():
            with self.os_client() as c:
                problems, info = voting_config_problems(c, pods)
            if problems:
                raise Exception("; ".join(problems))
            return info

        try:
            info = k8s.wait_for("voting configuration", check, 120, 10)
        except TimeoutError as e:
            return ActionResult(False, f"{result.message}; {e}", result.data)
        result.message += f"; voting configuration {info['voters']}, elected {info['elected']}, no exclusions left"
        return result


class CheckVotingConfigAction(_QuorumWatch):
    """Assert the voting configuration holds only live nodes, includes every pod of the manager `component`, has no
    exclusions left and that a cluster manager is elected (#1448)."""

    action_name = "check_voting_config"
    params = {"component"}

    def execute(self, params):
        return self.voting_check(ActionResult(True, "Voting configuration checked"), params.get("component"))


class ScaleClusterManagersAction(_QuorumWatch):
    """Scale a cluster-manager pool and fail fast on quorum loss (#1448): the API may never be unreachable for more than
    `max_unreachable` (90s), health never below `min_health`, no member beyond the removed ones may be lost, and afterwards
    the voting configuration must contain exactly the surviving managers with nothing excluded."""

    action_name = "scale_cluster_managers"
    params = {"component", "replicas", "max_unreachable", "min_health", "indices"}

    def execute(self, params):
        cr = self.cr()
        component = params["component"]
        pool = next((p for p in cr["spec"]["nodePools"] if p["component"] == component), None)
        if not pool:
            return ActionResult(False, f"No node pool {component}")
        old, new = pool["replicas"], int(params["replicas"])
        max_unreachable = parse_duration(params.get("max_unreachable", "90s"))
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)
        pool["replicas"] = new
        expected = sum(p["replicas"] for p in cr["spec"]["nodePools"])
        k8s.replace_cr(cr)
        what = f"while scaling managers {component} {old} -> {new}"
        failed = self.wait_available(obs, expected, max_unreachable, self.timeout(self.config.timeouts.scaling), what)
        if failed:
            return failed
        # the operator rolls the remaining managers for the config change after the scale-down, so one extra member may be out at a time
        result = self.finish_observed(obs, f"Scaled managers {component} {old} -> {new}", params.get("min_health", "yellow"), max_unready_pods=None, max_nodes_down=max(old - new, 0) + 1)
        if result.success and obs.max_unreachable_streak > max_unreachable:
            return ActionResult(False, f"{result.message}; but the API was unreachable for {int(obs.max_unreachable_streak)}s {what} (issue #1448)", result.data)
        return self.voting_check(result, component) if result.success else result


class DeletePodAfterBootstrapAction(_QuorumWatch):
    """On a fresh cluster, wait for the operator to remove the bootstrap pod and immediately delete one manager pod of
    `component` (the elected cluster manager when reachable). The cluster must re-elect and stay available (#1448: the
    bootstrap node may still sit in the voting configuration when it is removed)."""

    action_name = "delete_pod_after_bootstrap"
    params = {"component", "max_unreachable", "min_health"}

    def execute(self, params):
        bootstrap = f"{self.cluster}-bootstrap-0"
        timeout = self.timeout(self.config.timeouts.deployment)
        k8s.wait_for("bootstrap pod created", lambda: any(p["name"] == bootstrap for p in self.pods()), timeout, 3)
        k8s.wait_for("bootstrap pod removed by the operator", lambda: not any(p["name"] == bootstrap and not p["deletion"] for p in self.pods()), timeout, 2)
        obs = self.observer()
        pods = [p for p in self.pods(params.get("component")) if p["labels"].get(k8s.NODEPOOL_LABEL)]
        victim = pods[0]
        try:
            with self.os_client() as c:
                elected = c.get("/_cat/cluster_manager?format=json")[0]["node"]
            victim = next((p for p in pods if p["name"] == elected), victim)
        except Exception as e:  # noqa: BLE001
            self.logger.warning(f"could not find the elected manager ({e}); deleting {victim['name']}")
        k8s.delete_pod(self.namespace, victim["name"])
        what = f"after deleting {victim['name']} right after bootstrap-pod removal"
        expected = sum(p["replicas"] for p in self.cr()["spec"]["nodePools"])
        failed = self.wait_available(obs, expected, parse_duration(params.get("max_unreachable", "90s")), self.timeout(self.config.timeouts.recovery), what)
        if failed:
            return failed
        result = self.finish_observed(
            obs, f"Deleted {victim['name']} right after bootstrap-pod removal; cluster available again", params.get("min_health", "yellow"), max_unready_pods=1, max_nodes_down=None
        )
        return self.voting_check(result, params.get("component")) if result.success else result


class RollingRestartAction(BaseAction):
    """Trigger a rolling restart through a spec change (`patch` = merge patch on the CR and/or `node_pool` =
    {component, ...fields}) and watch it: every pod of `expect_pools` (default: all) must be replaced, pods of other pools
    untouched, at most `max_unready_pods` (1) down at a time, health >= `min_health`, no document loss, and afterwards the
    operator RUNNING with allocation settings restored (#1471, #1369, #1450; never completes on one manager: #1449)."""

    action_name = "rolling_restart"
    params = {"patch", "node_pool", "expect_pools", "min_health", "max_unready_pods", "indices"}

    def execute(self, params):
        before = {p["name"]: p for p in self.pods() if p["labels"].get(k8s.NODEPOOL_LABEL)}
        pools = params.get("expect_pools")
        must = {n for n, p in before.items() if not pools or p["labels"][k8s.NODEPOOL_LABEL] in pools}
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)
        if params.get("patch"):
            k8s.kubectl("patch", self.cr_resource(), self.cluster, "-n", self.namespace, "--type", "merge", "-p", json.dumps(params["patch"]))
        if params.get("node_pool"):
            cr, np = self.cr(), dict(params["node_pool"])
            comp = np.pop("component")  # pop outside the generator: popping inside raises KeyError on the second pool
            pool = next((p for p in cr["spec"]["nodePools"] if p["component"] == comp), None)
            if pool is None:
                obs.stop()
                return ActionResult(False, f"No node pool {params['node_pool'].get('component')}")
            pool.update(np)
            k8s.replace_cr(cr)
        timeout = self.timeout(self.config.timeouts.upgrade)
        deadline = time.time() + timeout

        def replaced():
            now = {p["name"]: p["uid"] for p in self.pods()}
            stale = sorted(n for n in must if now.get(n) == before[n]["uid"])
            if stale:
                raise Exception(f"pods not restarted yet: {stale}")
            return True

        try:
            k8s.wait_for(f"every pod of {pools or 'every pool'} replaced", replaced, timeout, 10)
            self.wait_cluster_running(max(60, int(deadline - time.time())))
        except TimeoutError as e:
            obs.stop()
            return ActionResult(False, f"rolling restart did not complete: {e}", obs.summary())
        after = {p["name"]: p["uid"] for p in self.pods()}
        touched = sorted(n for n in before if n not in must and after.get(n) != before[n]["uid"])
        if touched:
            obs.stop()
            return ActionResult(False, f"pods outside {pools} were restarted too: {touched}", obs.summary())
        with self.os_client() as c:
            k8s.wait_for("health green", lambda: green(c), max(60, int(deadline - time.time())), 10)
            violations = operator_invariant_violations(c, self.cr(), self.pods())
        result = self.finish_observed(obs, f"Rolling restart of {sorted(must)} completed", params.get("min_health", "yellow"), params.get("max_unready_pods", 1))
        if result.success and violations:
            return ActionResult(False, f"{result.message}; but operator invariants violated afterwards: {'; '.join(violations)}", result.data)
        return result


class WaitCrashloopRecoveryAction(BaseAction):
    """After a bad spec change put a pod into CrashLoopBackOff and the spec was fixed again, the operator must delete the
    stuck pod itself and finish the rolling restart (#1453): wait until no pod crash-loops or waits on an image, the
    cluster is RUNNING and green, with at most `max_unready_pods` (1) down at any time meanwhile."""

    action_name = "wait_crashloop_recovery"
    params = {"min_health", "max_unready_pods", "indices"}

    def execute(self, params):
        obs = self.observer(params.get("indices"))
        timeout = self.timeout(self.config.timeouts.recovery)
        deadline = time.time() + timeout

        def healthy_pods():
            pods = [p for p in self.pods() if p["labels"].get(k8s.NODEPOOL_LABEL)]
            bad = [p["name"] for p in pods if p["crash_loop"] or p["waiting_reason"]]
            if bad:
                raise Exception(f"pods still stuck: {bad}")
            return True

        try:
            k8s.wait_for("no crash-looping pod", healthy_pods, timeout, 10)
            self.wait_cluster_running(max(60, int(deadline - time.time())))
            with self.os_client() as c:
                k8s.wait_for("health green", lambda: green(c), max(60, int(deadline - time.time())), 10)
        except TimeoutError as e:
            obs.stop()
            return ActionResult(False, f"operator did not recover the crash-looping pod: {e}", obs.summary())
        return self.finish_observed(obs, "Crash-looping pod replaced by the operator, cluster RUNNING and green", params.get("min_health", "yellow"), params.get("max_unready_pods", 1))


class EditNodePoolsAction(_ScaleBase):
    """One read-modify-write of spec.nodePools: `add` pools (deploy_cluster style), `remove` components and `update`
    fields of existing pools ({component, ...}) in a single apply - e.g. a pool rename combined with a change that also
    starts a rolling restart (#1476), or a new pool whose pods must all start at once (#1329). `wait: false` returns right
    after the apply; otherwise the operator must settle under the observer (`min_health`, `max_unready_pods`,
    `max_nodes_down`)."""

    action_name = "edit_node_pools"
    params = {"add", "remove", "update", "wait", "min_health", "max_unready_pods", "max_nodes_down", "indices"}

    def execute(self, params):
        cr = self.cr()
        removed = set(params.get("remove") or [])
        removed_nodes = [n for p in cr["spec"]["nodePools"] if p["component"] in removed for n in pool_node_names(self.cluster, p["component"], 0, p["replicas"])]
        pools = [p for p in cr["spec"]["nodePools"] if p["component"] not in removed]
        for up in params.get("update") or []:
            pool = next((p for p in pools if p["component"] == up["component"]), None)
            if pool is None:
                return ActionResult(False, f"No node pool {up['component']}")
            pool.update({k: v for k, v in up.items() if k != "component"})
        if params.get("add"):
            built = DeployClusterAction(self.config, self.variables)
            built.namespace, built.cluster, built.p = self.namespace, self.cluster, {}
            pools += built.build_cr({"node_pools": params["add"], "version": cr["spec"]["general"]["version"]})["spec"]["nodePools"]
        cr["spec"]["nodePools"] = pools
        msg = f"Node pools edited: added {[p['component'] for p in params.get('add') or []]}, removed {sorted(removed)}, updated {[u['component'] for u in params.get('update') or []]}"
        if not params.get("wait", True):
            k8s.replace_cr(cr)
            return ActionResult(True, msg + " (not waiting)")
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)
        # removed pools go away as a whole and may overlap with a rolling restart of updated pools (see findings N19):
        # judge by health, document counts and total members lost, not by per-sample drops
        return self.apply_and_wait(cr, obs, msg, sum(p["replicas"] for p in pools), params, removed=removed_nodes, step_drop=None)


class PausePodsAction(BaseAction):
    """SIGSTOP the OpenSearch JVM in every pod of `component` (via crictl on the k3d/kind node) for `duration` (3m), then
    SIGCONT: readiness and liveness probes fail cluster-wide while the pods and their emptyDir volumes stay in place. The
    operator must not treat that as data loss (#1455): no EmptyDirRecovery teardown event, same pod UIDs (a container
    restart by the liveness probe is fine), cluster RUNNING and green again, no document loss."""

    action_name = "pause_pods"
    params = {"component", "duration", "indices"}

    def execute(self, params):
        ctx = k8s.current_context()
        if not (ctx.startswith("k3d-") or ctx.startswith("kind-")):
            return ActionResult(False, f"pause_pods needs a k3d or kind cluster (context {ctx})")
        pods = [p for p in self.pods(params.get("component")) if p["labels"].get(k8s.NODEPOOL_LABEL)]
        targets = []
        for p in pods:
            cid = k8s.kubectl("get", "pod", p["name"], "-n", self.namespace, "-o", "jsonpath={.status.containerStatuses[?(@.name=='opensearch')].containerID}").split("//")[-1]
            pid = sh(["docker", "exec", p["node"], "crictl", "inspect", "-o", "go-template", "--template", "{{.info.pid}}", cid]).strip()
            targets.append((p, pid))
        duration = parse_duration(params.get("duration", "3m"))
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)
        started = time.time()
        for p, pid in targets:
            sh(["docker", "exec", p["node"], "kill", "-STOP", pid])
        self.logger.info(f"Paused the JVM in {[p['name'] for p in pods]} for {duration}s")
        try:
            time.sleep(duration)
        finally:
            for p, pid in targets:
                k8s.run(["docker", "exec", p["node"], "kill", "-CONT", pid], check=False)  # the container may have been restarted by the liveness probe
        time.sleep(15)
        timeout = self.timeout(self.config.timeouts.recovery)
        self.wait_cluster_running(timeout)
        with self.os_client() as c:
            k8s.wait_for("health green", lambda: green(c), timeout, 10)
        after = {p["name"]: p["uid"] for p in self.pods()}
        problems = []
        recreated = sorted(p["name"] for p in pods if after.get(p["name"]) != p["uid"])
        if recreated:
            problems.append(f"pods were recreated (StatefulSet torn down): {recreated}")
        teardown = [e.get("message") for e in events_since(self.namespace, started, "EmptyDirRecovery") if "Recreating" in (e.get("message") or "")]
        if teardown:
            problems.append(f"operator recreated the cluster: {teardown[0][:200]}")
        result = self.finish_observed(obs, f"JVM paused {duration}s in {len(pods)} pods; pods kept, cluster green again", min_health="red", max_unready_pods=None)
        if result.success and problems:
            return ActionResult(False, "; ".join(problems), result.data)
        return result


class PatchSecretAction(BaseAction, _Placeholders):
    """Merge `data` (file name -> plain text, placeholders substituted) into an existing Secret, e.g. to break and then
    repair the securityconfig secret (#1456)."""

    action_name = "patch_secret"
    params = {"name", "data"}

    def execute(self, params):
        payload = {"stringData": {k: self.sub(v) for k, v in params["data"].items()}}
        k8s.kubectl("patch", "secret", self.sub(params["name"]), "-n", self.namespace, "--type", "merge", "-p", json.dumps(payload))
        return ActionResult(True, f"Patched secret {params['name']}: {sorted(params['data'])}")


class CheckServedCertAction(BaseAction):
    """Read the HTTP certificate every pod of `component` actually serves on :9200 (curl -v inside the pod) and assert its
    remaining validity lies between `min_days` and `max_days`: renewed certificates must be loaded by the nodes, not only
    written to the Secret (#1451). Polls until `timeout` (5m)."""

    action_name = "check_served_cert"
    params = {"component", "min_days", "max_days"}

    def execute(self, params):
        lo, hi = float(params.get("min_days", 0)), float(params.get("max_days", 10_000))

        def check():
            problems, seen = [], {}
            for p in [p for p in self.pods(params.get("component")) if p["labels"].get(k8s.NODEPOOL_LABEL)]:
                r = k8s.run(
                    ["kubectl", "exec", p["name"], "-n", self.namespace, "-c", "opensearch", "--", "sh", "-c", "curl -sk -v https://localhost:9200/ -o /dev/null 2>&1 | grep -i 'expire date'"],
                    check=False,
                    timeout=60,
                )
                m = re.search(r"expire date:\s*(.+)", r.stdout + r.stderr)
                if not m:
                    problems.append(f"{p['name']}: no expire date in curl output: {(r.stdout + r.stderr)[-200:]}")
                    continue
                expires = datetime.datetime.strptime(m.group(1).strip(), "%b %d %H:%M:%S %Y %Z").replace(tzinfo=datetime.timezone.utc)
                days = (expires - datetime.datetime.now(datetime.timezone.utc)).total_seconds() / 86400
                seen[p["name"]] = round(days, 1)
                if not lo <= days <= hi:
                    problems.append(f"{p['name']}: served certificate valid for {days:.1f} more days (expected {lo}-{hi})")
            if problems:
                raise Exception("; ".join(problems))
            return seen

        seen = k8s.wait_for("served certificate validity", check, self.timeout("5m"), 15)
        return ActionResult(True, f"Served HTTP certificate days remaining: {seen}")


class CreateNamespaceAction(BaseAction):
    """Create (and label) the cluster namespace before deploy_cluster, so namespace-scoped objects such as a LimitRange
    can be applied ahead of the cluster (#1364)."""

    action_name = "create_namespace"
    params = set()

    def execute(self, params):
        k8s.ensure_namespace(self.namespace)
        return ActionResult(True, f"Namespace {self.namespace} ready")
