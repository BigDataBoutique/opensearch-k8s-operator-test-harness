"""Feature-coverage actions for the OpenSearchCluster CRD: generic OpenSearch API and Kubernetes resource
assertions, webhook rejection checks, CR patches, events and deletion semantics.

Kept generic on purpose: a playbook states the expectation (`expect: {status.phase: RUNNING}`,
`contains: [...]`) and the action fails when the operator did not produce it. Cluster-side state (users, templates,
policies, settings) is created straight through the REST API with check_opensearch_api, never through CRDs."""

import json
import time
from typing import Any, Dict, List, Optional

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.actions.cluster import DeployClusterAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.opensearch import OpenSearchClient


def lookup(obj: Any, path: str) -> Any:
    """Dotted path into nested dicts/lists: 'status.conditions.0.type'. Raises KeyError when absent."""
    cur = obj
    if path == "$":
        return cur
    for part in path.split("."):
        if isinstance(cur, list):
            cur = cur[int(part)]
        elif isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            raise KeyError(path)
    return cur


def matches(actual: Any, expected: Any) -> bool:
    """expected dicts are subsets of actual; lists must have a matching element for every expected item; else equality (as str)."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(k in actual and matches(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and all(any(matches(a, e) for a in actual) for e in expected)
    return actual == expected or str(actual) == str(expected)


def mismatches(obj: Any, expect: Optional[Dict[str, Any]]) -> List[str]:
    out = []
    for path, want in (expect or {}).items():
        try:
            got = lookup(obj, path)
        except (KeyError, IndexError, ValueError):
            out.append(f"{path} missing (expected {want!r})")
            continue
        if not matches(got, want):
            out.append(f"{path}={json.dumps(got)[:200]} expected {want!r}")
    return out


class _Placeholders:
    """__CLUSTER__ / __NAMESPACE__ tokens in manifests, names and paths (playbook ${VAR} substitution is env-only,
    and the cluster name is random per run)."""

    def sub(self, text: Optional[str]) -> Optional[str]:
        return text.replace("__CLUSTER__", self.cluster).replace("__NAMESPACE__", self.namespace) if isinstance(text, str) else text

    def fill(self, manifest: Any) -> Any:
        import yaml

        return yaml.safe_load(self.sub(manifest if isinstance(manifest, str) else json.dumps(manifest)))


class ApplyResourceAction(BaseAction, _Placeholders):
    """Apply any raw Kubernetes `manifest` (namespace defaults to the cluster's; __CLUSTER__/__NAMESPACE__ substituted)
    and optionally wait for `wait_condition` (a status.conditions[].type that must be True, e.g. Ready for a
    cert-manager Certificate)."""

    action_name = "apply_resource"
    params = {"manifest", "wait_condition"}

    def execute(self, params):
        obj = self.fill(params["manifest"])
        obj.setdefault("metadata", {}).setdefault("namespace", self.namespace)
        if obj["kind"] != "Namespace":
            k8s.ensure_namespace(obj["metadata"]["namespace"])  # resources are often applied before deploy_cluster creates it
        k8s.apply(json.dumps(obj), obj["metadata"]["namespace"])
        ref = f"{obj['kind']}/{obj['metadata']['name']}"
        condition = params.get("wait_condition")
        if not condition:
            return ActionResult(True, f"Applied {ref}")

        def check():
            cur = k8s.get_json(obj["kind"], obj["metadata"]["name"], "-n", obj["metadata"]["namespace"])
            conds = {c.get("type"): c for c in (cur.get("status") or {}).get("conditions", [])}
            if conds.get(condition, {}).get("status") != "True":
                raise Exception(f"condition {condition}: {conds.get(condition)}")
            return cur

        k8s.wait_for(f"{ref} {condition}", check, self.timeout("5m"), 5)
        return ActionResult(True, f"{ref} applied and {condition}")


class CheckOpenSearchApiAction(BaseAction, _Placeholders):
    """Call the OpenSearch REST API (as admin, or as `user: {username, password}`) until the response matches:
    `status` (HTTP code, default 200), `expect` (dotted path -> value/subset on the JSON body), `contains` and
    `absent` (substrings of the raw body). Polls until `timeout` (default 3m) because the operator reconciles
    asynchronously; the last mismatch is reported on failure."""

    action_name = "check_opensearch_api"
    params = {"path", "method", "body", "status", "expect", "contains", "absent", "user"}

    def execute(self, params):
        method, path = params.get("method", "GET").upper(), self.sub(params["path"])
        s = self.config.opensearch.security
        user = params.get("user") or {}
        client = OpenSearchClient(self.namespace, self.cluster, s.username, s.password, s.use_ssl)
        want_status = int(params.get("status", 200))
        body = self.fill(params["body"]) if params.get("body") is not None else None

        def check():
            r = client.request(method, path, body, ok=tuple(range(100, 600)))
            problems = []
            if r.status_code != want_status:
                problems.append(f"HTTP {r.status_code} (expected {want_status}): {r.text[:300]}")
            else:
                if params.get("expect"):
                    try:
                        problems += mismatches(r.json(), params["expect"])
                    except ValueError:
                        problems.append("response is not JSON")
                problems += [f"body lacks {s!r}" for s in params.get("contains") or [] if s not in r.text]
                problems += [f"body contains {s!r}" for s in params.get("absent") or [] if s in r.text]
            if problems:
                raise Exception("; ".join(problems))
            return {"response": r}  # a bare Response is falsy for non-2xx codes, which wait_for would read as "not met"

        with client:
            # the port-forward probe in connect() needs a working login, so connect as admin and only then act as `user`
            if user:
                client.session.auth = (user.get("username", ""), user.get("password", ""))
            r = k8s.wait_for(f"{method} {path}", check, self.timeout("3m"), 5)["response"]
        who = user.get("username", s.username)
        return ActionResult(True, f"{method} {path} as {who} -> {r.status_code}" + (f", {params['expect']}" if params.get("expect") else ""))


class CheckK8sResourceAction(BaseAction, _Placeholders):
    """Assert on Kubernetes objects: `kind` + `name`, or `kind` + `selector`/`component` (every matching object must
    satisfy `expect`). `exists: false` asserts absence (also true when the resource type itself is not installed).
    Values in `expect` are dotted paths -> value (dict = subset match, list = every item must match some element).
    Polls until `timeout` (default 2m)."""

    action_name = "check_k8s_resource"
    params = {"kind", "name", "selector", "component", "exists", "expect", "min_count"}

    def execute(self, params):
        kind = params["kind"]
        params = {**params, "name": self.sub(params.get("name")), "expect": self.fill(params.get("expect")) if params.get("expect") else None}
        selector = self.sub(params.get("selector")) or (k8s.selector(self.cluster, params.get("component")) if params.get("component") or kind.lower() in ("pod", "pods") else None)
        should_exist = params.get("exists", True)

        def fetch() -> List[Dict]:
            if params.get("name"):
                obj = k8s.get_json(kind, params["name"], "-n", self.namespace)
                return [obj] if obj else []
            args = [kind, "-n", self.namespace] + (["-l", selector] if selector else [])
            return k8s.get_json(*args).get("items", [])

        def check():
            items = fetch()
            if not should_exist:
                if items:
                    raise Exception(f"{len(items)} {kind} object(s) exist: {[i['metadata']['name'] for i in items]}")
                return True
            if len(items) < int(params.get("min_count", 1)):
                raise Exception(f"{len(items)} {kind} object(s) found, need {params.get('min_count', 1)}")
            problems = [f"{i['metadata']['name']}: {m}" for i in items for m in mismatches(i, params.get("expect"))]
            if problems:
                raise Exception("; ".join(problems)[:1500])
            return items

        items = k8s.wait_for(f"{kind} {params.get('name') or selector or ''}", check, self.timeout("2m"), 5)
        if not should_exist:
            return ActionResult(True, f"No {kind} {params.get('name') or selector or ''} in {self.namespace}, as expected")
        return ActionResult(True, f"{len(items)} {kind} object(s) match {params.get('expect') or 'existence'}")


class CheckPodExecAction(BaseAction, _Placeholders):
    """Run `command` inside the opensearch container of one (or every) pod of `component` (or any `selector`) and
    assert on the output."""

    action_name = "check_pod_exec"
    params = {"component", "selector", "command", "contains", "absent", "all_pods", "container"}

    def execute(self, params):
        pods = k8s.get_pods(self.namespace, self.sub(params["selector"])) if params.get("selector") else self.pods(params.get("component"))
        pods = [p for p in pods if p["ready"] and not p["deletion"]]  # pods of a superseded ReplicaSet may be terminating
        if not pods:
            return ActionResult(False, f"No pods for {params.get('selector') or params.get('component')}")
        targets = pods if params.get("all_pods") else pods[:1]
        cmd = self.fill(params["command"]) if isinstance(params["command"], list) else ["sh", "-c", self.sub(params["command"])]
        container = params.get("container", "opensearch")
        problems = []
        for p in targets:
            r = k8s.run(["kubectl", "exec", p["name"], "-n", self.namespace, "-c", container, "--", *cmd], check=False, timeout=120)
            out = r.stdout + r.stderr
            if r.returncode != 0:
                problems.append(f"{p['name']}: rc={r.returncode} {out[-300:]}")
                continue
            problems += [f"{p['name']}: output lacks {s!r}" for s in map(self.sub, params.get("contains") or []) if s not in out]
            problems += [f"{p['name']}: output contains {s!r}" for s in map(self.sub, params.get("absent") or []) if s in out]
        if problems:
            return ActionResult(False, "; ".join(problems))
        return ActionResult(True, f"{' '.join(cmd)[:80]} ok on {[p['name'] for p in targets]}")


class ExpectEventAction(BaseAction):
    """Wait for a Kubernetes event in the cluster namespace with `reason` and (optionally) a `contains` substring
    in its message; `type` restricts to Warning/Normal."""

    action_name = "expect_event"
    params = {"reason", "contains", "type", "since"}

    def execute(self, params):
        since = time.time() - self.timeout(params.get("since", "10m")) if params.get("since") else 0

        def find():
            import datetime

            for e in k8s.get_json("events", "-n", self.namespace).get("items", []):
                if params.get("reason") and e.get("reason") != params["reason"]:
                    continue
                if params.get("type") and e.get("type") != params["type"]:
                    continue
                if params.get("contains") and params["contains"] not in (e.get("message") or ""):
                    continue
                ts = e.get("lastTimestamp") or e.get("eventTime") or (e.get("series") or {}).get("lastObservedTime") or ""
                if since and ts:
                    when = datetime.datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
                    if when < since:
                        continue
                return e
            raise Exception(f"no event reason={params.get('reason')} type={params.get('type')} containing {params.get('contains')!r}")

        e = k8s.wait_for("event", find, self.timeout("5m"), 5)
        return ActionResult(True, f"Event {e.get('type')}/{e.get('reason')}: {(e.get('message') or '')[:200]}")


class PatchClusterAction(BaseAction):
    """Change the OpenSearchCluster spec without waiting for the operator (`patch` = JSON merge patch on the CR,
    `node_pool` = {component, ...fields} merged into that pool via read-modify-write, `remove` = list of dotted
    spec paths to drop). `wait_phase` optionally waits for status.phase (e.g. UPGRADING); `expect_restart: true`
    requires every pod to be replaced (a rolling restart really happened); `wait_running: true` waits for the operator
    to settle again (rolling restarts included)."""

    action_name = "patch_cluster"
    params = {"patch", "node_pool", "remove", "wait_phase", "wait_running", "expect_restart"}

    def execute(self, params):
        before = {p["name"]: p["uid"] for p in self.pods() if p["labels"].get(k8s.NODEPOOL_LABEL)}
        if params.get("patch"):
            k8s.kubectl("patch", self.cr_resource(), self.cluster, "-n", self.namespace, "--type", "merge", "-p", json.dumps(params["patch"]))
        if params.get("node_pool") or params.get("remove"):
            cr = self.cr()
            if params.get("node_pool"):
                np = dict(params["node_pool"])
                pool = next((p for p in cr["spec"]["nodePools"] if p["component"] == np.pop("component")), None)
                if pool is None:
                    return ActionResult(False, f"No node pool {params['node_pool'].get('component')}")
                pool.update(np)
            for path in params.get("remove") or []:
                parent, _, key = path.rpartition(".")
                lookup(cr["spec"], parent).pop(key, None) if parent else cr["spec"].pop(key, None)
            k8s.replace_cr(cr)
        msg = f"Patched {self.cluster}: {params.get('patch') or ''} {params.get('node_pool') or ''} {('removed ' + str(params['remove'])) if params.get('remove') else ''}".strip()
        if params.get("wait_phase"):
            k8s.wait_for(f"phase {params['wait_phase']}", lambda: self.cr().get("status", {}).get("phase") == params["wait_phase"], self.timeout("5m"), 3)
            msg += f"; phase {params['wait_phase']} reached"
        if params.get("expect_restart"):

            def all_replaced():
                now = {p["name"]: p["uid"] for p in self.pods() if p["labels"].get(k8s.NODEPOOL_LABEL)}
                stale = [n for n, uid in before.items() if now.get(n) == uid]
                if stale:
                    raise Exception(f"pods not restarted yet: {stale}")
                return True

            k8s.wait_for("every pod replaced", all_replaced, self.timeout(self.config.timeouts.upgrade), 10)
            msg += f"; all {len(before)} pods were replaced"
        if params.get("wait_running"):
            time.sleep(20)  # give the operator a reconcile loop to pick the change up before judging
            self.wait_cluster_running(self.timeout(self.config.timeouts.upgrade))
            msg += "; cluster RUNNING again"
        return ActionResult(True, msg)


class ExpectRejectedAction(BaseAction, _Placeholders):
    """Assert the API server (validating webhook) denies a change: `patch` (merge patch on the cluster CR),
    `node_pool` (read-modify-write of one pool), `cluster_params` (a deploy_cluster-style spec for a NEW cluster
    named `name`, optionally under another `api_version`) or a raw `manifest`. `message` must appear in the denial.
    For patches, also proves the CR spec was not changed."""

    action_name = "expect_rejected"
    params = {"patch", "node_pool", "cluster_params", "manifest", "message", "name", "api_version"}

    def execute(self, params):
        message = self.sub(params["message"])
        before = self.cr() if not (params.get("cluster_params") or params.get("manifest")) else None
        if params.get("patch"):
            r = k8s.run(["kubectl", "patch", self.cr_resource(), self.cluster, "-n", self.namespace, "--type", "merge", "-p", json.dumps(params["patch"])], check=False)
            what = f"patch {params['patch']}"
        elif params.get("node_pool"):
            cr, np = self.cr(), dict(params["node_pool"])
            comp = np.pop("component")
            pool = next((p for p in cr["spec"]["nodePools"] if p["component"] == comp), None)
            if pool is None:
                cr["spec"]["nodePools"].append({"component": comp, **np})
            else:
                pool.update(np)
            for k in ("managedFields", "resourceVersion", "uid", "creationTimestamp", "generation"):
                cr["metadata"].pop(k, None)
            cr.pop("status", None)
            r = k8s.run(["kubectl", "apply", "-f", "-"], input=json.dumps(cr), check=False)
            what = f"node pool change {params['node_pool']}"
        else:
            if params.get("cluster_params"):
                builder = DeployClusterAction(self.config, self.variables)
                builder.namespace, builder.cluster, builder.p = self.namespace, params.get("name", f"{self.cluster}-bad"), {}
                obj = builder.build_cr(self.fill(params["cluster_params"]))
                if params.get("api_version"):
                    obj["apiVersion"] = params["api_version"]
            else:
                obj = self.fill(params["manifest"])
                obj.setdefault("metadata", {}).setdefault("namespace", self.namespace)
            r = k8s.run(["kubectl", "apply", "-f", "-"], input=json.dumps(obj), check=False)
            what = f"create {obj['apiVersion']} {obj['kind']}/{obj['metadata']['name']}"
        err = (r.stderr or r.stdout).strip()
        if r.returncode == 0:
            # clean up anything that slipped through so the namespace stays consistent
            if before is None and "obj" in locals():
                k8s.kubectl("delete", "-f", "-", input=json.dumps(obj), check=False)
            return ActionResult(False, f"{what} was ACCEPTED but should have been rejected with {message!r}")
        if message not in err:
            return ActionResult(False, f"{what} was rejected, but not with {message!r}: {err[-500:]}")
        if before is not None:
            after = self.cr()
            if after["spec"] != before["spec"]:
                return ActionResult(False, f"{what} rejected but the CR spec changed anyway")
        return ActionResult(True, f"{what} rejected as expected: {err[-300:]}")


class DeleteNamespaceAction(BaseAction):
    """Delete the whole namespace (cluster CR included) and require it to disappear: operator finalizers must not
    block namespace deletion."""

    action_name = "delete_namespace"
    params = set()

    def execute(self, params):
        k8s.kubectl("delete", "ns", self.namespace, "--ignore-not-found", "--wait=false")
        timeout = self.timeout("10m")
        try:
            k8s.wait_for("namespace gone", lambda: k8s.run(["kubectl", "get", "ns", self.namespace], check=False).returncode != 0, timeout, 5)
        except TimeoutError:
            cr = k8s.get_cr(self.config.opensearch.api_group, self.cluster, self.namespace)
            return ActionResult(False, f"namespace {self.namespace} still exists after {timeout}s; CR finalizers={cr.get('metadata', {}).get('finalizers') if cr else 'CR gone'}")
        return ActionResult(True, f"Namespace {self.namespace} deleted within {timeout}s")


class DeleteClusterKeepPvcsAction(BaseAction):
    """Delete only the OpenSearchCluster CR and report what happens to its PVCs (the operator retains them by
    default; `expect_retained: false` flips the assertion). Teardown's delete_cluster removes leftovers later."""

    action_name = "delete_cluster_check_pvcs"
    params = {"expect_retained"}

    def execute(self, params):
        pvcs_before = sorted(i["metadata"]["name"] for i in k8s.get_json("pvc", "-n", self.namespace).get("items", []))
        k8s.kubectl("delete", self.cr_resource(), self.cluster, "-n", self.namespace, "--ignore-not-found", "--wait=false")
        timeout = self.timeout("10m")
        k8s.wait_for("cluster resource deletion", lambda: not k8s.get_cr(self.config.opensearch.api_group, self.cluster, self.namespace), timeout, 5)
        k8s.wait_for("cluster pods deletion", lambda: not self.pods(), timeout, 5)
        time.sleep(15)
        pvcs_after = sorted(i["metadata"]["name"] for i in k8s.get_json("pvc", "-n", self.namespace).get("items", []))
        retained = params.get("expect_retained", True)
        if retained and pvcs_after != pvcs_before:
            return ActionResult(False, f"PVCs changed after cluster deletion: before {pvcs_before}, after {pvcs_after} (operator should retain them)")
        if not retained and pvcs_after:
            return ActionResult(False, f"PVCs still present after cluster deletion: {pvcs_after}")
        return ActionResult(True, f"Cluster deleted; PVCs {'retained' if retained else 'removed'}: {pvcs_after or pvcs_before}")


class WaitDashboardsVersionAction(BaseAction):
    """Watch the Dashboards deployment roll to `version` and assert at most `max_unavailable` pods were unavailable
    at any sample (a rolling update, not a recreate)."""

    action_name = "wait_dashboards_version"
    params = {"version", "max_unavailable"}

    def execute(self, params):
        name, version = f"{self.cluster}-dashboards", str(params["version"])
        worst = 0
        deadline = time.time() + self.timeout("10m")
        while time.time() < deadline:
            d = k8s.get_json("deployment", name, "-n", self.namespace)
            if not d:
                return ActionResult(False, f"deployment {name} not found")
            st, want = d.get("status", {}), d["spec"].get("replicas", 1)
            worst = max(worst, want - st.get("availableReplicas", 0))
            image = d["spec"]["template"]["spec"]["containers"][0]["image"]
            if image.endswith(":" + version) and st.get("updatedReplicas", 0) == want and st.get("availableReplicas", 0) == want and st.get("replicas", 0) == want:
                pods = k8s.get_pods(self.namespace, f"opensearch.cluster.dashboards={self.cluster}")
                wrong = [p["name"] for p in pods if not p["image"].endswith(":" + version)]
                if not wrong and all(p["ready"] for p in pods):
                    if worst > int(params.get("max_unavailable", 1)):
                        return ActionResult(False, f"Dashboards on {version}, but {worst} pod(s) were unavailable at once (max {params.get('max_unavailable', 1)})")
                    return ActionResult(True, f"Dashboards rolled to {version}; max unavailable during rollout: {worst}")
            time.sleep(3)
        return ActionResult(False, f"Dashboards deployment did not reach {version}: image={image} status={st}")
