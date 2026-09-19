"""Kubernetes cluster, operator and OpenSearch cluster lifecycle actions."""

import base64
import hashlib
import json
import os
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional

import bcrypt
from loguru import logger

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult

OPERATOR_SELECTOR = "app.kubernetes.io/name=opensearch-operator"
LEGACY_OPERATOR_SELECTOR = "control-plane=controller-manager"  # charts <= 2.8.x label the pod this way only


def operator_pods(ns: str):
    """Operator pods for the current (3.x) chart labels, falling back to the 2.x chart labels."""
    return k8s.get_pods(ns, OPERATOR_SELECTOR) or k8s.get_pods(ns, LEGACY_OPERATOR_SELECTOR)


def operator_deployment(release: str, ns: str) -> str:
    """'opensearch-operator' (3.x chart) or 'opensearch-operator-controller-manager' (2.x chart)."""
    names = k8s.kubectl("get", "deploy", "-n", ns, "-o", "jsonpath={.items[*].metadata.name}").split()
    return next((n for n in names if n == release), None) or next((n for n in names if n.startswith(release)), release)


def sh(cmd: List[str], timeout: int = 1800, cwd=None) -> str:
    logger.debug("$ " + " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    if r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:3])} failed: {(r.stderr or r.stdout).strip()[-2000:]}")
    return r.stdout


class SetupClusterAction(BaseAction):
    """Create (or reuse) the Kubernetes cluster: k3d, kind or an existing context."""

    action_name = "setup_cluster"
    params = {"provider", "nodes", "kubernetes_version"}

    def execute(self, params):
        k = self.config.kubernetes
        provider = params.get("provider", k.provider)
        nodes = int(params.get("nodes", k.nodes))
        version = params.get("kubernetes_version", k.kubernetes_version)
        if provider == "existing":
            return ActionResult(True, f"Using existing cluster (context {k8s.current_context()})")
        if provider == "k3d":
            if any(c["name"] == k.cluster_name for c in json.loads(sh(["k3d", "cluster", "list", "-o", "json"]))):
                sh(["kubectl", "config", "use-context", f"k3d-{k.cluster_name}"])
                return ActionResult(True, f"k3d cluster {k.cluster_name} already exists")
            cmd = ["k3d", "cluster", "create", k.cluster_name, "--agents", str(nodes), "--wait", "--timeout", "5m"]
            # k3d nodes share the host disk: on a busy laptop the kubelet defaults (image GC at 85%, eviction at 5% free)
            # continuously garbage-collect pre-pulled images and can evict OpenSearch pods; relax them for a test cluster
            for arg in ("image-gc-high-threshold=97", "image-gc-low-threshold=95", "eviction-hard=nodefs.available<2%,imagefs.available<2%"):
                cmd += ["--k3s-arg", f"--kubelet-arg={arg}@agent:*", "--k3s-arg", f"--kubelet-arg={arg}@server:*"]
            # keep workloads off the control plane: a saturated server node made containerd miss deadlines and took the API server down with it
            cmd += ["--k3s-arg", "--node-taint=CriticalAddonsOnly=true:NoExecute@server:*"]
            if version:
                cmd += ["--image", f"rancher/k3s:{version}-k3s1"]
            sh(cmd)
        elif provider == "kind":
            if k.cluster_name in sh(["kind", "get", "clusters"]).split():
                sh(["kubectl", "config", "use-context", f"kind-{k.cluster_name}"])
                return ActionResult(True, f"kind cluster {k.cluster_name} already exists")
            image = f"kindest/node:{version}" if version else None
            cfg = "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n"
            for role in ["control-plane"] + ["worker"] * nodes:
                cfg += f"- role: {role}\n" + (f"  image: {image}\n" if image else "")
            subprocess.run(["kind", "create", "cluster", "--name", k.cluster_name, "--config", "-", "--wait", "5m"], input=cfg, text=True, check=True, capture_output=True)
        else:
            return ActionResult(False, f"Unsupported provider {provider}; use k3d, kind or existing")
        k8s.wait_for("nodes Ready", lambda: "NotReady" not in k8s.kubectl("get", "nodes") and "Ready" in k8s.kubectl("get", "nodes"), 300, 5)
        return ActionResult(True, f"Created {provider} cluster {k.cluster_name} with {nodes} worker nodes (context {k8s.current_context()})")


class InstallOperatorAction(BaseAction):
    """Install or upgrade the operator with Helm. version 'local' builds the image from local_operator_path."""

    action_name = "install_operator"
    params = {"version", "values", "values_file", "cert_manager", "legacy_api", "build_ref"}

    def execute(self, params):
        o = self.config.opensearch
        version = str(params.get("version", o.operator_version))
        ns = o.operator_namespace
        if params.get("cert_manager", True):
            ensure_cert_manager()
        # --reset-values: helm reuses the previous release's user values when none are given, which kept the local dev image on a "released" chart
        common = ["-n", ns, "--create-namespace", "--reset-values", "--wait", "--timeout", self.p.get("timeout", "10m")]
        image = None
        if version == "local":
            go_dir, chart = operator_dirs(o.local_operator_path)
            image = local_operator_image(go_dir)
            running = [p for p in operator_pods(ns) if p["ready"]]
            # the image alone is not proof: a released chart installed by 50-operator-upgrade can still run the dev image
            same_chart = installed_chart(o.operator_release, ns) == local_chart_name_version(chart)
            if running and same_chart and all(p["image"] == image for p in running) and not params.get("values") and not params.get("values_file") and "legacy_api" not in params:
                # kubelet image GC on a full host removes unused-looking images; re-import so a restarted operator pod can start
                import_image_into_cluster(image)
                o.operator_restart_baseline = sum(p["restarts"] for p in running)
                return ActionResult(True, f"Operator already running {image} in {ns}", {"image": image})
            build_local_operator(go_dir, image)
            repo, tag = image.rsplit(":", 1)
            common.remove("--wait")  # the rollout is watched below so a GC'd image can be re-imported instead of timing out
            cmd = [
                "helm",
                "upgrade",
                "--install",
                o.operator_release,
                chart,
                *common,
                "--set",
                f"manager.image.repository={repo}",
                "--set",
                f"manager.image.tag={tag}",
                "--set",
                "manager.image.pullPolicy=IfNotPresent",
            ]
        else:
            sh(["helm", "repo", "add", "opensearch-operator", o.helm_repo_url, "--force-update"])
            sh(["helm", "repo", "update", "opensearch-operator"])
            if installed_chart(o.operator_release, ns):
                # uninstall deletes the CRDs; a leftover CR with the operator's finalizer would deadlock that (operator gone first)
                leftovers = k8s.operator_crs()  # child CRs (users, roles, ...) carry finalizers too and deadlock the CRD deletion just the same
                if leftovers:
                    raise RuntimeError(f"cannot replace the operator while operator CRs exist: {leftovers}; this playbook must run alone (poetry run oko-test cleanup)")
                # a released chart is the *starting point* of an upgrade scenario, so it is installed from scratch: helm's
                # three-way merge from a different (or failed) release leaves resources such as the renamed Deployment untouched.
                # Note: the operator chart templates its CRDs, so this deletes every OpenSearchCluster in the k8s cluster (N23).
                logger.warning(f"Uninstalling existing release {o.operator_release} before installing chart {version}")
                sh(["helm", "uninstall", o.operator_release, "-n", ns, "--wait", "--timeout", "5m"])
            cmd = ["helm", "upgrade", "--install", o.operator_release, "opensearch-operator/opensearch-operator", "--version", version, *common]
            if params.get("build_ref"):
                # released operator images older than 2.5.0 no longer exist in any registry (opsterio ECR repo is gone), so an
                # old operator is built from its git tag in the local checkout and run under the published chart of that version
                go_dir, _ = operator_dirs(o.local_operator_path)
                image = build_operator_ref(go_dir, str(params["build_ref"]))
                repo, tag = image.rsplit(":", 1)
                cmd += ["--set", f"manager.image.repository={repo}", "--set", f"manager.image.tag={tag}", "--set", "manager.image.pullPolicy=IfNotPresent"]
        if "legacy_api" in params:
            cmd += ["--set", f"legacyAPI.enabled={'true' if params['legacy_api'] else 'false'}"]
        for key, val in (params.get("values") or {}).items():
            cmd += ["--set", f"{key}={val}"]
        if params.get("values_file"):
            cmd += ["-f", params["values_file"]]
        for attempt in range(6):  # playbooks may run concurrently against one operator; helm allows one operation at a time
            try:
                sh(cmd, timeout=900)
                break
            except RuntimeError as e:
                if "another operation" in str(e) and attempt < 5:
                    logger.info("helm release busy, retrying in 20s")
                    time.sleep(20)
                    continue
                raise
        wait_operator_rollout(o.operator_release, ns, image)
        k8s.kubectl("wait", "--for=condition=established", "--timeout=60s", f"crd/{self.cr_resource()}")
        pods = operator_pods(ns)
        o.operator_restart_baseline = sum(p["restarts"] for p in pods if p["ready"])
        return ActionResult(True, f"Operator {version} installed in {ns} ({[p['image'] for p in pods]})", {"image": pods[0]["image"] if pods else None})


def refuse_if_foreign_clusters(namespace: str, what: str) -> None:
    """Fail fast before restarting the shared operator while another playbook's cluster exists: the restart interrupts that
    cluster's reconcile mid-operation and loses the operator log it is asserting on (2026-09-18, playbook 76 under 31).
    The runner puts every playbook with such a step in the solo group; this is the backstop for manual/lane runs."""
    others = k8s.foreign_clusters(namespace)
    if others:
        raise RuntimeError(f"cannot {what} while OpenSearchCluster objects exist outside {namespace}: {others}; the operator Deployment is shared, so this playbook must run alone")


def wait_operator_rollout(release: str, ns: str, local_image: Optional[str], timeout: int = 600) -> None:
    """kubectl rollout status, but a locally imported image that kubelet image GC removed meanwhile is re-imported and the pod recreated."""
    deadline = time.time() + timeout
    while True:
        try:
            k8s.kubectl("rollout", "status", f"deployment/{operator_deployment(release, ns)}", "-n", ns, "--timeout=30s")
            return
        except k8s.KubectlError as e:
            if time.time() > deadline:
                raise RuntimeError(f"operator rollout did not complete within {timeout}s: {e}") from e
        stuck = [p for p in operator_pods(ns) if p["image"] == local_image and p.get("waiting_reason") in ("ErrImagePull", "ImagePullBackOff")]
        if stuck and local_image:
            logger.warning(f"Operator pod(s) {[p['name'] for p in stuck]} cannot pull {local_image} (kubelet image GC?); re-importing")
            import_image_into_cluster(local_image)
            for p in stuck:
                k8s.delete_pod(ns, p["name"])


def ensure_cert_manager() -> None:
    if subprocess.run(["kubectl", "get", "deployment", "cert-manager-webhook", "-n", "cert-manager"], capture_output=True).returncode == 0:
        return
    logger.info("Installing cert-manager (required by the operator's validating webhooks)")
    sh(["helm", "repo", "add", "jetstack", "https://charts.jetstack.io", "--force-update"])
    sh(["helm", "repo", "update", "jetstack"])
    sh(["helm", "upgrade", "--install", "cert-manager", "jetstack/cert-manager", "-n", "cert-manager", "--create-namespace", "--set", "crds.enabled=true", "--wait", "--timeout", "5m"])


def operator_dirs(local_path: str):
    """Accept the repo root or the opensearch-operator/ subdir; return (go_dir, chart_dir)."""
    if not local_path:
        raise RuntimeError("opensearch.local_operator_path is not set but operator_version is 'local'")
    root = os.path.abspath(os.path.expanduser(local_path))
    go_dir = root if os.path.exists(os.path.join(root, "Makefile")) else os.path.join(root, "opensearch-operator")
    chart = os.path.join(os.path.dirname(go_dir), "charts", "opensearch-operator")
    for d in (go_dir, chart):
        if not os.path.isdir(d):
            raise RuntimeError(f"operator directory not found: {d}")
    return go_dir, chart


def installed_chart(release: str, ns: str) -> Optional[str]:
    """'opensearch-operator-3.0.10' for the deployed helm release, None when not installed."""
    out = subprocess.run(["helm", "list", "-n", ns, "-o", "json"], capture_output=True, text=True).stdout
    return next((r.get("chart") for r in (json.loads(out) if out.strip() else []) if r.get("name") == release), None)


def local_chart_name_version(chart_dir: str) -> str:
    meta = {}
    with open(os.path.join(chart_dir, "Chart.yaml")) as f:
        for line in f:
            if ":" in line and not line.startswith(" "):
                k, v = line.split(":", 1)
                meta[k.strip()] = v.strip().strip("\"'")
    return f"{meta.get('name')}-{meta.get('version')}"


def local_operator_image(go_dir: str) -> str:
    """Image tag derived from the source: git sha plus a hash of the working-tree diff."""
    sha = sh(["git", "rev-parse", "--short", "HEAD"], cwd=go_dir).strip()
    diff = sh(["git", "diff", "HEAD", "--", "."], cwd=go_dir)
    return f"opensearch-operator:dev-{sha}" + (f"-{hashlib.sha1(diff.encode()).hexdigest()[:8]}" if diff.strip() else "")


def build_local_operator(go_dir: str, image: str) -> None:
    """Build the operator image from source (unless already built) and load it into the k3d/kind cluster."""
    if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
        logger.info(f"Building operator image {image} from {go_dir} (this can take a few minutes)")
        sh(["make", "docker-build", f"IMG={image}"], cwd=go_dir, timeout=1800)
    else:
        logger.info(f"Reusing already built operator image {image}")
    import_image_into_cluster(image)


def build_operator_ref(go_dir: str, ref: str) -> str:
    """Build opensearch-operator:<ref> from a git ref of the operator repo (temporary worktree, plain docker build:
    old Makefiles need toolchains that no longer install) and load it into the cluster."""
    image = f"opensearch-operator:{ref}"
    if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
        root = sh(["git", "rev-parse", "--show-toplevel"], cwd=go_dir).strip()
        wt = os.path.join(tempfile.gettempdir(), f"oko-operator-{ref}")
        subprocess.run(["git", "worktree", "remove", "--force", wt], cwd=root, capture_output=True)
        sh(["git", "worktree", "add", "--detach", wt, ref], cwd=root)
        try:
            logger.info(f"Building operator image {image} from git ref {ref}")
            sh(["docker", "build", "-t", image, "."], cwd=os.path.join(wt, os.path.relpath(go_dir, root)), timeout=1800)
        finally:
            subprocess.run(["git", "worktree", "remove", "--force", wt], cwd=root, capture_output=True)
    import_image_into_cluster(image)
    return image


def import_image_into_cluster(image: str) -> None:
    """Load a local docker image into the k3d/kind nodes (idempotent, retried: k3d's tools container is single-instance)."""
    ctx = k8s.current_context()
    if ctx.startswith("k3d-"):
        cmd = ["k3d", "image", "import", image, "-c", ctx[4:]]
    elif ctx.startswith("kind-"):
        cmd = ["kind", "load", "docker-image", image, "--name", ctx[5:]]
    else:
        logger.warning(f"Context {ctx} is neither k3d nor kind; assuming {image} is reachable by the cluster")
        return
    for attempt in range(5):
        try:
            sh(cmd, timeout=600)
            return
        except RuntimeError as e:
            if attempt == 4:
                raise
            logger.info(f"image import failed ({str(e)[-120:]}); retrying in 15s")
            time.sleep(15)


class DeployClusterAction(BaseAction):
    """Create the security secrets and the OpenSearchCluster resource."""

    action_name = "deploy_cluster"
    params = {"version", "node_pools", "plugins", "cluster_settings", "dashboards", "storage_class", "disk_size", "manifest", "drain_data_nodes", "extra_spec"}

    def execute(self, params):
        o = self.config.opensearch
        # __CLUSTER__ / __NAMESPACE__ placeholders (cluster names are random per run) anywhere in the params
        params = json.loads(json.dumps(params).replace("__CLUSTER__", self.cluster).replace("__NAMESPACE__", self.namespace))
        k8s.ensure_namespace(self.namespace)
        create_security_secrets(self.namespace, o.security.username, o.security.password)
        manifest = params.get("manifest") or json.dumps(self.build_cr(params))
        k8s.apply(manifest, self.namespace)
        version = params.get("version", o.opensearch_version)
        return ActionResult(True, f"Applied OpenSearchCluster {self.namespace}/{self.cluster} (OpenSearch {version})")

    def build_cr(self, params: Dict[str, Any]) -> Dict[str, Any]:
        o = self.config.opensearch
        version = str(params.get("version", o.opensearch_version))
        pools = params.get("node_pools") or [{"component": "nodes", "replicas": 3, "roles": ["cluster_manager", "data", "ingest"]}]
        spec: Dict[str, Any] = {
            "general": {
                "version": version,
                "serviceName": self.cluster,
                "httpPort": 9200,
                "setVMMaxMapCount": True,
                "drainDataNodes": bool(params.get("drain_data_nodes", False)),
                "additionalConfig": {k: str(v) for k, v in {**o.default_cluster_settings, **(params.get("cluster_settings") or {})}.items()},
            },
            "security": {
                "config": {"securityConfigSecret": {"name": "securityconfig-secret"}, "adminCredentialsSecret": {"name": "admin-credentials-secret"}},
                "tls": {"transport": {"generate": True, "perNode": True}, "http": {"generate": True}},
            },
            "dashboards": {"enable": False, "version": version, "replicas": 0},  # the 2.x CRD requires version+replicas; a stored 0 blocks migration (FINDINGS-round3 N25)
            "nodePools": [],
        }
        if params.get("plugins"):
            spec["general"]["pluginsList"] = list(params["plugins"])
        dash = params.get("dashboards") or {}
        if dash.get("enabled"):
            spec["dashboards"] = {"enable": True, "version": dash.get("version", version), "replicas": int(dash.get("replicas", 1)), "opensearchCredentialsSecret": {"name": "dashboards-credentials"}}
        elif "replicas" in dash:  # disabled dashboards with an explicit replica count (51-* sidesteps N25 with 1)
            spec["dashboards"]["replicas"] = int(dash["replicas"])
        storage_class = params.get("storage_class", o.storage_class)
        for pool in pools:
            p = {
                "component": pool["component"],
                "replicas": int(pool.get("replicas", 1)),
                "roles": list(pool.get("roles", ["cluster_manager", "data", "ingest"])),
                "diskSize": str(pool.get("disk_size", params.get("disk_size", o.disk_size))),
                "resources": pool.get("resources", o.node_resources),
                "jvm": pool.get("jvm", o.jvm_heap),
            }
            if storage_class:
                p["persistence"] = {"pvc": {"storageClass": storage_class, "accessModes": ["ReadWriteOnce"]}}
            for key in ("labels", "annotations", "env", "nodeSelector", "tolerations", "additionalConfig", "pdb"):
                if key in pool:
                    p[key] = pool[key]
            spec["nodePools"].append(p)
        deep_merge(spec, params.get("extra_spec") or {})
        return {"apiVersion": f"{o.api_group}/v1", "kind": "OpenSearchCluster", "metadata": {"name": self.cluster, "namespace": self.namespace}, "spec": spec}


def deep_merge(dst: Dict, src: Dict) -> Dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def create_security_secrets(namespace: str, username: str, password: str) -> None:
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()
    files = {
        "config.yml": "_meta:\n  type: config\n  config_version: 2\nconfig:\n  dynamic:\n    authc:\n      basic_internal_auth_domain:\n        http_enabled: true\n        transport_enabled: true\n        order: 4\n        http_authenticator:\n          type: basic\n          challenge: true\n        authentication_backend:\n          type: intern\n",
        # kibanaserver is the Dashboards service user (dashboards-credentials below); the 3.x operator adds it to a custom
        # securityconfig itself, operators <= 2.8 do not, so the harness config is self-contained
        "internal_users.yml": f'_meta:\n  type: internalusers\n  config_version: 2\n{username}:\n  hash: "{pw_hash}"\n  reserved: true\n  backend_roles:\n  - admin\nkibanaserver:\n  hash: "{pw_hash}"\n  reserved: true\n',
        "roles.yml": "_meta:\n  type: roles\n  config_version: 2\n",
        "roles_mapping.yml": f"_meta:\n  type: rolesmapping\n  config_version: 2\nall_access:\n  reserved: false\n  backend_roles:\n  - admin\n  users:\n  - {username}\nsecurity_rest_api_access:\n  reserved: false\n  backend_roles:\n  - admin\n  users:\n  - {username}\nkibana_server:\n  reserved: true\n  users:\n  - kibanaserver\n",
        "action_groups.yml": "_meta:\n  type: actiongroups\n  config_version: 2\n",
        "tenants.yml": "_meta:\n  type: tenants\n  config_version: 2\n",
        "nodes_dn.yml": "_meta:\n  type: nodesdn\n  config_version: 2\n",
        "whitelist.yml": "_meta:\n  type: whitelist\n  config_version: 2\nconfig:\n  enabled: false\n",
    }
    b64 = lambda s: base64.b64encode(s.encode()).decode()  # noqa: E731
    secrets = [
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "admin-credentials-secret", "namespace": namespace}, "data": {"username": b64(username), "password": b64(password)}},
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "securityconfig-secret", "namespace": namespace}, "data": {k: b64(v) for k, v in files.items()}},
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "dashboards-credentials", "namespace": namespace}, "data": {"username": b64("kibanaserver"), "password": b64(password)}},
    ]
    for s in secrets:
        k8s.apply(json.dumps(s), namespace)


class DeleteClusterAction(BaseAction):
    """Delete the OpenSearchCluster and wait for its pods and PVCs to go away."""

    action_name = "delete_cluster"
    params = {"delete_namespace", "expect_pvcs_deleted"}

    def execute(self, params):
        res = self.cr_resource()
        k8s.kubectl("delete", res, self.cluster, "-n", self.namespace, "--ignore-not-found", "--wait=false")
        timeout = self.timeout("10m")
        k8s.wait_for("cluster resource deletion", lambda: not k8s.get_cr(self.config.opensearch.api_group, self.cluster, self.namespace), timeout, 5)
        k8s.wait_for("cluster pods deletion", lambda: not self.pods(), timeout, 5)
        pvcs = [i["metadata"]["name"] for i in k8s.get_json("pvc", "-n", self.namespace).get("items", [])]
        # The operator keeps PVCs by default; the harness removes them so re-runs start clean.
        if pvcs and params.get("expect_pvcs_deleted"):
            return ActionResult(False, f"PVCs still present after cluster deletion: {pvcs}")
        for pvc in pvcs:
            k8s.kubectl("delete", "pvc", pvc, "-n", self.namespace, "--ignore-not-found", "--wait=false")
        if params.get("delete_namespace"):
            k8s.kubectl("delete", "ns", self.namespace, "--ignore-not-found", "--wait=false")
        return ActionResult(True, f"Deleted OpenSearchCluster {self.namespace}/{self.cluster}" + (f", removed PVCs {pvcs}" if pvcs else ""))


class CleanupClusterAction(BaseAction):
    """Delete the k3d/kind cluster created by setup_cluster."""

    action_name = "cleanup_cluster"
    params = set()

    def execute(self, params):
        k = self.config.kubernetes
        if k.provider == "k3d":
            sh(["k3d", "cluster", "delete", k.cluster_name])
        elif k.provider == "kind":
            sh(["kind", "delete", "cluster", "--name", k.cluster_name])
        else:
            return ActionResult(True, "Existing cluster left untouched")
        return ActionResult(True, f"Deleted {k.provider} cluster {k.cluster_name}")


class UpdateClusterSettingsAction(BaseAction):
    """Explicit transient cluster settings change. Do NOT use this to 'fix' allocation after operator
    operations: the operator must restore allocation itself, and wait_for_cluster_ready asserts that."""

    action_name = "update_cluster_settings"
    params = {"settings"}

    def execute(self, params):
        with self.os_client() as c:
            c.put_cluster_settings({k: v for k, v in params["settings"].items()})
        return ActionResult(True, f"Applied transient settings {params['settings']}")


class SetApiGroupAction(BaseAction):
    """Switch the API group used by subsequent steps (e.g. after a legacy cluster was migrated to opensearch.org)."""

    action_name = "set_api_group"
    params = {"api_group"}

    def execute(self, params):
        self.config.opensearch.api_group = params["api_group"]
        try:  # the migration controller creates the opensearch.org twin asynchronously (only once the legacy CR is RUNNING)
            cr = k8s.wait_for(f"{k8s.cluster_resource(params['api_group'])}/{self.cluster}", lambda: k8s.get_cr(params["api_group"], self.cluster, self.namespace), self.timeout("30s"), 5)
        except TimeoutError as e:
            return ActionResult(False, f"No {k8s.cluster_resource(params['api_group'])} named {self.cluster} in {self.namespace}: {e}")
        ann = cr["metadata"].get("annotations", {})
        return ActionResult(True, f"Now using {params['api_group']}; CR phase {cr.get('status', {}).get('phase')}, migrated-from={ann.get('opensearch.org/migrated-from')}")
