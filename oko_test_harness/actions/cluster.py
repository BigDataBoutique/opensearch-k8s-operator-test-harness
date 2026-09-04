"""Kubernetes cluster, operator and OpenSearch cluster lifecycle actions."""

import base64
import hashlib
import json
import os
import subprocess
import time
from typing import Any, Dict, List

import bcrypt
from loguru import logger

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult

OPERATOR_SELECTOR = "app.kubernetes.io/name=opensearch-operator"


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
    params = {"version", "values", "values_file", "cert_manager", "legacy_api"}

    def execute(self, params):
        o = self.config.opensearch
        version = str(params.get("version", o.operator_version))
        ns = o.operator_namespace
        if params.get("cert_manager", True):
            ensure_cert_manager()
        common = ["-n", ns, "--create-namespace", "--wait", "--timeout", self.p.get("timeout", "10m")]
        if version == "local":
            go_dir, chart = operator_dirs(o.local_operator_path)
            image = local_operator_image(go_dir)
            running = [p for p in k8s.get_pods(ns, OPERATOR_SELECTOR) if p["ready"]]
            if running and all(p["image"] == image for p in running) and not params.get("values") and not params.get("values_file") and "legacy_api" not in params:
                return ActionResult(True, f"Operator already running {image} in {ns}", {"image": image})
            build_local_operator(go_dir, image)
            repo, tag = image.rsplit(":", 1)
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
            cmd = ["helm", "upgrade", "--install", o.operator_release, "opensearch-operator/opensearch-operator", "--version", version, *common]
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
        k8s.kubectl("rollout", "status", f"deployment/{o.operator_release}", "-n", ns, "--timeout=5m")
        k8s.kubectl("wait", "--for=condition=established", "--timeout=60s", f"crd/{self.cr_resource()}")
        pods = k8s.get_pods(ns, OPERATOR_SELECTOR)
        return ActionResult(True, f"Operator {version} installed in {ns} ({[p['image'] for p in pods]})", {"image": pods[0]["image"] if pods else None})


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
    ctx = k8s.current_context()
    if ctx.startswith("k3d-"):
        cmd = ["k3d", "image", "import", image, "-c", ctx[4:]]
    elif ctx.startswith("kind-"):
        cmd = ["kind", "load", "docker-image", image, "--name", ctx[5:]]
    else:
        logger.warning(f"Context {ctx} is neither k3d nor kind; assuming {image} is reachable by the cluster")
        return
    for attempt in range(5):  # concurrent playbooks may import at the same time; k3d's tools container is single-instance
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
            "dashboards": {"enable": False, "version": version, "replicas": 0},
            "nodePools": [],
        }
        if params.get("plugins"):
            spec["general"]["pluginsList"] = list(params["plugins"])
        dash = params.get("dashboards") or {}
        if dash.get("enabled"):
            spec["dashboards"] = {"enable": True, "version": dash.get("version", version), "replicas": int(dash.get("replicas", 1)), "opensearchCredentialsSecret": {"name": "dashboards-credentials"}}
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
        "internal_users.yml": f'_meta:\n  type: internalusers\n  config_version: 2\n{username}:\n  hash: "{pw_hash}"\n  reserved: true\n  backend_roles:\n  - admin\n',
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
            k8s.kubectl("delete", "pvc", pvc, "-n", self.namespace, "--wait=false")
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
        cr = k8s.get_cr(params["api_group"], self.cluster, self.namespace)
        if not cr:
            return ActionResult(False, f"No {k8s.cluster_resource(params['api_group'])} named {self.cluster} in {self.namespace}")
        ann = cr["metadata"].get("annotations", {})
        return ActionResult(True, f"Now using {params['api_group']}; CR phase {cr.get('status', {}).get('phase')}, migrated-from={ann.get('opensearch.org/migrated-from')}")
