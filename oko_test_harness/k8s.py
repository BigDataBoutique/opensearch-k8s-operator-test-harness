"""Thin kubectl wrapper. Everything the harness needs from Kubernetes goes through here."""

import json
import subprocess
import time
from typing import Any, Dict, List, Optional

from loguru import logger

CLUSTER_LABEL = "opensearch.org/opensearch-cluster"
NODEPOOL_LABEL = "opensearch.org/opensearch-nodepool"
HARNESS_LABEL = "oko-test"  # stamped on namespaces we create, so cleanup can find them
SYSTEM_INDEX_PREFIX = "."


class KubectlError(RuntimeError):
    pass


def run(cmd: List[str], input: Optional[str] = None, check: bool = True, timeout: int = 300) -> subprocess.CompletedProcess:
    logger.debug("$ " + " ".join(cmd))
    result = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise KubectlError(f"{' '.join(cmd[:4])}... failed: {result.stderr.strip() or result.stdout.strip()}")
    return result


def kubectl(*args: str, input: Optional[str] = None, check: bool = True, timeout: int = 300) -> str:
    return run(["kubectl", *args], input=input, check=check, timeout=timeout).stdout


def current_context() -> str:
    return kubectl("config", "current-context").strip()


def get_json(*args: str) -> Any:
    out = kubectl("get", *args, "-o", "json", check=False)
    return json.loads(out) if out.strip() else {}


def apply(manifest: str, namespace: Optional[str] = None) -> None:
    args = ["apply", "-f", "-"]
    if namespace:
        args += ["-n", namespace]
    kubectl(*args, input=manifest)


def ensure_namespace(name: str) -> None:
    if run(["kubectl", "get", "ns", name], check=False).returncode != 0:
        kubectl("create", "ns", name)
    kubectl("label", "ns", name, f"{HARNESS_LABEL}=true", "--overwrite")


def cluster_resource(api_group: str) -> str:
    return f"opensearchclusters.{api_group}"


def operator_crs(namespace: Optional[str] = None) -> List[str]:
    """Every operator-managed CR (clusters and child resources, both API groups) as 'kind ns/name'."""
    found = []
    for group in ("opensearch.org", "opensearch.opster.io"):
        for kind in kubectl("api-resources", f"--api-group={group}", "-o", "name", check=False).split():
            args = ["-n", namespace] if namespace else ["-A"]
            for i in (get_json(kind, *args) or {}).get("items", []):
                found.append(f"{kind} {i['metadata']['namespace']}/{i['metadata']['name']}")
    return found


def strip_operator_finalizers(namespace: str) -> List[str]:
    """Remove finalizers from every operator CR in a namespace that is already being deleted, so a namespace or CRD
    deletion cannot deadlock once the operator is gone (N23). Cleanup only: never call this while asserting on the operator."""
    freed = []
    for entry in operator_crs(namespace):
        kind, ref = entry.split(" ", 1)
        name = ref.split("/", 1)[1]
        kubectl("patch", kind, name, "-n", namespace, "--type=merge", "-p", '{"metadata":{"finalizers":[]}}', check=False)
        freed.append(entry)
    return freed


def selector(cluster: str, component: Optional[str] = None) -> str:
    sel = f"{CLUSTER_LABEL}={cluster}"
    if component:
        sel += f",{NODEPOOL_LABEL}={component}"
    return sel


def get_cr(api_group: str, name: str, namespace: str) -> Dict[str, Any]:
    return get_json(cluster_resource(api_group), name, "-n", namespace)


def replace_cr(cr: Dict[str, Any]) -> None:
    """Read-modify-write of the full CR (merge patches would replace the nodePools array)."""
    for k in ("managedFields", "resourceVersion", "uid", "creationTimestamp", "generation"):
        cr["metadata"].pop(k, None)
    cr.pop("status", None)
    apply(json.dumps(cr))


def patch_json(kind: str, name: str, namespace: str, ops: List[Dict[str, Any]]) -> None:
    """Targeted JSON patch. Unlike replace_cr it writes only the listed paths, so a spec change made
    concurrently by another step (an upgrade patch, say) is not clobbered by a stale read-modify-write."""
    kubectl("patch", kind, name, "-n", namespace, "--type", "json", "-p", json.dumps(ops))


LEGACY_LABEL_PREFIX = "opster.io/"  # operator <= 2.8 labels pods opster.io/opensearch-cluster|nodepool; 3.x uses opensearch.org/


def get_pods(namespace: str, label_selector: Optional[str] = None) -> List[Dict[str, Any]]:
    """Pods as dicts; a selector on the 3.x labels falls back to the 2.x labels and legacy labels are mirrored onto the 3.x keys."""
    items = get_json("pods", "-n", namespace, *(["-l", label_selector] if label_selector else [])).get("items", [])
    if not items and label_selector and "opensearch.org/" in label_selector:
        items = get_json("pods", "-n", namespace, "-l", label_selector.replace("opensearch.org/", LEGACY_LABEL_PREFIX)).get("items", [])
    pods = []
    for p in items:
        statuses = (p.get("status") or {}).get("containerStatuses") or []
        labels = p["metadata"].get("labels") or {}
        labels = {**{k.replace(LEGACY_LABEL_PREFIX, "opensearch.org/", 1): v for k, v in labels.items() if k.startswith(LEGACY_LABEL_PREFIX)}, **labels}
        pods.append(
            {
                "name": p["metadata"]["name"],
                "phase": (p.get("status") or {}).get("phase"),
                "ready": bool(statuses) and all(c.get("ready") for c in statuses),
                "restarts": sum(c.get("restartCount", 0) for c in statuses),
                "crash_loop": any(((c.get("state") or {}).get("waiting") or {}).get("reason") == "CrashLoopBackOff" for c in statuses),
                "waiting_reason": next((((c.get("state") or {}).get("waiting") or {}).get("reason") for c in statuses if (c.get("state") or {}).get("waiting")), None),
                "uid": p["metadata"]["uid"],
                "image": p["spec"]["containers"][0]["image"],
                "node": p["spec"].get("nodeName"),
                "labels": labels,
                "containers": p["spec"]["containers"],
                "deletion": bool(p["metadata"].get("deletionTimestamp")),
            }
        )
    return pods


def delete_pod(namespace: str, name: str, force: bool = False) -> None:
    args = ["delete", "pod", name, "-n", namespace, "--wait=false"]
    if force:
        args += ["--grace-period=0", "--force"]
    kubectl(*args)


def exec_in_pod(namespace: str, pod: str, *cmd: str, check: bool = False) -> subprocess.CompletedProcess:
    return run(["kubectl", "exec", pod, "-n", namespace, "--", *cmd], check=check, timeout=60)


def wait_for(description: str, predicate, timeout: int, interval: int = 10):
    """Poll predicate until it returns a truthy value. Raises TimeoutError with the last failure reason."""
    deadline = time.time() + timeout
    last = None
    while True:
        try:
            result = predicate()
            if result:
                return result
            last = "condition not met"
        except StopWaiting:
            raise
        except Exception as e:  # noqa: BLE001 - we report the last error on timeout
            last = str(e)
        if time.time() > deadline:
            raise TimeoutError(f"Timed out after {timeout}s waiting for {description}: {last}")
        time.sleep(interval)


class StopWaiting(Exception):
    """Raise inside a wait_for predicate to abort immediately (e.g. crash loop detected)."""


def unstick_local_path_helpers(min_age: int = 120) -> List[str]:
    """ponytail: k3s local-path-provisioner helper pods sometimes hang in Terminating under IO pressure
    (containerd StopPodSandbox deadline), which blocks PVC provisioning for minutes. Force-delete those older
    than min_age seconds so the provisioner retries. Only touches kube-system helper-pod-* pods."""
    import datetime

    killed = []
    for p in get_json("pods", "-n", "kube-system").get("items", []):
        name, meta = p["metadata"]["name"], p["metadata"]
        if not name.startswith("helper-pod-") or not meta.get("deletionTimestamp"):
            continue
        ts = datetime.datetime.strptime(meta["deletionTimestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
        if (datetime.datetime.now(datetime.timezone.utc) - ts).total_seconds() > min_age:
            kubectl("delete", "pod", name, "-n", "kube-system", "--force", "--grace-period=0", check=False)
            killed.append(name)
    if killed:
        logger.warning(f"force-deleted stuck local-path helper pods {killed} (k3s provisioner hang; see k8s.unstick_local_path_helpers)")
    return killed
