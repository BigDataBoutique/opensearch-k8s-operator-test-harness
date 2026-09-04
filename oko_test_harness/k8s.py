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


def get_pods(namespace: str, label_selector: Optional[str] = None) -> List[Dict[str, Any]]:
    args = ["pods", "-n", namespace]
    if label_selector:
        args += ["-l", label_selector]
    items = get_json(*args).get("items", [])
    pods = []
    for p in items:
        statuses = (p.get("status") or {}).get("containerStatuses") or []
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
                "labels": p["metadata"].get("labels") or {},
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
