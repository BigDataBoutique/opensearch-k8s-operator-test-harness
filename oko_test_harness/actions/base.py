"""Base action class."""

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set

from loguru import logger

from oko_test_harness import k8s
from oko_test_harness.models.playbook import ActionResult, Config
from oko_test_harness.observer import ClusterObserver
from oko_test_harness.opensearch import OpenSearchClient

_DURATION = re.compile(r"^(\d+)([smh]?)$")
_UNITS = {"": 1, "s": 1, "m": 60, "h": 3600}


def parse_duration(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    m = _DURATION.match(str(value).strip())
    if not m:
        raise ValueError(f"Bad duration: {value!r} (use e.g. 30s, 5m, 1h)")
    return int(m.group(1)) * _UNITS[m.group(2)]


class BaseAction(ABC):
    action_name: str = ""
    params: Set[str] = set()  # accepted parameter names; anything else is an error
    # common params every action accepts
    common_params = {"namespace", "cluster_name", "timeout"}

    def __init__(self, config: Config, variables: Optional[Dict[str, Any]] = None):
        self.config = config
        self.variables = variables or {}
        self.logger = logger.bind(action=self.action_name)

    def run(self, params: Dict[str, Any]) -> ActionResult:
        unknown = set(params) - self.params - self.common_params
        if unknown:
            return ActionResult(False, f"{self.action_name}: unknown params {sorted(unknown)}. Accepted: {sorted(self.params | self.common_params)}")
        self.p = params
        self.namespace = params.get("namespace") or self.config.opensearch.namespace
        self.cluster = params.get("cluster_name") or self.config.opensearch.cluster_name
        try:
            return self.execute(params)
        except Exception as e:  # noqa: BLE001
            self.logger.exception(f"{self.action_name} raised")
            return ActionResult(False, f"{self.action_name} failed: {e}")

    @abstractmethod
    def execute(self, params: Dict[str, Any]) -> ActionResult: ...

    # --- helpers --------------------------------------------------------------------
    def timeout(self, default: str) -> int:
        return parse_duration(self.p.get("timeout", default))

    def os_client(self) -> OpenSearchClient:
        s = self.config.opensearch.security
        return OpenSearchClient(self.namespace, self.cluster, s.username, s.password, s.use_ssl)

    def cr(self) -> Dict[str, Any]:
        cr = k8s.get_cr(self.config.opensearch.api_group, self.cluster, self.namespace)
        if not cr:
            raise RuntimeError(f"OpenSearchCluster {self.namespace}/{self.cluster} not found")
        return cr

    def cr_resource(self) -> str:
        return k8s.cluster_resource(self.config.opensearch.api_group)

    def pods(self, component: Optional[str] = None):
        return k8s.get_pods(self.namespace, k8s.selector(self.cluster, component))

    def observer(self, indices=None) -> ClusterObserver:
        obs = ClusterObserver(self.os_client, self.namespace, self.cluster, indices)
        obs.start()
        return obs

    def finish_observed(self, obs: ClusterObserver, message: str, min_health="yellow", max_unready_pods=1, allow_doc_loss=False, max_nodes_down=None, max_step_drop=None) -> ActionResult:
        summary = obs.stop()
        self.logger.info(f"observer summary: {summary}")
        violations = obs.violations(min_health=min_health, max_unready_pods=max_unready_pods, allow_doc_loss=allow_doc_loss, max_nodes_down=max_nodes_down, max_step_drop=max_step_drop)
        if violations:
            return ActionResult(False, f"{message}; invariants violated during operation: {'; '.join(violations)}", summary)
        return ActionResult(True, f"{message}; during operation: worst health={summary['worst_health']}, max unready pods={summary['max_unready_pods']}, no document loss", summary)

    def wait_cluster_running(self, timeout: int, expect_version: Optional[str] = None) -> Dict[str, Any]:
        """Wait until the operator reports RUNNING, all pods in every pool are ready, and (optionally) the version matches."""

        def check():
            cr = self.cr()
            status = cr.get("status", {})
            spec_pools = {p["component"]: p for p in cr["spec"]["nodePools"]}
            # ignore the operator's helper job pods (securityconfig update etc.); keep node-pool pods and the bootstrap pod
            pods = [p for p in self.pods() if p["labels"].get(k8s.NODEPOOL_LABEL) or "-bootstrap-" in p["name"]]
            if any(p["phase"] == "Pending" for p in pods):
                k8s.unstick_local_path_helpers()
            if any(p["crash_loop"] for p in pods):
                raise k8s.StopWaiting(f"crash loop: {[p['name'] for p in pods if p['crash_loop']]}")
            if status.get("phase") != "RUNNING":
                raise Exception(f"phase={status.get('phase')} components={[(c.get('component'), c.get('status')) for c in status.get('componentsStatus', []) if c.get('status')]}")
            # the operator rolls pods after bootstrap (initial cluster manager setting removed) and after config changes
            in_flight = [
                (c.get("component"), c.get("description"), c.get("status"))
                for c in status.get("componentsStatus", [])
                if c.get("component") == "Upgrader" or (c.get("component") in ("RollingRestart", "Scaler") and c.get("status") not in ("Finished", "Running", "", None))
            ]
            if in_flight:
                raise Exception(f"operator still busy: {in_flight}")
            for comp, pool in spec_pools.items():
                ready = [p for p in pods if p["labels"].get(k8s.NODEPOOL_LABEL) == comp and p["ready"] and not p["deletion"]]
                if len(ready) != pool["replicas"]:
                    raise Exception(f"pool {comp}: {len(ready)}/{pool['replicas']} pods ready")
                if expect_version:
                    wrong = [p["name"] for p in ready if not p["image"].endswith(":" + expect_version)]
                    if wrong:
                        raise Exception(f"pods not on {expect_version}: {wrong}")
            extra = [p["name"] for p in pods if p["labels"].get(k8s.NODEPOOL_LABEL) not in spec_pools]
            if extra:
                raise Exception(f"bootstrap pod or pods from removed pools still present: {extra}")
            if expect_version and status.get("version") not in (expect_version, None):
                raise Exception(f"status.version={status.get('version')}")
            return cr

        return k8s.wait_for(f"cluster {self.cluster} RUNNING", check, timeout, interval=10)
