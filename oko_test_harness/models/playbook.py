"""Playbook data models."""

from dataclasses import dataclass, field, fields
from enum import Enum
from typing import Any, Dict, List, Optional

from loguru import logger


def _from(cls, data: Dict[str, Any]):
    valid = {f.name for f in fields(cls)}
    unknown = sorted(set(data) - valid)
    if unknown:
        raise ValueError(f"Unknown {cls.__name__} keys: {unknown} (valid: {sorted(valid)})")
    return cls(**data)


@dataclass
class Metadata:
    name: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class KubernetesConfig:
    cluster_name: str = "oko"
    provider: str = "k3d"  # k3d | kind | existing
    nodes: int = 3  # worker/agent nodes
    kubernetes_version: Optional[str] = None
    cleanup_on_success: bool = True
    cleanup_on_failure: bool = False


@dataclass
class SecurityConfig:
    username: str = "admin"
    password: str = "Admin123!"  # meets the 2.12+ initial password policy
    use_ssl: bool = True


@dataclass
class OpenSearchConfig:
    operator_namespace: str = "opensearch-operator-system"
    operator_release: str = "opensearch-operator"
    operator_version: str = "local"  # "local" builds from local_operator_path, otherwise a chart version
    local_operator_path: Optional[str] = None
    helm_repo_url: str = "https://opensearch-project.github.io/opensearch-k8s-operator/"
    api_group: str = "opensearch.org"
    namespace: Optional[str] = None  # namespace for the OpenSearch cluster; random when unset
    cluster_name: Optional[str] = None
    opensearch_version: str = "3.0.0"
    security: SecurityConfig = field(default_factory=SecurityConfig)
    node_resources: Dict[str, Any] = field(default_factory=lambda: {"requests": {"cpu": "250m", "memory": "1Gi"}, "limits": {"cpu": "2", "memory": "2Gi"}})
    jvm_heap: str = "-Xmx768m -Xms768m"
    storage_class: Optional[str] = None
    disk_size: str = "5Gi"
    # applied to every deployed cluster via general.additionalConfig; playbook cluster_settings are merged on top
    default_cluster_settings: Dict[str, Any] = field(default_factory=dict)
    operator_restart_baseline: Optional[int] = None  # set by install_operator; validate_operator_status counts restarts since then


@dataclass
class TimeoutConfig:
    deployment: str = "15m"
    upgrade: str = "25m"
    scaling: str = "15m"
    recovery: str = "10m"
    step: str = "40m"  # hard backstop per foreground step


@dataclass
class Config:
    kubernetes: KubernetesConfig = field(default_factory=KubernetesConfig)
    opensearch: OpenSearchConfig = field(default_factory=OpenSearchConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        os_data = dict(data.get("opensearch", {}))
        security = _from(SecurityConfig, os_data.pop("security", {}) or {})
        return cls(
            kubernetes=_from(KubernetesConfig, data.get("kubernetes", {}) or {}),
            opensearch=_from(OpenSearchConfig, {**os_data, "security": security}),
            timeouts=_from(TimeoutConfig, data.get("timeouts", {}) or {}),
        )


@dataclass
class ActionStep:
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    background: bool = False  # run in a thread; joined and asserted at the end of the phase
    continue_on_error: bool = False


@dataclass
class Phase:
    name: str
    description: Optional[str] = None
    steps: List[ActionStep] = field(default_factory=list)


@dataclass
class Playbook:
    metadata: Metadata = field(default_factory=Metadata)
    config: Config = field(default_factory=Config)
    phases: List[Phase] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Playbook":
        unknown = set(data) - {"metadata", "config", "phases"}
        if unknown:
            raise ValueError(f"Unknown top-level playbook keys: {sorted(unknown)}")
        phases = []
        for p in data.get("phases", []):
            p = dict(p)
            steps = [_from(ActionStep, s) for s in p.pop("steps", [])]
            phases.append(_from(Phase, {**p, "steps": steps}))
        return cls(metadata=_from(Metadata, data.get("metadata", {}) or {}), config=Config.from_dict(data.get("config", {}) or {}), phases=phases)


class ActionResult:
    def __init__(self, success: bool, message: str = "", data: Optional[Dict[str, Any]] = None):
        self.success, self.message, self.data = success, message, data or {}

    def __bool__(self) -> bool:
        return self.success


class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class ExecutionContext:
    playbook: Playbook
    variables: Dict[str, Any] = field(default_factory=dict)
    status: ExecutionStatus = ExecutionStatus.PENDING
    current_phase: Optional[str] = None
    results: Dict[str, ActionResult] = field(default_factory=dict)
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    failure: Optional[str] = None


__all__ = ["logger"]
