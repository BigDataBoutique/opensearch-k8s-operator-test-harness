"""Playbook data models."""

from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from loguru import logger


def filter_dataclass_kwargs(cls, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Filter kwargs to only include valid dataclass fields, warning about unknown ones."""
    valid_fields = {f.name for f in fields(cls)}
    filtered_kwargs = {}
    unknown_fields = []
    
    for key, value in kwargs.items():
        if key in valid_fields:
            filtered_kwargs[key] = value
        else:
            unknown_fields.append(key)
    
    if unknown_fields:
        logger.warning(f"Ignoring unknown fields for {cls.__name__}: {unknown_fields}")
    
    return filtered_kwargs


@dataclass
class Metadata:
    """Playbook metadata."""
    name: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None


@dataclass
class KubernetesConfig:
    """Kubernetes cluster configuration."""
    cluster_name: str = "opensearch-test"
    kubernetes_version: str = "v1.28.0"
    nodes: int = 3
    cleanup_on_failure: bool = True
    cleanup_on_success: bool = True
    provider: str = "kind"
    extra_config: Optional[str] = None


@dataclass
class SecurityConfig:
    """Security configuration for OpenSearch."""
    enabled: bool = True  # Security enabled by default
    username: str = "admin"
    password: str = "Admin123!"  # OpenSearch 2.17+ compliant password
    use_ssl: bool = True
    verify_ssl: bool = False
    admin_secret_name: str = "admin-credentials-secret"
    security_config_secret_name: str = "securityconfig-secret"


@dataclass
class OpenSearchConfig:
    """OpenSearch configuration."""
    operator_namespace: str = "opensearch-operator-system"
    operator_version: str = "latest"
    local_operator_path: Optional[str] = None
    opensearch_version: str = "2.11.0"
    cluster_name: str = "test-cluster"
    service_name: str = "test-cluster"
    security: SecurityConfig = field(default_factory=SecurityConfig)
    node_resources: Optional[Dict[str, Any]] = None
    jvm_heap: Optional[str] = None


@dataclass
class TimeoutConfig:
    """Timeout configuration for various operations."""
    deployment: str = "10m"
    upgrade: str = "15m"
    scaling: str = "5m"
    recovery: str = "20m"


@dataclass
class DataValidationConfig:
    """Data validation configuration."""
    document_count: int = 10000
    index_pattern: str = "test-index-{timestamp}"
    query_timeout: str = "30s"


@dataclass
class Config:
    """Global configuration for the test run."""
    kubernetes: KubernetesConfig = field(default_factory=KubernetesConfig)
    opensearch: OpenSearchConfig = field(default_factory=OpenSearchConfig)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    data_validation: DataValidationConfig = field(default_factory=DataValidationConfig)


@dataclass
class ActionStep:
    """A single action step in a phase."""
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    timeout: Optional[str] = None
    retry_count: int = 0
    background: bool = False


@dataclass
class Phase:
    """A phase containing multiple action steps."""
    name: str
    description: Optional[str] = None
    steps: List[ActionStep] = field(default_factory=list)


@dataclass
class Playbook:
    """Complete playbook definition."""
    metadata: Metadata = field(default_factory=Metadata)
    config: Config = field(default_factory=Config)
    phases: List[Phase] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Playbook":
        """Create a playbook from a dictionary."""
        metadata_data = filter_dataclass_kwargs(Metadata, data.get("metadata", {}))
        metadata = Metadata(**metadata_data)
        
        config_data = data.get("config", {})
        k8s_data = filter_dataclass_kwargs(KubernetesConfig, config_data.get("kubernetes", {}))
        k8s_config = KubernetesConfig(**k8s_data)
        
        opensearch_data = config_data.get("opensearch", {})
        security_data = opensearch_data.get("security", {})
        security_filtered = filter_dataclass_kwargs(SecurityConfig, security_data)
        security_config = SecurityConfig(**security_filtered)
        
        # Remove security from opensearch_data to avoid duplicate parameter
        opensearch_data_clean = {k: v for k, v in opensearch_data.items() if k != "security"}
        opensearch_filtered = filter_dataclass_kwargs(OpenSearchConfig, opensearch_data_clean)
        os_config = OpenSearchConfig(security=security_config, **opensearch_filtered)
        
        timeout_data = filter_dataclass_kwargs(TimeoutConfig, config_data.get("timeouts", {}))
        timeout_config = TimeoutConfig(**timeout_data)
        
        data_validation_data = filter_dataclass_kwargs(DataValidationConfig, config_data.get("data_validation", {}))
        data_config = DataValidationConfig(**data_validation_data)
        
        config = Config(
            kubernetes=k8s_config,
            opensearch=os_config,
            timeouts=timeout_config,
            data_validation=data_config
        )
        
        phases = []
        for phase_data in data.get("phases", []):
            steps = []
            for step_data in phase_data.get("steps", []):
                step_filtered = filter_dataclass_kwargs(ActionStep, step_data)
                steps.append(ActionStep(**step_filtered))
            phase_filtered = filter_dataclass_kwargs(Phase, phase_data)
            # Handle steps separately since it's a complex field
            phase_filtered["steps"] = steps
            phases.append(Phase(**phase_filtered))
        
        return cls(metadata=metadata, config=config, phases=phases)


class ActionResult:
    """Result of an action execution."""
    
    def __init__(self, success: bool, message: str = "", data: Optional[Dict[str, Any]] = None):
        self.success = success
        self.message = message
        self.data = data or {}
        
    def __bool__(self) -> bool:
        return self.success


class ExecutionStatus(Enum):
    """Status of playbook execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ExecutionContext:
    """Context for playbook execution."""
    playbook: Playbook
    variables: Dict[str, Any] = field(default_factory=dict)
    status: ExecutionStatus = ExecutionStatus.PENDING
    current_phase: Optional[str] = None
    current_step: Optional[str] = None
    results: Dict[str, ActionResult] = field(default_factory=dict)
    start_time: Optional[float] = None
    end_time: Optional[float] = None