"""Base action class for all test operations."""

import time
from abc import ABC, abstractmethod
from typing import Any, Dict
from datetime import datetime
from loguru import logger

from oko_test_harness.models.playbook import ActionResult, Config


class BaseAction(ABC):
    """Base class for all actions."""

    action_name: str = ""

    def __init__(self, config: Config, variables: Dict[str, Any] = None):
        self.config = config
        self.variables = variables or {}
        self.logger = logger.bind(action=self.action_name)

    @abstractmethod
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute the action with given parameters."""
        pass

    def _merge_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Merge parameters with variables, with params taking precedence."""
        merged = self.variables.copy()
        merged.update(params)
        return merged

    def _substitute_template_vars(self, value: str) -> str:
        """Substitute template variables in a string."""
        # Simple template substitution
        substitutions = {
            "{timestamp}": str(int(time.time())),
            "{datetime}": datetime.now().isoformat(),
        }

        for placeholder, replacement in substitutions.items():
            value = value.replace(placeholder, replacement)

        return value

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string to seconds.

        Args:
            duration_str: Duration string like '30s', '5m', '2h', or '300'

        Returns:
            Duration in seconds
        """
        if duration_str.endswith("s"):
            return int(duration_str[:-1])
        elif duration_str.endswith("m"):
            return int(duration_str[:-1]) * 60
        elif duration_str.endswith("h"):
            return int(duration_str[:-1]) * 3600
        else:
            return int(duration_str)

    def _build_scale_patch(self, node_type: str, target_count: int) -> Dict[str, Any]:
        """Build patch data for scaling operation."""
        component_map = {"data": "data", "master": "masters"}
        component = component_map.get(node_type, node_type)

        return {
            "spec": {"nodePools": [{"component": component, "replicas": target_count}]}
        }
