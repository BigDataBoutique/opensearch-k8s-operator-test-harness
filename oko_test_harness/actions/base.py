"""Base action class for all test operations."""

from abc import ABC, abstractmethod
from typing import Any, Dict
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
        from datetime import datetime
        import time

        # Simple template substitution
        substitutions = {
            "{timestamp}": str(int(time.time())),
            "{datetime}": datetime.now().isoformat(),
        }

        for placeholder, replacement in substitutions.items():
            value = value.replace(placeholder, replacement)

        return value
