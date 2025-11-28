"""Playbook parser utilities."""

import os
import string
import random
from typing import Any, Dict, Optional
import yaml
from jinja2 import Environment, BaseLoader
from loguru import logger

from oko_test_harness.models.playbook import Playbook


class PlaybookParser:
    """Parser for YAML playbook files with templating support."""

    def __init__(self, global_config_path: Optional[str] = None):
        self.jinja_env = Environment(loader=BaseLoader())
        self._setup_template_functions()
        self._default_namespace = self._generate_random_namespace()
        self._default_cluster_name = self._generate_random_cluster_name()
        self._global_config = self._load_global_config(global_config_path)

    def _setup_template_functions(self) -> None:
        """Setup Jinja2 template functions."""
        import time
        import random
        from datetime import datetime

        def now() -> str:
            return datetime.now().isoformat()

        def timestamp() -> str:
            return str(int(time.time()))

        def random_choice(choices: list) -> str:
            return random.choice(choices)

        def env_var(name: str, default: str = "") -> str:
            return os.environ.get(name, default)

        self.jinja_env.globals.update(
            {
                "now": now,
                "timestamp": timestamp,
                "random": random_choice,
                "env": env_var,
            }
        )

    def _generate_random_namespace(self) -> str:
        """Generate a random namespace in format 'opensearch-{6 random chars}'."""
        random_chars = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=6)
        )
        return f"opensearch-{random_chars}"

    def _generate_random_cluster_name(self) -> str:
        """Generate a random cluster name in format 'opensearch-cluster-{6 random chars}'."""
        random_chars = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=6)
        )
        return f"opensearch-cluster-{random_chars}"

    def _load_global_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load global configuration from file."""
        if config_path is None:
            # Default to config.yaml in the project root
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "config.yaml",
            )

        if not os.path.exists(config_path):
            logger.debug(f"No global config found at {config_path}, using empty config")
            return {}

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            logger.debug(f"Loaded global config from {config_path}")
            return config or {}
        except Exception as e:
            logger.warning(f"Failed to load global config from {config_path}: {e}")
            return {}

    def _merge_configs(
        self, global_config: Dict[str, Any], playbook_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge global config with playbook config, allowing playbook to override global settings."""
        merged_config = global_config.copy()

        for key, value in playbook_config.items():
            if (
                key in merged_config
                and isinstance(merged_config[key], dict)
                and isinstance(value, dict)
            ):
                # Deep merge for nested dictionaries
                merged_config[key] = {**merged_config[key], **value}
            else:
                # Override for non-dict values or new keys
                merged_config[key] = value

        return merged_config

    def parse_file(self, file_path: str, variables: Dict[str, Any] = None) -> Playbook:
        """Parse a playbook file with optional variable substitution."""
        logger.debug(f"Parsing playbook file: {file_path}")

        with open(file_path, "r") as f:
            content = f.read()

        # Apply Jinja2 templating
        if variables:
            template = self.jinja_env.from_string(content)
            content = template.render(**variables)

        # Apply environment variable substitution
        content = self._substitute_env_vars(content)

        # Parse YAML
        data = yaml.safe_load(content)

        # Merge global config with playbook config
        playbook_config = data.get("config", {})
        merged_config = self._merge_configs(self._global_config, playbook_config)
        data["config"] = merged_config

        # Set playbook name from filename if not specified
        if "metadata" not in data:
            data["metadata"] = {}

        if not data["metadata"].get("name"):
            filename = os.path.basename(file_path)
            playbook_name = os.path.splitext(filename)[0]  # Remove extension
            data["metadata"]["name"] = playbook_name

        # Apply default random namespace if not specified
        self._apply_default_namespace(data)

        # Apply default random cluster name if not specified
        self._apply_default_cluster_name(data)

        # Create and return Playbook object
        playbook = Playbook.from_dict(data)
        logger.debug(f"Successfully parsed playbook: {playbook.metadata.name}")

        return playbook

    def _substitute_env_vars(self, content: str) -> str:
        """Substitute environment variables in ${VAR:-default} format."""
        import re

        def replace_env_var(match):
            var_expr = match.group(1)
            if ":-" in var_expr:
                var_name, default = var_expr.split(":-", 1)
                return os.environ.get(var_name.strip(), default.strip())
            else:
                return os.environ.get(var_expr.strip(), "")

        # Pattern matches ${VAR:-default} or ${VAR}
        pattern = r"\$\{([^}]+)\}"
        return re.sub(pattern, replace_env_var, content)

    def _apply_default_namespace(self, data: Dict[str, Any]) -> None:
        """Apply default randomized namespace if not specified in playbook."""
        # Check if opensearch namespace is already defined in config
        config = data.get("config", {})
        opensearch_config = config.get("opensearch", {})

        # If operator_namespace is not defined, use random namespace
        current_namespace = opensearch_config.get("operator_namespace")
        if not current_namespace:
            if "config" not in data:
                data["config"] = {}
            if "opensearch" not in data["config"]:
                data["config"]["opensearch"] = {}

            data["config"]["opensearch"]["operator_namespace"] = self._default_namespace
            logger.debug(
                f"Applied random operator_namespace: {self._default_namespace}"
            )

        # Also check for namespace references in action steps
        phases = data.get("phases", [])
        for phase in phases:
            for step in phase.get("steps", []):
                params = step.get("params", {})
                # If namespace is not specified in action params, use the default
                if "namespace" in params and params["namespace"] in [
                    "opensearch",
                    "opensearch-system",
                ]:
                    params["namespace"] = self._default_namespace
                    logger.debug(
                        f"Updated step namespace to: {self._default_namespace}"
                    )

    def _apply_default_cluster_name(self, data: Dict[str, Any]) -> None:
        """Apply cluster name configuration from steps or generate random if not specified."""
        # First, check if any step has explicitly set cluster_name
        cluster_name_from_steps = None
        phases = data.get("phases", [])
        for phase in phases:
            for step in phase.get("steps", []):
                params = step.get("params", {})
                if "cluster_name" in params:
                    cluster_name_from_steps = params["cluster_name"]
                    break
            if cluster_name_from_steps:
                break

        # Check if opensearch cluster_name is already defined in config
        config = data.get("config", {})
        opensearch_config = config.get("opensearch", {})

        # Determine the cluster name to use
        final_cluster_name = None
        if cluster_name_from_steps:
            # Use cluster name from step params (highest priority)
            final_cluster_name = cluster_name_from_steps
            logger.debug(f"Using cluster name from step params: {final_cluster_name}")
        elif (
            "cluster_name" in opensearch_config
            and opensearch_config["cluster_name"] != "test-cluster"
        ):
            # Use cluster name from config if not default
            final_cluster_name = opensearch_config["cluster_name"]
            logger.debug(f"Using cluster name from config: {final_cluster_name}")
        else:
            # Generate random cluster name
            final_cluster_name = self._default_cluster_name
            logger.debug(f"Applied random cluster name: {final_cluster_name}")

        # Set the cluster name in config
        if "config" not in data:
            data["config"] = {}
        if "opensearch" not in data["config"]:
            data["config"]["opensearch"] = {}

        data["config"]["opensearch"]["cluster_name"] = final_cluster_name
        # Sync service_name with cluster_name since the OpenSearch operator creates services with cluster_name
        data["config"]["opensearch"]["service_name"] = final_cluster_name

    def get_global_config(self) -> Any:
        """Get the global configuration as a Config object by creating a minimal playbook."""
        from oko_test_harness.models.playbook import Config, Playbook

        if not self._global_config:
            return Config()

        try:
            # Create minimal playbook data with just the config section
            minimal_playbook_data = {
                "metadata": {"name": "minimal"},
                "config": self._global_config,
                "phases": [{"name": "dummy", "steps": [{"action": "dummy"}]}],
            }

            # Use the existing from_dict method which properly handles config creation
            playbook = Playbook.from_dict(minimal_playbook_data)
            return playbook.config

        except Exception as e:
            logger.warning(f"Failed to create Config object from global config: {e}")
            return Config()

    def validate_playbook(self, playbook: Playbook) -> bool:
        """Validate a playbook structure."""
        try:
            if not playbook.phases:
                logger.error("Playbook must have at least one phase")
                return False

            for phase in playbook.phases:
                if not phase.steps:
                    logger.error(f"Phase '{phase.name}' must have at least one step")
                    return False

                for step in phase.steps:
                    if not step.action:
                        logger.error(
                            f"Step in phase '{phase.name}' must have an action"
                        )
                        return False

            logger.debug("Playbook validation successful")
            return True

        except Exception as e:
            logger.error(f"Playbook validation failed: {e}")
            return False
