"""Scaling actions."""

import json
import subprocess
import time
from typing import Any, Dict

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.utils.opensearch_client import KubernetesOpenSearchClient


class ScaleClusterAction(BaseAction):
    """Action to scale up cluster nodes."""

    action_name = "scale_cluster"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        node_type = params.get("node_type", "data")
        target_count = params.get("target_count", 5)
        strategy = params.get("strategy", "gradual")
        wait_for_green = params.get("wait_for_green", True)
        timeout_str = params.get("timeout", "10m")
        cluster_name = params.get("cluster_name", self.config.opensearch.cluster_name)
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)

        self.logger.info(f"Scaling {node_type} nodes to {target_count}")

        try:
            # Update cluster spec
            patch_data = self._build_scale_patch(node_type, target_count)

            result = subprocess.run(
                [
                    "kubectl",
                    "patch",
                    "opensearchcluster",
                    cluster_name,
                    "-n",
                    namespace,
                    "--type",
                    "merge",
                    "-p",
                    json.dumps(patch_data),
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                return ActionResult(
                    False, f"Failed to update cluster spec: {result.stderr}"
                )

            # Wait for scaling to complete
            timeout = self._parse_duration(timeout_str)
            if not self._wait_for_scaling_completion(
                namespace, node_type, target_count, timeout
            ):
                return ActionResult(False, "Scaling operation timed out")

            # Wait for cluster health if requested
            if wait_for_green:
                try:
                    with KubernetesOpenSearchClient(namespace=namespace) as client:
                        if not client.wait_for_cluster_health("green", 300):
                            return ActionResult(
                                False,
                                "Cluster did not reach green status after scaling",
                            )
                except Exception as e:
                    self.logger.warning(f"Could not verify cluster health: {e}")

            return ActionResult(
                True, f"Successfully scaled {node_type} nodes to {target_count}"
            )

        except Exception as e:
            return ActionResult(False, f"Failed to scale cluster: {e}")

    def _build_scale_patch(self, node_type: str, target_count: int) -> Dict[str, Any]:
        """Build patch data for scaling operation."""
        # This would depend on the specific OpenSearch operator CRD structure
        # Here's a simplified example
        if node_type == "data":
            return {
                "spec": {"nodePools": [{"component": "data", "replicas": target_count}]}
            }
        elif node_type == "master":
            return {
                "spec": {
                    "nodePools": [{"component": "masters", "replicas": target_count}]
                }
            }
        else:
            return {
                "spec": {
                    "nodePools": [{"component": node_type, "replicas": target_count}]
                }
            }

    def _wait_for_scaling_completion(
        self, namespace: str, node_type: str, target_count: int, timeout: int
    ) -> bool:
        """Wait for scaling operation to complete."""
        start_time = time.time()

        label_selector = f"opensearch.role/{node_type}=true"
        if node_type == "master":
            label_selector = "opensearch.role/master=true"
        elif node_type == "data":
            label_selector = "opensearch.role/data=true"

        while time.time() - start_time < timeout:
            try:
                result = subprocess.run(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-n",
                        namespace,
                        "-l",
                        label_selector,
                        "--field-selector",
                        "status.phase=Running",
                        "-o",
                        "json",
                    ],
                    capture_output=True,
                    text=True,
                )

                if result.returncode == 0:
                    pods_data = json.loads(result.stdout)
                    running_pods = len(pods_data.get("items", []))

                    if running_pods >= target_count:
                        return True

                time.sleep(30)
            except Exception as e:
                self.logger.warning(f"Error checking scaling progress: {e}")
                time.sleep(30)

        return False

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string to seconds."""
        if duration_str.endswith("s"):
            return int(duration_str[:-1])
        elif duration_str.endswith("m"):
            return int(duration_str[:-1]) * 60
        elif duration_str.endswith("h"):
            return int(duration_str[:-1]) * 3600
        else:
            return int(duration_str)


class ScaleDownClusterAction(BaseAction):
    """Action to scale down cluster nodes."""

    action_name = "scale_down_cluster"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        node_type = params.get("node_type", "data")
        target_count = params.get("target_count", 2)
        drain_data = params.get("drain_data", True)
        force_after_timeout = params.get("force_after_timeout", "15m")
        cluster_name = params.get("cluster_name", self.config.opensearch.cluster_name)
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)

        self.logger.info(f"Scaling down {node_type} nodes to {target_count}")

        try:
            # Get current node count
            current_count = self._get_current_node_count(namespace, node_type)
            if current_count <= target_count:
                return ActionResult(
                    True, f"Already at or below target count ({current_count})"
                )

            # Drain data if requested and applicable
            if drain_data and node_type == "data":
                if not self._drain_data_nodes(namespace, current_count - target_count):
                    self.logger.warning(
                        "Data draining failed, proceeding with scale down"
                    )

            # Update cluster spec
            patch_data = self._build_scale_patch(node_type, target_count)

            result = subprocess.run(
                [
                    "kubectl",
                    "patch",
                    "opensearchcluster",
                    cluster_name,
                    "-n",
                    namespace,
                    "--type",
                    "merge",
                    "-p",
                    json.dumps(patch_data),
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                return ActionResult(
                    False, f"Failed to update cluster spec: {result.stderr}"
                )

            # Wait for scaling to complete
            timeout = self._parse_duration(force_after_timeout)
            if not self._wait_for_scale_down_completion(
                namespace, node_type, target_count, timeout
            ):
                return ActionResult(False, "Scale down operation timed out")

            return ActionResult(
                True, f"Successfully scaled down {node_type} nodes to {target_count}"
            )

        except Exception as e:
            return ActionResult(False, f"Failed to scale down cluster: {e}")

    def _get_current_node_count(self, namespace: str, node_type: str) -> int:
        """Get current count of nodes of specified type."""
        label_selector = f"opensearch.role/{node_type}=true"
        if node_type == "master":
            label_selector = "opensearch.role/master=true"
        elif node_type == "data":
            label_selector = "opensearch.role/data=true"

        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    label_selector,
                    "-o",
                    "json",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                pods_data = json.loads(result.stdout)
                return len(pods_data.get("items", []))
            return 0
        except Exception:
            return 0

    def _drain_data_nodes(self, namespace: str, nodes_to_remove: int) -> bool:
        """Drain data from nodes before removal."""
        try:
            # This would implement proper data draining logic
            # For now, we'll simulate waiting for data redistribution
            self.logger.info(f"Draining data from {nodes_to_remove} nodes...")

            with KubernetesOpenSearchClient(namespace=namespace) as client:
                # Wait for cluster to be stable before proceeding
                return client.wait_for_cluster_health("yellow", 300)
        except Exception as e:
            self.logger.error(f"Data draining failed: {e}")
            return False

    def _wait_for_scale_down_completion(
        self, namespace: str, node_type: str, target_count: int, timeout: int
    ) -> bool:
        """Wait for scale down operation to complete."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            current_count = self._get_current_node_count(namespace, node_type)
            if current_count <= target_count:
                return True
            time.sleep(30)

        return False

    def _build_scale_patch(self, node_type: str, target_count: int) -> Dict[str, Any]:
        """Build patch data for scaling operation."""
        if node_type == "data":
            return {
                "spec": {"nodePools": [{"component": "data", "replicas": target_count}]}
            }
        elif node_type == "master":
            return {
                "spec": {
                    "nodePools": [{"component": "masters", "replicas": target_count}]
                }
            }
        else:
            return {
                "spec": {
                    "nodePools": [{"component": node_type, "replicas": target_count}]
                }
            }

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string to seconds."""
        if duration_str.endswith("s"):
            return int(duration_str[:-1])
        elif duration_str.endswith("m"):
            return int(duration_str[:-1]) * 60
        elif duration_str.endswith("h"):
            return int(duration_str[:-1]) * 3600
        else:
            return int(duration_str)
