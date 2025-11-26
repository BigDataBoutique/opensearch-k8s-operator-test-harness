"""Chaos engineering and failure injection actions."""

import random
import subprocess
import time
from typing import Any, Dict

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.utils.kubernetes import KubernetesManager


class InjectPodFailureAction(BaseAction):
    """Action to inject pod failures."""

    action_name = "inject_pod_failure"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        target = params.get("target", "data-nodes")
        count = params.get("count", 1)
        method = params.get("method", "delete")
        recovery_wait = params.get("recovery_wait", "2m")
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)

        self.logger.info(f"Injecting pod failure: {method} {count} {target}")

        try:
            k8s_manager = KubernetesManager()
            if not k8s_manager.load_config():
                return ActionResult(False, "Failed to load Kubernetes config")

            # Get target pods
            label_selector = self._get_label_selector(target)
            pods = k8s_manager.get_pods(namespace, label_selector)

            if len(pods) < count:
                return ActionResult(
                    False,
                    f"Not enough {target} pods found. Requested {count}, found {len(pods)}",
                )

            # Select random pods to target
            target_pods = random.sample(pods, count)

            # Apply failure method
            for pod in target_pods:
                if method == "delete":
                    self._delete_pod(pod["name"], namespace)
                elif method == "kill":
                    self._kill_pod_process(pod["name"], namespace)
                else:
                    return ActionResult(False, f"Unsupported failure method: {method}")

            # Wait for recovery
            recovery_seconds = self._parse_duration(recovery_wait)
            time.sleep(recovery_seconds)

            return ActionResult(
                True, f"Injected {method} failure on {count} {target} pod(s)"
            )

        except Exception as e:
            return ActionResult(False, f"Failed to inject pod failure: {e}")

    def _get_label_selector(self, target: str) -> str:
        """Get label selector for target type."""
        selectors = {
            "master-nodes": "opensearch.role/master=true",
            "data-nodes": "opensearch.role/data=true",
            "all-nodes": "app=opensearch",
            "ingest-nodes": "opensearch.role/ingest=true",
        }
        return selectors.get(target, "app=opensearch")

    def _delete_pod(self, pod_name: str, namespace: str) -> bool:
        """Delete a pod."""
        try:
            result = subprocess.run(
                ["kubectl", "delete", "pod", pod_name, "-n", namespace],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception as e:
            self.logger.error(f"Failed to delete pod {pod_name}: {e}")
            return False

    def _kill_pod_process(self, pod_name: str, namespace: str) -> bool:
        """Kill main process in pod."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "exec",
                    pod_name,
                    "-n",
                    namespace,
                    "--",
                    "pkill",
                    "-f",
                    "opensearch",
                ],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception as e:
            self.logger.error(f"Failed to kill process in pod {pod_name}: {e}")
            return False


class InjectNodeFailureAction(BaseAction):
    """Action to inject node failures."""

    action_name = "inject_node_failure"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        node_selector = params.get(
            "node_selector", "node-role.kubernetes.io/worker=true"
        )
        count = params.get("count", 1)
        method = params.get("method", "drain")

        self.logger.info(f"Injecting node failure: {method} {count} node(s)")

        try:
            # Get nodes matching selector
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "nodes",
                    "-l",
                    node_selector,
                    "-o",
                    "jsonpath={.items[*].metadata.name}",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                return ActionResult(False, f"Failed to get nodes: {result.stderr}")

            nodes = result.stdout.strip().split()
            if len(nodes) < count:
                return ActionResult(
                    False,
                    f"Not enough nodes found. Requested {count}, found {len(nodes)}",
                )

            target_nodes = random.sample(nodes, count)

            # Apply failure method
            for node in target_nodes:
                if method == "drain":
                    self._drain_node(node)
                elif method == "taint":
                    self._taint_node(node)
                else:
                    return ActionResult(
                        False, f"Unsupported node failure method: {method}"
                    )

            return ActionResult(
                True, f"Applied {method} to {count} node(s): {', '.join(target_nodes)}"
            )

        except Exception as e:
            return ActionResult(False, f"Failed to inject node failure: {e}")

    def _drain_node(self, node_name: str) -> bool:
        """Drain a node."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "drain",
                    node_name,
                    "--ignore-daemonsets",
                    "--delete-emptydir-data",
                ],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception as e:
            self.logger.error(f"Failed to drain node {node_name}: {e}")
            return False

    def _taint_node(self, node_name: str) -> bool:
        """Taint a node."""
        try:
            result = subprocess.run(
                ["kubectl", "taint", "node", node_name, "chaos=true:NoSchedule"],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception as e:
            self.logger.error(f"Failed to taint node {node_name}: {e}")
            return False


class InjectNetworkPartitionAction(BaseAction):
    """Action to inject network partitions."""

    action_name = "inject_network_partition"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        target = params.get("target", "master-nodes")
        duration = params.get("duration", "5m")
        partition_type = params.get("partition_type", "isolate")
        chaos_tool = params.get("chaos_tool", "chaos-mesh")

        self.logger.info(
            f"Injecting network partition: {partition_type} {target} for {duration}"
        )

        # This is a simplified implementation
        # In reality, you'd use Chaos Mesh, Litmus, or similar tools

        if chaos_tool == "chaos-mesh":
            return self._inject_with_chaos_mesh(target, duration, partition_type)
        else:
            return ActionResult(False, f"Unsupported chaos tool: {chaos_tool}")

    def _inject_with_chaos_mesh(
        self, target: str, duration: str, partition_type: str
    ) -> ActionResult:
        """Inject network partition using Chaos Mesh."""
        # This would require Chaos Mesh to be installed
        # For now, we'll simulate with a simplified approach

        chaos_manifest = f"""
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: network-partition-{int(time.time())}
  namespace: opensearch
spec:
  action: partition
  mode: fixed
  duration: {duration}
  selector:
    labelSelectors:
      app: opensearch
      opensearch.role/master: "true"
"""

        try:
            # Apply chaos manifest
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            ) as f:
                f.write(chaos_manifest)
                temp_file = f.name

            result = subprocess.run(
                ["kubectl", "apply", "-f", temp_file], capture_output=True, text=True
            )

            if result.returncode == 0:
                return ActionResult(True, f"Network partition injected for {duration}")
            else:
                return ActionResult(
                    False, f"Failed to inject network partition: {result.stderr}"
                )

        except Exception as e:
            return ActionResult(False, f"Failed to inject network partition: {e}")


class InjectResourcePressureAction(BaseAction):
    """Action to inject resource pressure."""

    action_name = "inject_resource_pressure"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        target = params.get("target", "data-nodes")
        resource = params.get("resource", "memory")
        limit = params.get("limit", "80%")
        duration = params.get("duration", "5m")
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)

        self.logger.info(
            f"Injecting {resource} pressure on {target} (limit: {limit}, duration: {duration})"
        )

        try:
            # Get target pods
            k8s_manager = KubernetesManager()
            if not k8s_manager.load_config():
                return ActionResult(False, "Failed to load Kubernetes config")

            label_selector = self._get_label_selector(target)
            pods = k8s_manager.get_pods(namespace, label_selector)

            if not pods:
                return ActionResult(False, f"No {target} pods found")

            # Apply resource pressure to each pod
            for pod in pods:
                self._apply_resource_pressure(
                    pod["name"], namespace, resource, limit, duration
                )

            return ActionResult(
                True, f"Applied {resource} pressure to {len(pods)} {target} pod(s)"
            )

        except Exception as e:
            return ActionResult(False, f"Failed to inject resource pressure: {e}")

    def _get_label_selector(self, target: str) -> str:
        """Get label selector for target type."""
        selectors = {
            "master-nodes": "opensearch.role/master=true",
            "data-nodes": "opensearch.role/data=true",
            "all-nodes": "app=opensearch",
        }
        return selectors.get(target, "app=opensearch")

    def _apply_resource_pressure(
        self, pod_name: str, namespace: str, resource: str, limit: str, duration: str
    ) -> bool:
        """Apply resource pressure to a pod."""
        try:
            if resource == "memory":
                # Use stress-ng or similar tool to consume memory
                cmd = [
                    "kubectl",
                    "exec",
                    pod_name,
                    "-n",
                    namespace,
                    "--",
                    "stress-ng",
                    "--vm",
                    "1",
                    "--vm-bytes",
                    limit,
                    "--timeout",
                    duration,
                ]
            elif resource == "cpu":
                # Use stress-ng to consume CPU
                cmd = [
                    "kubectl",
                    "exec",
                    pod_name,
                    "-n",
                    namespace,
                    "--",
                    "stress-ng",
                    "--cpu",
                    "2",
                    "--timeout",
                    duration,
                ]
            else:
                self.logger.warning(f"Unsupported resource type: {resource}")
                return False

            # Run in background
            subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return True

        except Exception as e:
            self.logger.error(f"Failed to apply resource pressure to {pod_name}: {e}")
            return False
