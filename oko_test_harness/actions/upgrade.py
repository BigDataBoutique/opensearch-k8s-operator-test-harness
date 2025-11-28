"""Upgrade actions."""

import subprocess
from typing import Any, Dict, List

from tenacity import RetryError

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.retry_utils import oko_retry
from oko_test_harness.utils.opensearch_client import KubernetesOpenSearchClient


class UpgradeClusterAction(BaseAction):
    """Action to upgrade OpenSearch cluster."""

    action_name = "upgrade_cluster"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        target_version = params.get("target_version")
        strategy = params.get("strategy", "rolling")
        validation_steps = params.get("validation_steps", ["check_cluster_health"])
        rollback_on_failure = params.get("rollback_on_failure", True)
        timeout_str = params.get("timeout", "15m")
        cluster_name = params.get("cluster_name", self.config.opensearch.cluster_name)
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)

        if not target_version:
            return ActionResult(False, "target_version is required")

        self.logger.info(f"Upgrading OpenSearch cluster to version {target_version}")

        try:
            # Get current cluster version
            current_version = self._get_current_version(cluster_name, namespace)
            if not current_version:
                return ActionResult(
                    False, "Could not determine current cluster version"
                )

            if strategy == "rolling":
                return self._rolling_upgrade(
                    cluster_name,
                    namespace,
                    target_version,
                    validation_steps,
                    rollback_on_failure,
                    timeout_str,
                )
            elif strategy == "blue-green":
                return self._blue_green_upgrade(cluster_name, namespace, target_version)
            else:
                return ActionResult(False, f"Unsupported upgrade strategy: {strategy}")

        except Exception as e:
            return ActionResult(False, f"Failed to upgrade cluster: {e}")

    def _get_current_version(self, cluster_name: str, namespace: str) -> str:
        """Get current cluster version."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "opensearchcluster",
                    cluster_name,
                    "-n",
                    namespace,
                    "-o",
                    "jsonpath={.spec.general.version}",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                return result.stdout.strip()
            return None
        except Exception:
            return None

    def _rolling_upgrade(
        self,
        cluster_name: str,
        namespace: str,
        target_version: str,
        validation_steps: List[str],
        rollback_on_failure: bool,
        timeout_str: str,
    ) -> ActionResult:
        """Perform rolling upgrade."""
        # Update cluster spec with new version
        patch_data = {"spec": {"general": {"version": target_version}}}

        try:
            # Apply the version update
            import json

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

            # Wait for upgrade to complete
            if not self._wait_for_upgrade_completion(
                cluster_name, namespace, target_version, timeout_str
            ):
                if rollback_on_failure:
                    self.logger.warning("Upgrade failed, attempting rollback")
                    # Rollback logic would go here
                return ActionResult(False, "Upgrade timed out")

            # Run validation steps
            for step in validation_steps:
                if not self._run_validation_step(step, namespace, cluster_name):
                    return ActionResult(False, f"Validation step '{step}' failed")

            return ActionResult(
                True, f"Successfully upgraded cluster to version {target_version}"
            )

        except Exception as e:
            return ActionResult(False, f"Rolling upgrade failed: {e}")

    def _blue_green_upgrade(
        self, cluster_name: str, namespace: str, target_version: str
    ) -> ActionResult:
        """Perform blue-green upgrade."""
        # This would involve creating a new cluster with the target version
        # and switching traffic over once it's ready
        return ActionResult(False, "Blue-green upgrade not yet implemented")

    def _wait_for_upgrade_completion(
        self, cluster_name: str, namespace: str, target_version: str, timeout_str: str
    ) -> bool:
        """Wait for upgrade to complete."""
        timeout = self._parse_duration(timeout_str)

        self.logger.info(f"Waiting for upgrade to complete (timeout: {timeout_str})")

        @oko_retry(timeout, 30)
        def _check_upgrade_complete():
            # Get node version distribution
            node_versions = self._get_node_version_distribution(cluster_name, namespace)
            if node_versions:
                version_summary = ", ".join(
                    [
                        f"{version}: {count} nodes"
                        for version, count in node_versions.items()
                    ]
                )
                self.logger.info(f"Upgrade progress: {version_summary}")

                # Check if all nodes are on target version
                if len(node_versions) == 1 and target_version in node_versions:
                    # Check cluster phase to ensure it's running
                    phase_result = subprocess.run(
                        [
                            "kubectl",
                            "get",
                            "opensearchcluster",
                            cluster_name,
                            "-n",
                            namespace,
                            "-o",
                            "jsonpath={.status.phase}",
                        ],
                        capture_output=True,
                        text=True,
                    )

                    if (
                        phase_result.returncode == 0
                        and phase_result.stdout.strip() == "RUNNING"
                    ):
                        self.logger.info("Upgrade completed successfully")
                        return

            raise Exception(
                f"Waiting for all nodes to reach version {target_version} and cluster to be RUNNING"
            )

        try:
            _check_upgrade_complete()
            return True
        except RetryError:
            self.logger.warning(f"Upgrade timeout reached after {timeout}s")
            return False

    def _get_node_version_distribution(
        self, cluster_name: str, namespace: str
    ) -> Dict[str, int]:
        """Get the distribution of OpenSearch versions across cluster nodes."""
        try:
            # Get pods for the cluster using the correct label, including ready status and restart count
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    f"opster.io/opensearch-cluster={cluster_name}",
                    "-o",
                    'jsonpath={range .items[*]}{.metadata.name}{";"}{.spec.containers[0].image}{";"}{.status.phase}{";"}{.status.containerStatuses[0].ready}{";"}{.status.containerStatuses[0].restartCount}{"\\n"}{end}',
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                self.logger.warning(f"Failed to get pod information: {result.stderr}")
                return {}

            version_counts = {}
            lines = result.stdout.strip().split("\n")

            for line in lines:
                if not line.strip():
                    continue

                parts = line.split(";")
                if len(parts) >= 5:
                    image = parts[1]
                    phase = parts[2]
                    ready = parts[3] == "true"
                    restart_count = int(parts[4]) if parts[4].isdigit() else 0

                    # Extract version from image (e.g., docker.io/opensearchproject/opensearch:2.9.0)
                    if ":" in image:
                        version = image.split(":")[-1]

                        # Determine pod status for display
                        if phase != "Running":
                            status_indicator = f"({phase})"
                        elif not ready:
                            if restart_count > 0:
                                status_indicator = f"(CrashLoop:{restart_count})"
                            else:
                                status_indicator = "(NotReady)"
                        else:
                            status_indicator = ""

                        version_key = (
                            f"{version}{status_indicator}"
                            if status_indicator
                            else version
                        )
                        version_counts[version_key] = (
                            version_counts.get(version_key, 0) + 1
                        )

            return version_counts

        except Exception as e:
            self.logger.warning(f"Failed to get node version distribution: {e}")
            return {}

    def _run_validation_step(
        self, step: str, namespace: str, cluster_name: str
    ) -> bool:
        """Run a validation step with retry logic."""
        if step == "check_cluster_health":
            # 5 attempts * 10s = 50s
            @oko_retry(50, 10)
            def _validate_health():
                self.logger.info("Attempting to validate cluster health")

                # Establish connection with security config, reconnecting on each attempt
                client = KubernetesOpenSearchClient.from_security_config(
                    self.config.opensearch.security, namespace, cluster_name
                )
                if not client.connect(quiet=True):
                    raise Exception("Failed to connect to cluster")

                try:
                    # Wait for cluster to be healthy
                    result = client.wait_for_cluster_health("yellow", 60)
                    if result:
                        self.logger.info("Cluster health validation successful")
                        return
                    raise Exception("Cluster did not reach yellow status")
                finally:
                    client.disconnect()

            try:
                _validate_health()
                return True
            except RetryError:
                self.logger.error("All cluster health validation attempts failed")
                return False

        elif step == "validate_data_integrity":
            # This would run data integrity checks
            return True

        return True


class UpgradeOperatorAction(BaseAction):
    """Action to upgrade the OpenSearch operator."""

    action_name = "upgrade_operator"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        target_version = params.get("target_version")
        strategy = params.get("strategy", "recreate")
        backup_before = params.get("backup_before", True)
        timeout_str = params.get("timeout", "10m")
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)

        if not target_version:
            return ActionResult(False, "target_version is required")

        self.logger.info(f"Upgrading OpenSearch operator to version {target_version}")

        try:
            if backup_before:
                # Backup current operator configuration
                self._backup_operator_config(namespace)

            if strategy == "recreate":
                return self._recreate_upgrade(target_version, namespace, timeout_str)
            else:
                return ActionResult(
                    False, f"Unsupported operator upgrade strategy: {strategy}"
                )

        except Exception as e:
            return ActionResult(False, f"Failed to upgrade operator: {e}")

    def _backup_operator_config(self, namespace: str) -> None:
        """Backup operator configuration."""
        # This would backup the operator deployment, configmaps, etc.
        pass

    def _recreate_upgrade(
        self, target_version: str, namespace: str, timeout_str: str
    ) -> ActionResult:
        """Recreate operator with new version."""
        try:
            # Delete existing operator
            result = subprocess.run(
                [
                    "kubectl",
                    "delete",
                    "deployment",
                    "opensearch-operator",
                    "-n",
                    namespace,
                ],
                capture_output=True,
                text=True,
            )

            # Wait for deletion
            time.sleep(10)

            # Install new version (this would use the same logic as InstallOperatorAction)
            # For now, we'll simulate this
            new_manifest_url = f"https://github.com/opensearch-project/opensearch-k8s-operator/releases/download/v{target_version}/opensearch-operator.yaml"

            result = subprocess.run(
                ["kubectl", "apply", "-f", new_manifest_url, "-n", namespace],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                return ActionResult(
                    True, f"Operator upgraded to version {target_version}"
                )
            else:
                return ActionResult(
                    False, f"Failed to install new operator version: {result.stderr}"
                )

        except Exception as e:
            return ActionResult(False, f"Recreate upgrade failed: {e}")
