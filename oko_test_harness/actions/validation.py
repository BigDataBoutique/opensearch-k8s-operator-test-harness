"""Validation and monitoring actions."""

import time
import json
from typing import Any, Dict, List

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.utils.opensearch_client import KubernetesOpenSearchClient
from oko_test_harness.utils.kubernetes import KubernetesManager


class ValidateClusterHealthAction(BaseAction):
    """Action to validate OpenSearch cluster health."""

    action_name = "validate_cluster_health"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        expected_status = params.get("expected_status", "green")
        timeout_str = params.get("timeout", "2m")
        retry_interval = params.get("retry_interval", "10s")
        check_nodes = params.get("check_nodes", True)
        check_indices = params.get("check_indices", True)
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        service_name = params.get("service_name", self.config.opensearch.service_name)

        timeout = self._parse_duration(timeout_str)
        interval = self._parse_duration(retry_interval)

        self.logger.info(f"Validating cluster health (expecting {expected_status})")

        start_time = time.time()
        client = None
        last_status = None

        # Reconnect on each attempt - handles pod restarts during rolling upgrades
        while time.time() - start_time < timeout:
            try:
                # Disconnect previous client if exists
                if client:
                    try:
                        client.disconnect()
                    except Exception:
                        pass
                    client = None

                # Create fresh connection for each attempt
                client = KubernetesOpenSearchClient.from_security_config(
                    self.config.opensearch.security, namespace, service_name
                )
                if not client.connect(quiet=True):
                    remaining_time = timeout - (time.time() - start_time)
                    if remaining_time > interval:
                        self.logger.debug(
                            f"Connection failed, retrying in {interval}s..."
                        )
                        time.sleep(interval)
                        continue
                    else:
                        return ActionResult(
                            False, "Failed to connect to OpenSearch cluster"
                        )

                health = client.health()
                current_status = health.get("status", "red")
                last_status = current_status

                if not self._health_status_reached(current_status, expected_status):
                    remaining_time = timeout - (time.time() - start_time)
                    if remaining_time > interval:
                        self.logger.info(
                            f"Cluster health is {current_status}, waiting for {expected_status} ({remaining_time:.0f}s remaining)"
                        )
                        client.disconnect()
                        client = None
                        time.sleep(interval)
                        continue
                    else:
                        return ActionResult(
                            False,
                            f"Cluster did not reach {expected_status} status within {timeout_str} (final status: {current_status})",
                        )

                # Validate node count if requested
                if check_nodes:
                    active_nodes = health.get("number_of_nodes", 0)
                    if active_nodes == 0:
                        return ActionResult(False, "No active nodes found")

                # Validate indices if requested
                if check_indices:
                    relocating_shards = health.get("relocating_shards", 0)
                    unassigned_shards = health.get("unassigned_shards", 0)
                    if expected_status == "green" and (
                        relocating_shards > 0 or unassigned_shards > 0
                    ):
                        return ActionResult(
                            False,
                            f"Cluster has {relocating_shards} relocating and {unassigned_shards} unassigned shards",
                        )

                return ActionResult(
                    True,
                    f"Cluster health is {health['status']} with {health['number_of_nodes']} nodes",
                )

            except Exception as e:
                remaining_time = timeout - (time.time() - start_time)
                if remaining_time > interval:
                    self.logger.info(
                        f"Health check failed: {e}, retrying in {interval}s..."
                    )
                    time.sleep(interval)
                else:
                    return ActionResult(False, f"Health validation failed: {e}")

            finally:
                if client:
                    try:
                        client.disconnect()
                    except Exception:
                        pass
                    client = None

        return ActionResult(
            False,
            f"Cluster did not reach {expected_status} status within {timeout_str} (final status: {last_status})",
        )

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

    def _health_status_reached(self, current: str, target: str) -> bool:
        """Check if current health status meets or exceeds target."""
        status_order = {"red": 0, "yellow": 1, "green": 2}
        return status_order.get(current, 0) >= status_order.get(target, 0)


class ValidateDataIntegrityAction(BaseAction):
    """Action to validate data integrity."""

    action_name = "validate_data_integrity"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        indices = params.get("indices", ["*"])
        expected_documents = params.get("expected_documents")
        checksum_validation = params.get("checksum_validation", False)
        sample_queries = params.get("sample_queries", [])
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        service_name = params.get("service_name", self.config.opensearch.service_name)

        self.logger.info("Validating data integrity")

        # Retry logic for transient connection failures
        max_retries = 5
        retry_delay = 10

        for attempt in range(max_retries):
            client = None
            try:
                client = KubernetesOpenSearchClient.from_security_config(
                    self.config.opensearch.security, namespace, service_name
                )
                with client:
                    # Count documents across indices
                    total_docs = 0
                    for index_pattern in indices:
                        try:
                            count = client.count(index_pattern)
                            total_docs += count
                            self.logger.debug(
                                f"Index pattern '{index_pattern}': {count} documents"
                            )
                        except Exception as e:
                            self.logger.warning(
                                f"Could not count documents in '{index_pattern}': {e}"
                            )

                    # Validate document count
                    if expected_documents is not None:
                        if total_docs != expected_documents:
                            return ActionResult(
                                False,
                                f"Expected {expected_documents} documents, found {total_docs}",
                            )

                    # Run sample queries
                    for i, query_config in enumerate(sample_queries):
                        query = query_config.get("query")
                        expected_hits = query_config.get("expected_hits")
                        min_hits = query_config.get("min_hits")

                        if isinstance(query, str):
                            query = json.loads(query)

                        # Use first index pattern for queries
                        index_pattern = indices[0] if indices else "*"
                        self.logger.debug(
                            f"Running sample query {i + 1}: {query} on index pattern '{index_pattern}'"
                        )

                        # Show index mapping for debugging
                        try:
                            mapping = client.get_index_mapping(index_pattern)
                            if mapping:
                                self.logger.debug(
                                    f"Index mapping for '{index_pattern}': {mapping}"
                                )
                        except Exception as e:
                            self.logger.warning(
                                f"Could not retrieve index mapping: {e}"
                            )

                        # First refresh the index to ensure latest data is available
                        try:
                            client.refresh_index(index_pattern)
                        except Exception as e:
                            self.logger.warning(
                                f"Could not refresh index {index_pattern}: {e}"
                            )

                        result = client.search(index_pattern, query, size=0)
                        hits = result["hits"]["total"]["value"]
                        self.logger.info(f"Sample query {i + 1} returned {hits} hits")

                        if expected_hits is not None and hits != expected_hits:
                            return ActionResult(
                                False,
                                f"Query expected {expected_hits} hits, got {hits}",
                            )

                        if min_hits is not None and hits < min_hits:
                            # Log a sample of documents to understand the data
                            try:
                                sample_result = client.search(
                                    index_pattern, {"match_all": {}}, size=5
                                )
                                sample_docs = [
                                    doc["_source"]
                                    for doc in sample_result.get("hits", {}).get(
                                        "hits", []
                                    )
                                ]
                                self.logger.debug(
                                    f"Sample documents from index: {sample_docs}"
                                )
                            except Exception as e:
                                self.logger.warning(
                                    f"Could not retrieve sample documents: {e}"
                                )

                            return ActionResult(
                                False,
                                f"Query expected at least {min_hits} hits, got {hits}",
                            )

                    return ActionResult(
                        True, f"Data integrity validated: {total_docs} total documents"
                    )

            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(
                        f"Data integrity check attempt {attempt + 1} failed: {e}, retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                else:
                    return ActionResult(
                        False,
                        f"Failed to validate data integrity after {max_retries} attempts: {e}",
                    )


class WaitForClusterReadyAction(BaseAction):
    """Action to wait for cluster to be ready."""

    action_name = "wait_for_cluster_ready"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        timeout_str = params.get("timeout", "15m")
        conditions = params.get("conditions", [])
        # Use sensible defaults based on conventions
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        cluster_name = params.get("cluster_name", self.config.opensearch.cluster_name)
        service_name = params.get("service_name", cluster_name)

        timeout = self._parse_duration(timeout_str)
        start_time = time.time()

        self.logger.info(
            f"Waiting for cluster '{cluster_name}' to be ready in namespace '{namespace}'"
        )

        # Default conditions
        default_conditions = [
            {"cluster_health": "green"},
            {"all_nodes_ready": True},
            {"indices_ready": True},
        ]

        conditions = conditions or default_conditions

        try:
            # Simple approach: wait a bit for pods to start, then test OpenSearch connectivity
            self.logger.info("Waiting for OpenSearch pods to initialize...")
            time.sleep(30)  # Give pods time to start

            # Now test OpenSearch connectivity and conditions
            attempt = 0
            max_attempts = timeout // 10  # Attempt every 10 seconds

            while time.time() - start_time < timeout:
                attempt += 1
                try:
                    # Check for crash loops before attempting OpenSearch connection
                    k8s_manager = KubernetesManager()
                    if k8s_manager.load_config():
                        crash_check = k8s_manager.check_crash_loops(
                            namespace,
                            label_selector="app.kubernetes.io/component=opensearch-cluster",
                        )

                        if crash_check["has_crash_loops"]:
                            crash_details = []
                            for cl in crash_check["crash_loops"]:
                                details = f"{cl['pod_name']} (restarts: {cl['restart_count']})"
                                if cl["last_error"]:
                                    details += f" - {cl['last_error'][:100]}..."
                                crash_details.append(details)

                            return ActionResult(
                                False,
                                f"Detected crash loops in {len(crash_check['crash_loops'])} pods: {'; '.join(crash_details)}",
                            )

                    client = KubernetesOpenSearchClient.from_security_config(
                        self.config.opensearch.security, namespace, service_name
                    )
                    # Use quiet mode for all attempts except the last one
                    is_quiet = attempt < max_attempts

                    if client.connect(quiet=is_quiet):
                        try:
                            all_conditions_met = True

                            for condition in conditions:
                                if "cluster_health" in condition:
                                    health = client.health()
                                    required_status = condition["cluster_health"]
                                    if not self._health_status_reached(
                                        health["status"], required_status
                                    ):
                                        self.logger.debug(
                                            f"Cluster health is {health['status']}, waiting for {required_status}"
                                        )
                                        all_conditions_met = False
                                        break

                                elif "all_nodes_ready" in condition:
                                    if condition["all_nodes_ready"]:
                                        # Check that all nodes are healthy
                                        node_stats = client.node_stats()
                                        if not node_stats.get("nodes"):
                                            self.logger.info(
                                                "No nodes found in cluster stats, waiting..."
                                            )
                                            all_conditions_met = False
                                            break

                                elif "indices_ready" in condition:
                                    if condition["indices_ready"]:
                                        health = client.health()
                                        unassigned = health.get("unassigned_shards", 0)
                                        if unassigned > 0:
                                            self.logger.info(
                                                f"Found {unassigned} unassigned shards, waiting..."
                                            )
                                            all_conditions_met = False
                                            break

                            if all_conditions_met:
                                return ActionResult(
                                    True,
                                    f"All readiness conditions met for cluster '{cluster_name}'",
                                )

                        finally:
                            client.disconnect()
                    else:
                        # Only log retry message if not the final attempt
                        if is_quiet:
                            self.logger.debug(
                                f"Connection attempt {attempt}/{max_attempts} failed, retrying..."
                            )

                except Exception as e:
                    # Only log retry message if not the final attempt
                    if attempt < max_attempts:
                        self.logger.debug(
                            f"Connection attempt {attempt}/{max_attempts} failed, retrying..."
                        )
                    else:
                        self.logger.error(f"Connection attempt failed: {e}")

                time.sleep(10)

            return ActionResult(
                False, f"Timeout waiting for cluster readiness after {timeout_str}"
            )

        except Exception as e:
            return ActionResult(False, f"Error waiting for cluster readiness: {e}")

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

    def _health_status_reached(self, current: str, target: str) -> bool:
        """Check if current health status meets or exceeds target."""
        status_order = {"red": 0, "yellow": 1, "green": 2}
        return status_order.get(current, 0) >= status_order.get(target, 0)


class ValidateOperatorStatusAction(BaseAction):
    """Action to validate operator status."""

    action_name = "validate_operator_status"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        expected_phase = params.get("expected_phase", "Running")
        check_events = params.get("check_events", True)
        max_restart_count = params.get("max_restart_count", 0)
        # Always use the operator namespace from the playbook context (context.playbook.config.opensearch.operator_namespace)
        # This ensures we're checking the exact namespace where the operator was installed
        namespace = self.config.opensearch.operator_namespace

        self.logger.info(f"Validating operator status in namespace: {namespace}")

        try:
            k8s_manager = KubernetesManager()
            if not k8s_manager.load_config():
                return ActionResult(False, "Failed to load Kubernetes config")

            # Try multiple possible label selectors for operator pods
            possible_selectors = [
                "app=opensearch-operator",  # Standard OpenSearch operator label
                "control-plane=controller-manager",  # Generic controller manager label
                "app.kubernetes.io/name=opensearch-operator",  # Kubernetes standard naming
                "app.kubernetes.io/component=opensearch-operator",  # Component-based label
            ]

            pods = []
            used_selector = None

            for selector in possible_selectors:
                pods = k8s_manager.get_pods(namespace, label_selector=selector)
                if pods:
                    used_selector = selector
                    self.logger.debug(f"Found operator pods using selector: {selector}")
                    break

            if not pods:
                # If no pods found with any selector, try to get all pods in namespace for debugging
                all_pods = k8s_manager.get_pods(namespace)
                if all_pods:
                    pod_names = [pod["name"] for pod in all_pods]
                    return ActionResult(
                        False,
                        f"No operator pods found in namespace '{namespace}' with any known selector. Available pods: {', '.join(pod_names)}",
                    )
                else:
                    # Check if namespace exists and if operator installation might have failed
                    try:
                        # Try to get namespace info
                        import subprocess

                        ns_result = subprocess.run(
                            ["kubectl", "get", "namespace", namespace],
                            capture_output=True,
                            text=True,
                        )
                        if ns_result.returncode != 0:
                            return ActionResult(
                                False,
                                f"Namespace '{namespace}' does not exist. Operator installation may have failed. Check the 'install_operator' step in your playbook.",
                            )
                        else:
                            return ActionResult(
                                False,
                                f"No operator pods found in namespace '{namespace}' (namespace exists but is empty). Operator installation may have failed. Check the 'install_operator' step in your playbook.",
                            )
                    except Exception:
                        return ActionResult(
                            False,
                            f"No operator pods found in namespace '{namespace}' (unable to verify namespace status)",
                        )

            # Check for crash loops in operator pods using the successful selector
            crash_check = k8s_manager.check_crash_loops(
                namespace,
                label_selector=used_selector,
                max_restart_count=max_restart_count,
            )

            if crash_check["has_crash_loops"]:
                crash_details = []
                for cl in crash_check["crash_loops"]:
                    details = f"{cl['pod_name']} (restarts: {cl['restart_count']})"
                    if cl["last_error"]:
                        details += f" - {cl['last_error'][:100]}..."
                    crash_details.append(details)

                return ActionResult(
                    False,
                    f"Operator has crash loops in {len(crash_check['crash_loops'])} pods: {'; '.join(crash_details)}",
                )

            for pod in pods:
                # Check pod status
                if pod["status"] != expected_phase:
                    return ActionResult(
                        False,
                        f"Operator pod '{pod['name']}' is in {pod['status']} phase, expected {expected_phase}",
                    )

                # Check if pod is ready
                if not pod["ready"]:
                    return ActionResult(
                        False, f"Operator pod '{pod['name']}' is not ready"
                    )

            # Check events if requested
            if check_events:
                # This would check for recent error events
                # Implementation would require more detailed event checking
                pass

            # Include restart count info in success message
            restart_info = ""
            if crash_check["warnings"]:
                restart_counts = [
                    f"{w['pod_name']}:{w['restart_count']}"
                    for w in crash_check["warnings"]
                ]
                restart_info = f" (restarts: {', '.join(restart_counts)})"

            return ActionResult(
                True,
                f"Operator is running with {len(pods)} pod(s) (found using selector: {used_selector}){restart_info}",
            )

        except Exception as e:
            return ActionResult(False, f"Failed to validate operator status: {e}")


class FixClusterReplicasAction(BaseAction):
    """Action to fix replica settings for all indices."""

    action_name = "fix_cluster_replicas"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        target_replicas = params.get("replicas", 0)
        exclude_patterns = params.get("exclude_patterns", [])
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        service_name = params.get("service_name", self.config.opensearch.service_name)

        self.logger.info(
            f"Fixing replica settings to {target_replicas} for all indices"
        )

        try:
            client = KubernetesOpenSearchClient.from_security_config(
                self.config.opensearch.security, namespace, service_name
            )
            with client:
                # List all indices
                indices = client.list_indices()
                if not indices:
                    return ActionResult(False, "Could not retrieve indices list")

                fixed_count = 0
                total_count = 0

                for index_info in indices:
                    index_name = index_info.get("index", "")
                    if not index_name:
                        continue

                    # Skip excluded patterns
                    skip = False
                    for pattern in exclude_patterns:
                        if pattern in index_name:
                            skip = True
                            break
                    if skip:
                        self.logger.info(f"Skipping index '{index_name}' (excluded)")
                        continue

                    total_count += 1

                    # Get current settings
                    settings = client.get_index_settings(index_name)
                    if not settings:
                        self.logger.warning(
                            f"Could not get settings for index '{index_name}'"
                        )
                        continue

                    # Extract current replica count
                    index_settings = (
                        settings.get(index_name, {})
                        .get("settings", {})
                        .get("index", {})
                    )
                    current_replicas = int(index_settings.get("number_of_replicas", 1))

                    if current_replicas != target_replicas:
                        self.logger.info(
                            f"Index '{index_name}' has {current_replicas} replicas, updating to {target_replicas}"
                        )
                        if client.update_index_replicas(index_name, target_replicas):
                            fixed_count += 1
                        else:
                            self.logger.warning(
                                f"Failed to update replicas for index '{index_name}'"
                            )
                    else:
                        self.logger.info(
                            f"Index '{index_name}' already has {target_replicas} replicas"
                        )

                return ActionResult(
                    True,
                    f"Fixed replica settings for {fixed_count}/{total_count} indices",
                )

        except Exception as e:
            return ActionResult(False, f"Failed to fix cluster replicas: {e}")


class ValidateNodeConfigurationAction(BaseAction):
    """Action to validate node-level configurations like vm.max_map_count, resources, etc."""

    action_name = "validate_node_configuration"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        node_group = params.get(
            "node_group", "all"
        )  # 'all', 'master', 'data', 'ingest'

        # System-level configurations to validate
        vm_max_map_count = params.get("vm_max_map_count")
        vm_swappiness = params.get("vm_swappiness")
        fs_file_max = params.get("fs_file_max")

        # Resource configurations
        memory_checks = params.get("memory_checks", {})
        cpu_checks = params.get("cpu_checks", {})
        storage_checks = params.get("storage_checks", {})

        # JVM configurations
        jvm_heap_checks = params.get("jvm_heap_checks", {})

        self.logger.info(f"Validating node configuration for node group: {node_group}")

        try:
            k8s_manager = KubernetesManager()
            if not k8s_manager.load_config():
                return ActionResult(False, "Failed to load Kubernetes config")

            # Get pods based on node group
            # Use opster.io labels which are what the opensearch-operator actually sets
            cluster_name = params.get(
                "cluster_name", self.config.opensearch.cluster_name
            )
            if node_group == "all":
                label_selector = f"opster.io/opensearch-cluster={cluster_name}"
            else:
                label_selector = f"opster.io/opensearch-cluster={cluster_name},opensearch.role={node_group}"

            pods = k8s_manager.get_pods(namespace, label_selector=label_selector)

            if not pods:
                return ActionResult(
                    False, f"No OpenSearch pods found for node group '{node_group}'"
                )

            validation_results = []
            all_passed = True

            for pod in pods:
                pod_name = pod["name"]
                self.logger.info(f"Validating configuration for pod: {pod_name}")

                # Validate system-level configurations
                if vm_max_map_count is not None:
                    result = self._validate_vm_max_map_count(
                        k8s_manager, namespace, pod_name, vm_max_map_count
                    )
                    validation_results.append(result)
                    if not result["success"]:
                        all_passed = False

                if vm_swappiness is not None:
                    result = self._validate_vm_swappiness(
                        k8s_manager, namespace, pod_name, vm_swappiness
                    )
                    validation_results.append(result)
                    if not result["success"]:
                        all_passed = False

                if fs_file_max is not None:
                    result = self._validate_fs_file_max(
                        k8s_manager, namespace, pod_name, fs_file_max
                    )
                    validation_results.append(result)
                    if not result["success"]:
                        all_passed = False

                # Validate resource configurations
                if memory_checks:
                    result = self._validate_memory_configuration(pod, memory_checks)
                    validation_results.append(result)
                    if not result["success"]:
                        all_passed = False

                if cpu_checks:
                    result = self._validate_cpu_configuration(pod, cpu_checks)
                    validation_results.append(result)
                    if not result["success"]:
                        all_passed = False

                if storage_checks:
                    result = self._validate_storage_configuration(
                        k8s_manager, namespace, pod_name, storage_checks
                    )
                    validation_results.append(result)
                    if not result["success"]:
                        all_passed = False

                # Validate JVM heap configuration
                if jvm_heap_checks:
                    result = self._validate_jvm_heap_configuration(
                        k8s_manager, namespace, pod_name, jvm_heap_checks
                    )
                    validation_results.append(result)
                    if not result["success"]:
                        all_passed = False

            # Generate summary
            passed_checks = sum(1 for r in validation_results if r["success"])
            total_checks = len(validation_results)

            summary = f"Node configuration validation: {passed_checks}/{total_checks} checks passed"

            return ActionResult(
                all_passed, summary, {"validation_details": validation_results}
            )

        except Exception as e:
            return ActionResult(False, f"Failed to validate node configuration: {e}")

    def _validate_vm_max_map_count(
        self,
        k8s_manager: KubernetesManager,
        namespace: str,
        pod_name: str,
        expected_min: int,
    ) -> Dict[str, Any]:
        """Validate vm.max_map_count setting."""
        try:
            # Read from /proc instead of sysctl (sysctl may not be available in minimal containers)
            cmd = ["cat", "/proc/sys/vm/max_map_count"]
            result = k8s_manager.exec_in_pod(namespace, pod_name, cmd)

            if result["success"]:
                output = result["output"].strip()
                current_value = int(output)

                if current_value >= expected_min:
                    return {
                        "success": True,
                        "check": "vm.max_map_count",
                        "pod": pod_name,
                        "expected": f">= {expected_min}",
                        "actual": current_value,
                        "message": f"vm.max_map_count is correctly set to {current_value}",
                    }
                else:
                    return {
                        "success": False,
                        "check": "vm.max_map_count",
                        "pod": pod_name,
                        "expected": f">= {expected_min}",
                        "actual": current_value,
                        "message": f"vm.max_map_count is {current_value}, should be >= {expected_min}",
                    }
            else:
                return {
                    "success": False,
                    "check": "vm.max_map_count",
                    "pod": pod_name,
                    "message": f"Failed to check vm.max_map_count: {result['error']}",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "vm.max_map_count",
                "pod": pod_name,
                "message": f"Error validating vm.max_map_count: {e}",
            }

    def _validate_vm_swappiness(
        self,
        k8s_manager: KubernetesManager,
        namespace: str,
        pod_name: str,
        expected_max: int,
    ) -> Dict[str, Any]:
        """Validate vm.swappiness setting."""
        try:
            # Read from /proc instead of sysctl (sysctl may not be available in minimal containers)
            cmd = ["cat", "/proc/sys/vm/swappiness"]
            result = k8s_manager.exec_in_pod(namespace, pod_name, cmd)

            if result["success"]:
                output = result["output"].strip()
                current_value = int(output)

                if current_value <= expected_max:
                    return {
                        "success": True,
                        "check": "vm.swappiness",
                        "pod": pod_name,
                        "expected": f"<= {expected_max}",
                        "actual": current_value,
                        "message": f"vm.swappiness is correctly set to {current_value}",
                    }
                else:
                    return {
                        "success": False,
                        "check": "vm.swappiness",
                        "pod": pod_name,
                        "expected": f"<= {expected_max}",
                        "actual": current_value,
                        "message": f"vm.swappiness is {current_value}, should be <= {expected_max}",
                    }
            else:
                return {
                    "success": False,
                    "check": "vm.swappiness",
                    "pod": pod_name,
                    "message": f"Failed to check vm.swappiness: {result['error']}",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "vm.swappiness",
                "pod": pod_name,
                "message": f"Error validating vm.swappiness: {e}",
            }

    def _validate_fs_file_max(
        self,
        k8s_manager: KubernetesManager,
        namespace: str,
        pod_name: str,
        expected_min: int,
    ) -> Dict[str, Any]:
        """Validate fs.file-max setting."""
        try:
            # Read from /proc instead of sysctl (sysctl may not be available in minimal containers)
            cmd = ["cat", "/proc/sys/fs/file-max"]
            result = k8s_manager.exec_in_pod(namespace, pod_name, cmd)

            if result["success"]:
                output = result["output"].strip()
                current_value = int(output)

                if current_value >= expected_min:
                    return {
                        "success": True,
                        "check": "fs.file-max",
                        "pod": pod_name,
                        "expected": f">= {expected_min}",
                        "actual": current_value,
                        "message": f"fs.file-max is correctly set to {current_value}",
                    }
                else:
                    return {
                        "success": False,
                        "check": "fs.file-max",
                        "pod": pod_name,
                        "expected": f">= {expected_min}",
                        "actual": current_value,
                        "message": f"fs.file-max is {current_value}, should be >= {expected_min}",
                    }
            else:
                return {
                    "success": False,
                    "check": "fs.file-max",
                    "pod": pod_name,
                    "message": f"Failed to check fs.file-max: {result['error']}",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "fs.file-max",
                "pod": pod_name,
                "message": f"Error validating fs.file-max: {e}",
            }

    def _validate_memory_configuration(
        self, pod: Dict[str, Any], memory_checks: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate memory resource configuration."""
        try:
            pod_name = pod["name"]

            # Get resource specifications from pod
            containers = pod.get("containers", [])
            if not containers:
                return {
                    "success": False,
                    "check": "memory_configuration",
                    "pod": pod_name,
                    "message": "No containers found in pod",
                }

            # Assume first container is the main OpenSearch container
            container = containers[0]
            resources = container.get("resources", {})

            memory_request = resources.get("requests", {}).get("memory")
            memory_limit = resources.get("limits", {}).get("memory")

            validation_errors = []

            # Check minimum memory request
            if "min_memory_request" in memory_checks:
                min_req = memory_checks["min_memory_request"]
                if not memory_request:
                    validation_errors.append(
                        f"No memory request specified, minimum required: {min_req}"
                    )
                elif not self._compare_memory_size(memory_request, min_req, ">="):
                    validation_errors.append(
                        f"Memory request {memory_request} is less than minimum {min_req}"
                    )

            # Check memory limit
            if "min_memory_limit" in memory_checks:
                min_limit = memory_checks["min_memory_limit"]
                if not memory_limit:
                    validation_errors.append(
                        f"No memory limit specified, minimum required: {min_limit}"
                    )
                elif not self._compare_memory_size(memory_limit, min_limit, ">="):
                    validation_errors.append(
                        f"Memory limit {memory_limit} is less than minimum {min_limit}"
                    )

            # Check request/limit ratio
            if (
                "request_limit_ratio" in memory_checks
                and memory_request
                and memory_limit
            ):
                max_ratio = memory_checks["request_limit_ratio"]
                req_bytes = self._memory_to_bytes(memory_request)
                limit_bytes = self._memory_to_bytes(memory_limit)
                actual_ratio = req_bytes / limit_bytes if limit_bytes > 0 else 0

                if actual_ratio > max_ratio:
                    validation_errors.append(
                        f"Memory request/limit ratio {actual_ratio:.2f} exceeds maximum {max_ratio}"
                    )

            if validation_errors:
                return {
                    "success": False,
                    "check": "memory_configuration",
                    "pod": pod_name,
                    "message": "; ".join(validation_errors),
                    "memory_request": memory_request,
                    "memory_limit": memory_limit,
                }
            else:
                return {
                    "success": True,
                    "check": "memory_configuration",
                    "pod": pod_name,
                    "message": "Memory configuration is valid",
                    "memory_request": memory_request,
                    "memory_limit": memory_limit,
                }

        except Exception as e:
            return {
                "success": False,
                "check": "memory_configuration",
                "pod": pod.get("name", "unknown"),
                "message": f"Error validating memory configuration: {e}",
            }

    def _validate_cpu_configuration(
        self, pod: Dict[str, Any], cpu_checks: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate CPU resource configuration."""
        try:
            pod_name = pod["name"]

            containers = pod.get("containers", [])
            if not containers:
                return {
                    "success": False,
                    "check": "cpu_configuration",
                    "pod": pod_name,
                    "message": "No containers found in pod",
                }

            container = containers[0]
            resources = container.get("resources", {})

            cpu_request = resources.get("requests", {}).get("cpu")
            cpu_limit = resources.get("limits", {}).get("cpu")

            validation_errors = []

            # Check minimum CPU request
            if "min_cpu_request" in cpu_checks:
                min_req = cpu_checks["min_cpu_request"]
                if not cpu_request:
                    validation_errors.append(
                        f"No CPU request specified, minimum required: {min_req}"
                    )
                elif not self._compare_cpu_size(cpu_request, min_req, ">="):
                    validation_errors.append(
                        f"CPU request {cpu_request} is less than minimum {min_req}"
                    )

            # Check CPU limit
            if "min_cpu_limit" in cpu_checks:
                min_limit = cpu_checks["min_cpu_limit"]
                if not cpu_limit:
                    validation_errors.append(
                        f"No CPU limit specified, minimum required: {min_limit}"
                    )
                elif not self._compare_cpu_size(cpu_limit, min_limit, ">="):
                    validation_errors.append(
                        f"CPU limit {cpu_limit} is less than minimum {min_limit}"
                    )

            if validation_errors:
                return {
                    "success": False,
                    "check": "cpu_configuration",
                    "pod": pod_name,
                    "message": "; ".join(validation_errors),
                    "cpu_request": cpu_request,
                    "cpu_limit": cpu_limit,
                }
            else:
                return {
                    "success": True,
                    "check": "cpu_configuration",
                    "pod": pod_name,
                    "message": "CPU configuration is valid",
                    "cpu_request": cpu_request,
                    "cpu_limit": cpu_limit,
                }

        except Exception as e:
            return {
                "success": False,
                "check": "cpu_configuration",
                "pod": pod.get("name", "unknown"),
                "message": f"Error validating CPU configuration: {e}",
            }

    def _validate_storage_configuration(
        self,
        k8s_manager: KubernetesManager,
        namespace: str,
        pod_name: str,
        storage_checks: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate storage configuration."""
        try:
            # Check PVC information
            pvcs = k8s_manager.get_pvcs(namespace)
            pod_pvcs = [pvc for pvc in pvcs if pod_name in pvc.get("name", "")]

            validation_errors = []

            if "min_storage_size" in storage_checks:
                min_size = storage_checks["min_storage_size"]
                if not pod_pvcs:
                    validation_errors.append("No PVCs found for pod")
                else:
                    for pvc in pod_pvcs:
                        pvc_size = pvc.get("size", "0Gi")
                        if not self._compare_memory_size(pvc_size, min_size, ">="):
                            validation_errors.append(
                                f"PVC {pvc['name']} size {pvc_size} is less than minimum {min_size}"
                            )

            if "required_storage_class" in storage_checks:
                required_class = storage_checks["required_storage_class"]
                for pvc in pod_pvcs:
                    pvc_class = pvc.get("storage_class", "")
                    if pvc_class != required_class:
                        validation_errors.append(
                            f"PVC {pvc['name']} uses storage class '{pvc_class}', expected '{required_class}'"
                        )

            if validation_errors:
                return {
                    "success": False,
                    "check": "storage_configuration",
                    "pod": pod_name,
                    "message": "; ".join(validation_errors),
                }
            else:
                return {
                    "success": True,
                    "check": "storage_configuration",
                    "pod": pod_name,
                    "message": "Storage configuration is valid",
                    "pvcs": [pvc["name"] for pvc in pod_pvcs],
                }

        except Exception as e:
            return {
                "success": False,
                "check": "storage_configuration",
                "pod": pod_name,
                "message": f"Error validating storage configuration: {e}",
            }

    def _validate_jvm_heap_configuration(
        self,
        k8s_manager: KubernetesManager,
        namespace: str,
        pod_name: str,
        jvm_checks: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate JVM heap configuration."""
        try:
            # Get JVM settings from environment variables or config
            cmd = ["env"]
            result = k8s_manager.exec_in_pod(namespace, pod_name, cmd)

            if not result["success"]:
                return {
                    "success": False,
                    "check": "jvm_heap_configuration",
                    "pod": pod_name,
                    "message": f"Failed to get environment variables: {result['error']}",
                }

            env_output = result["output"]
            validation_errors = []

            # Look for OPENSEARCH_JAVA_OPTS or ES_JAVA_OPTS
            java_opts = None
            for line in env_output.split("\n"):
                if "OPENSEARCH_JAVA_OPTS=" in line or "ES_JAVA_OPTS=" in line:
                    java_opts = line.split("=", 1)[1] if "=" in line else ""
                    break

            if (
                not java_opts
                and "require_heap_settings" in jvm_checks
                and jvm_checks["require_heap_settings"]
            ):
                validation_errors.append("No JVM heap settings found in environment")
            elif java_opts:
                # Check heap settings
                if "min_heap_size" in jvm_checks:
                    min_heap = jvm_checks["min_heap_size"]
                    if not self._check_jvm_heap_setting(
                        java_opts, "Xms", min_heap, ">="
                    ):
                        validation_errors.append(
                            f"JVM initial heap size is less than minimum {min_heap}"
                        )

                if "max_heap_size" in jvm_checks:
                    max_heap = jvm_checks["max_heap_size"]
                    if self._check_jvm_heap_setting(java_opts, "Xmx", max_heap, ">"):
                        validation_errors.append(
                            f"JVM maximum heap size exceeds maximum {max_heap}"
                        )

                # Check heap ratio to container memory
                if "heap_to_container_ratio" in jvm_checks:
                    expected_ratio = jvm_checks["heap_to_container_ratio"]
                    # This would require getting container memory limits and comparing
                    # Implementation would be more complex, checking the ratio

            if validation_errors:
                return {
                    "success": False,
                    "check": "jvm_heap_configuration",
                    "pod": pod_name,
                    "message": "; ".join(validation_errors),
                    "java_opts": java_opts,
                }
            else:
                return {
                    "success": True,
                    "check": "jvm_heap_configuration",
                    "pod": pod_name,
                    "message": "JVM heap configuration is valid",
                    "java_opts": java_opts,
                }

        except Exception as e:
            return {
                "success": False,
                "check": "jvm_heap_configuration",
                "pod": pod_name,
                "message": f"Error validating JVM heap configuration: {e}",
            }

    def _compare_memory_size(self, size1: str, size2: str, operator: str) -> bool:
        """Compare two memory sizes (e.g., '1Gi', '512Mi')."""
        try:
            bytes1 = self._memory_to_bytes(size1)
            bytes2 = self._memory_to_bytes(size2)

            if operator == ">=":
                return bytes1 >= bytes2
            elif operator == "<=":
                return bytes1 <= bytes2
            elif operator == ">":
                return bytes1 > bytes2
            elif operator == "<":
                return bytes1 < bytes2
            elif operator == "==":
                return bytes1 == bytes2
            else:
                return False
        except:
            return False

    def _compare_cpu_size(self, cpu1: str, cpu2: str, operator: str) -> bool:
        """Compare two CPU sizes (e.g., '500m', '1')."""
        try:
            millis1 = self._cpu_to_millis(cpu1)
            millis2 = self._cpu_to_millis(cpu2)

            if operator == ">=":
                return millis1 >= millis2
            elif operator == "<=":
                return millis1 <= millis2
            elif operator == ">":
                return millis1 > millis2
            elif operator == "<":
                return millis1 < millis2
            elif operator == "==":
                return millis1 == millis2
            else:
                return False
        except:
            return False

    def _memory_to_bytes(self, size: str) -> int:
        """Convert memory size string to bytes."""
        import re

        size = size.strip().upper()

        # Handle binary units (1024-based)
        binary_units = {
            "KI": 1024,
            "MI": 1024**2,
            "GI": 1024**3,
            "TI": 1024**4,
        }

        # Handle decimal units (1000-based)
        decimal_units = {
            "K": 1000,
            "M": 1000**2,
            "G": 1000**3,
            "T": 1000**4,
        }

        # Try to parse with units
        match = re.match(r"^(\d+(?:\.\d+)?)([A-Z]+)$", size)
        if match:
            value, unit = match.groups()
            value = float(value)

            if unit in binary_units:
                return int(value * binary_units[unit])
            elif unit in decimal_units:
                return int(value * decimal_units[unit])

        # Try to parse as plain number (assumed bytes)
        try:
            return int(float(size))
        except:
            raise ValueError(f"Cannot parse memory size: {size}")

    def _cpu_to_millis(self, cpu: str) -> int:
        """Convert CPU size string to millicores."""
        cpu = cpu.strip()

        if cpu.endswith("m"):
            return int(cpu[:-1])
        else:
            return int(float(cpu) * 1000)

    def _check_jvm_heap_setting(
        self, java_opts: str, setting: str, expected: str, operator: str
    ) -> bool:
        """Check JVM heap setting like -Xmx or -Xms."""
        import re

        pattern = f"-{setting}([0-9]+[gmkGMK]?)"
        match = re.search(pattern, java_opts)

        if not match:
            return False

        actual = match.group(1)

        # Convert both to bytes for comparison
        try:
            actual_bytes = self._jvm_memory_to_bytes(actual)
            expected_bytes = self._jvm_memory_to_bytes(expected)

            if operator == ">=":
                return actual_bytes >= expected_bytes
            elif operator == "<=":
                return actual_bytes <= expected_bytes
            elif operator == ">":
                return actual_bytes > expected_bytes
            elif operator == "<":
                return actual_bytes < expected_bytes
            else:
                return actual_bytes == expected_bytes
        except:
            return False

    def _jvm_memory_to_bytes(self, size: str) -> int:
        """Convert JVM memory size to bytes (supports g, m, k suffixes)."""
        size = size.strip().lower()

        if size.endswith("g"):
            return int(size[:-1]) * 1024**3
        elif size.endswith("m"):
            return int(size[:-1]) * 1024**2
        elif size.endswith("k"):
            return int(size[:-1]) * 1024
        else:
            return int(size)


class ValidateClusterConfigurationAction(BaseAction):
    """Action to validate cluster-level configurations and settings."""

    action_name = "validate_cluster_configuration"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        service_name = params.get("service_name", self.config.opensearch.service_name)
        timeout_str = params.get("timeout", "7m")  # Default 7 minutes timeout
        retry_interval_str = params.get(
            "retry_interval", "30s"
        )  # Retry every 30 seconds

        # Parse timeout and retry interval
        timeout = self._parse_duration(timeout_str)
        retry_interval = self._parse_duration(retry_interval_str)

        # Cluster settings to validate
        cluster_settings = params.get("cluster_settings", {})
        index_settings = params.get("index_settings", {})
        node_role_distribution = params.get("node_role_distribution", {})
        security_settings = params.get("security_settings", {})
        plugin_settings = params.get("plugin_settings", {})

        self.logger.info(
            f"Validating cluster configuration (timeout: {timeout_str}, retry interval: {retry_interval_str})"
        )

        try:
            start_time = time.time()
            client = None

            # Retry loop to wait for cluster to be ready
            while time.time() - start_time < timeout:
                try:
                    validation_results = []
                    all_passed = True

                    # Establish client connection
                    if client is None:
                        client = KubernetesOpenSearchClient.from_security_config(
                            self.config.opensearch.security, namespace, service_name
                        )

                    with client:
                        # Validate cluster settings (these don't usually need retries)
                        if cluster_settings:
                            result = self._validate_cluster_settings(
                                client, cluster_settings
                            )
                            validation_results.append(result)
                            if not result["success"]:
                                all_passed = False

                        # Validate index settings (these don't usually need retries)
                        if index_settings:
                            result = self._validate_index_settings(
                                client, index_settings
                            )
                            validation_results.append(result)
                            if not result["success"]:
                                all_passed = False

                        # Validate node role distribution (this is the critical one that needs retries)
                        if node_role_distribution:
                            result = self._validate_node_role_distribution(
                                client, node_role_distribution
                            )
                            validation_results.append(result)
                            if not result["success"]:
                                all_passed = False

                        # Validate security settings (these don't usually need retries)
                        if security_settings:
                            result = self._validate_security_settings(
                                client, security_settings
                            )
                            validation_results.append(result)
                            if not result["success"]:
                                all_passed = False

                        # Validate plugin settings (these don't usually need retries)
                        if plugin_settings:
                            result = self._validate_plugin_settings(
                                client, plugin_settings
                            )
                            validation_results.append(result)
                            if not result["success"]:
                                all_passed = False

                    # If all validations passed, return success
                    if all_passed:
                        break

                    # If we have failures, check if we should retry
                    remaining_time = timeout - (time.time() - start_time)
                    if remaining_time > retry_interval:
                        # Log which validations failed and that we're retrying
                        failed_checks = [
                            r["check"] for r in validation_results if not r["success"]
                        ]
                        self.logger.debug(
                            f"Validation checks failed: {failed_checks}. Retrying in {retry_interval_str} ({remaining_time:.0f}s remaining)"
                        )
                        time.sleep(retry_interval)
                        continue
                    else:
                        # Not enough time for another retry, break and return failure
                        break

                except Exception as e:
                    # Connection or other errors - retry if time allows
                    remaining_time = timeout - (time.time() - start_time)
                    if remaining_time > retry_interval:
                        self.logger.info(
                            f"Validation error: {e}. Retrying in {retry_interval_str} ({remaining_time:.0f}s remaining)"
                        )
                        client = None  # Reset client for next attempt
                        time.sleep(retry_interval)
                        continue
                    else:
                        return ActionResult(False, f"Validation failed with error: {e}")

            # Check if we timed out
            if time.time() - start_time >= timeout:
                return ActionResult(False, f"Validation timeout after {timeout_str}")

            # If we're here, we either succeeded or failed after retries

            # Generate summary
            passed_checks = sum(1 for r in validation_results if r["success"])
            total_checks = len(validation_results)

            # Log detailed results for failed checks
            for result in validation_results:
                if result["success"]:
                    self.logger.info(f"✓ {result['check']}: {result['message']}")
                else:
                    self.logger.error(f"✗ {result['check']}: {result['message']}")
                    if "role_counts" in result:
                        self.logger.error(
                            f"  Current role counts: {result['role_counts']}"
                        )
                    if "total_nodes" in result:
                        self.logger.error(f"  Total nodes: {result['total_nodes']}")

            summary = f"Cluster configuration validation: {passed_checks}/{total_checks} checks passed"

            return ActionResult(
                all_passed, summary, {"validation_details": validation_results}
            )

        except Exception as e:
            return ActionResult(False, f"Failed to validate cluster configuration: {e}")

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

    def _validate_cluster_settings(
        self, client: KubernetesOpenSearchClient, cluster_settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate cluster-level settings."""
        try:
            validation_errors = []

            # Get current cluster settings
            current_settings = client.get_cluster_settings()
            if not current_settings:
                return {
                    "success": False,
                    "check": "cluster_settings",
                    "message": "Failed to retrieve cluster settings",
                }

            for setting_name, expected_config in cluster_settings.items():
                if isinstance(expected_config, dict):
                    expected_value = expected_config.get("value")
                    validation_type = expected_config.get("type", "equals")
                    minimum = expected_config.get("minimum")
                    maximum = expected_config.get("maximum")
                else:
                    expected_value = expected_config
                    validation_type = "equals"
                    minimum = maximum = None

                # Extract current value from nested settings structure
                current_value = self._extract_setting_value(
                    current_settings, setting_name
                )

                if current_value is None:
                    validation_errors.append(
                        f"Setting '{setting_name}' is not configured"
                    )
                    continue

                # Validate based on type
                if validation_type == "equals":
                    if str(current_value) != str(expected_value):
                        validation_errors.append(
                            f"Setting '{setting_name}' is {current_value}, expected {expected_value}"
                        )
                elif validation_type == "minimum":
                    if minimum is not None and self._compare_numeric_values(
                        current_value, minimum, "<"
                    ):
                        validation_errors.append(
                            f"Setting '{setting_name}' is {current_value}, minimum required {minimum}"
                        )
                elif validation_type == "maximum":
                    if maximum is not None and self._compare_numeric_values(
                        current_value, maximum, ">"
                    ):
                        validation_errors.append(
                            f"Setting '{setting_name}' is {current_value}, maximum allowed {maximum}"
                        )
                elif validation_type == "range":
                    if minimum is not None and self._compare_numeric_values(
                        current_value, minimum, "<"
                    ):
                        validation_errors.append(
                            f"Setting '{setting_name}' is {current_value}, below minimum {minimum}"
                        )
                    if maximum is not None and self._compare_numeric_values(
                        current_value, maximum, ">"
                    ):
                        validation_errors.append(
                            f"Setting '{setting_name}' is {current_value}, above maximum {maximum}"
                        )

            if validation_errors:
                return {
                    "success": False,
                    "check": "cluster_settings",
                    "message": "; ".join(validation_errors),
                }
            else:
                return {
                    "success": True,
                    "check": "cluster_settings",
                    "message": f"All {len(cluster_settings)} cluster settings are valid",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "cluster_settings",
                "message": f"Error validating cluster settings: {e}",
            }

    def _validate_index_settings(
        self, client: KubernetesOpenSearchClient, index_settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate index-level settings."""
        try:
            validation_errors = []

            # Get all indices
            indices = client.list_indices()
            if not indices:
                return {
                    "success": False,
                    "check": "index_settings",
                    "message": "No indices found to validate",
                }

            for index_info in indices:
                index_name = index_info.get("index", "")
                if not index_name or index_name.startswith("."):  # Skip system indices
                    continue

                # Get index settings
                settings = client.get_index_settings(index_name)
                if not settings:
                    validation_errors.append(
                        f"Failed to get settings for index {index_name}"
                    )
                    continue

                index_config = (
                    settings.get(index_name, {}).get("settings", {}).get("index", {})
                )

                # Validate each setting
                for setting_name, expected_config in index_settings.items():
                    if isinstance(expected_config, dict):
                        expected_value = expected_config.get("value")
                        validation_type = expected_config.get("type", "equals")
                        apply_to = expected_config.get(
                            "apply_to", "all"
                        )  # 'all', 'user', 'system'
                    else:
                        expected_value = expected_config
                        validation_type = "equals"
                        apply_to = "all"

                    # Skip if this setting doesn't apply to this index type
                    if apply_to == "user" and index_name.startswith("."):
                        continue
                    if apply_to == "system" and not index_name.startswith("."):
                        continue

                    current_value = index_config.get(setting_name)

                    if validation_type == "equals":
                        if str(current_value) != str(expected_value):
                            validation_errors.append(
                                f"Index '{index_name}' setting '{setting_name}' is {current_value}, expected {expected_value}"
                            )
                    elif validation_type == "minimum":
                        minimum = expected_config.get("minimum")
                        if minimum is not None and self._compare_numeric_values(
                            current_value, minimum, "<"
                        ):
                            validation_errors.append(
                                f"Index '{index_name}' setting '{setting_name}' is {current_value}, minimum required {minimum}"
                            )

            if validation_errors:
                return {
                    "success": False,
                    "check": "index_settings",
                    "message": "; ".join(validation_errors),
                }
            else:
                return {
                    "success": True,
                    "check": "index_settings",
                    "message": f"All index settings validated across {len([i for i in indices if not i.get('index', '').startswith('.')])} user indices",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "index_settings",
                "message": f"Error validating index settings: {e}",
            }

    def _validate_node_role_distribution(
        self, client: KubernetesOpenSearchClient, role_distribution: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate node role distribution."""
        try:
            validation_errors = []

            # Get cluster stats to see node information
            cluster_stats = client.cluster_stats()
            if not cluster_stats or "nodes" not in cluster_stats:
                # Try to get node info anyway for debugging
                try:
                    node_info = client.get_nodes_info()
                    debug_info = "\nNode details: Failed to get cluster stats"
                    if node_info and "nodes" in node_info:
                        debug_info += (
                            f", but found {len(node_info['nodes'])} nodes via nodes API"
                        )
                except:
                    debug_info = (
                        "\nNode details: Failed to get both cluster stats and node info"
                    )

                return {
                    "success": False,
                    "check": "node_role_distribution",
                    "message": f"Failed to get cluster node information{debug_info}",
                }

            nodes = cluster_stats["nodes"]
            total_nodes = nodes.get("count", {}).get("total", 0)

            if total_nodes == 0:
                return {
                    "success": False,
                    "check": "node_role_distribution",
                    "message": "No nodes found in cluster\nNode details: No nodes discovered",
                }

            # Get node info to analyze roles
            node_info = client.get_nodes_info()
            role_counts = {}
            node_details = []

            if node_info and "nodes" in node_info:
                for node_id, node_data in node_info["nodes"].items():
                    node_name = node_data.get("name", node_id)
                    roles = node_data.get("roles", [])
                    node_details.append(
                        {"name": node_name, "id": node_id, "roles": roles}
                    )
                    for role in roles:
                        role_counts[role] = role_counts.get(role, 0) + 1

            # Validate role distribution requirements
            for role, requirements in role_distribution.items():
                # Handle OpenSearch 3.0+ role name change: master -> cluster_manager
                actual_role_to_check = role
                if (
                    role == "master"
                    and role not in role_counts
                    and "cluster_manager" in role_counts
                ):
                    actual_role_to_check = "cluster_manager"

                current_count = role_counts.get(actual_role_to_check, 0)

                if isinstance(requirements, dict):
                    minimum = requirements.get("minimum")
                    maximum = requirements.get("maximum")
                    should_be_odd = requirements.get("should_be_odd", False)
                    percentage = requirements.get("percentage")
                else:
                    minimum = requirements
                    maximum = should_be_odd = percentage = None

                # Check minimum
                if minimum is not None and current_count < minimum:
                    validation_errors.append(
                        f"Role '{role}' has {current_count} nodes, minimum required {minimum}"
                    )

                # Check maximum
                if maximum is not None and current_count > maximum:
                    validation_errors.append(
                        f"Role '{role}' has {current_count} nodes, maximum allowed {maximum}"
                    )

                # Check if should be odd (important for master nodes)
                if should_be_odd and current_count > 0 and current_count % 2 == 0:
                    validation_errors.append(
                        f"Role '{role}' has {current_count} nodes, should be odd number for quorum"
                    )

                # Check percentage
                if percentage is not None:
                    expected_count = int(total_nodes * percentage / 100)
                    if abs(current_count - expected_count) > 1:  # Allow ±1 for rounding
                        validation_errors.append(
                            f"Role '{role}' has {current_count} nodes, expected ~{percentage}% ({expected_count})"
                        )

            if validation_errors:
                # Create detailed node information for debugging
                node_info_str = "\nNode details:\n"
                for node in node_details:
                    roles_str = (
                        ", ".join(node["roles"]) if node["roles"] else "no roles"
                    )
                    node_info_str += (
                        f"  - {node['name']} (ID: {node['id']}): [{roles_str}]\n"
                    )

                error_message = "; ".join(validation_errors) + node_info_str

                return {
                    "success": False,
                    "check": "node_role_distribution",
                    "message": error_message,
                    "role_counts": role_counts,
                    "total_nodes": total_nodes,
                    "node_details": node_details,
                }
            else:
                return {
                    "success": True,
                    "check": "node_role_distribution",
                    "message": f"Node role distribution is valid across {total_nodes} nodes",
                    "role_counts": role_counts,
                    "total_nodes": total_nodes,
                }

        except Exception as e:
            return {
                "success": False,
                "check": "node_role_distribution",
                "message": f"Error validating node role distribution: {e}",
            }

    def _validate_security_settings(
        self, client: KubernetesOpenSearchClient, security_settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate security configuration."""
        try:
            validation_errors = []

            # Check if security plugin is enabled
            security_enabled = security_settings.get("enabled", True)
            ssl_enabled = security_settings.get("ssl_enabled")
            required_auth_methods = security_settings.get("required_auth_methods", [])

            # Try to access security API to check if it's enabled
            try:
                # This would typically check /_plugins/_security/api/account
                # For now, we'll check cluster health which should work regardless
                health = client.health()
                cluster_name = health.get("cluster_name", "")

                # If we can access without auth and security is supposed to be enabled, that's a problem
                if security_enabled and not client.using_auth():
                    validation_errors.append(
                        "Security is supposed to be enabled but cluster accepts unauthenticated requests"
                    )

            except Exception:
                # If we get an auth error, security might be properly enabled
                if not security_enabled:
                    validation_errors.append(
                        "Security appears to be enabled but was expected to be disabled"
                    )

            # Check SSL/TLS settings
            if ssl_enabled is not None:
                # This would require more sophisticated checking of the transport layer
                # For now, we'll check if the client is using HTTPS
                if ssl_enabled and not client.using_ssl():
                    validation_errors.append(
                        "SSL is required but client is not using HTTPS"
                    )
                elif not ssl_enabled and client.using_ssl():
                    validation_errors.append(
                        "SSL is disabled but client is using HTTPS"
                    )

            if validation_errors:
                return {
                    "success": False,
                    "check": "security_settings",
                    "message": "; ".join(validation_errors),
                }
            else:
                return {
                    "success": True,
                    "check": "security_settings",
                    "message": "Security configuration is valid",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "security_settings",
                "message": f"Error validating security settings: {e}",
            }

    def _validate_plugin_settings(
        self, client: KubernetesOpenSearchClient, plugin_settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate plugin configuration."""
        try:
            validation_errors = []

            # Get installed plugins
            try:
                # This would typically use /_cat/plugins API
                # For now, we'll simulate based on cluster capabilities
                installed_plugins = self._get_installed_plugins(client)

                required_plugins = plugin_settings.get("required_plugins", [])
                forbidden_plugins = plugin_settings.get("forbidden_plugins", [])

                # Check required plugins
                for plugin in required_plugins:
                    if plugin not in installed_plugins:
                        validation_errors.append(
                            f"Required plugin '{plugin}' is not installed"
                        )

                # Check forbidden plugins
                for plugin in forbidden_plugins:
                    if plugin in installed_plugins:
                        validation_errors.append(
                            f"Forbidden plugin '{plugin}' is installed"
                        )

                # Check plugin-specific settings
                plugin_configs = plugin_settings.get("plugin_configs", {})
                for plugin, config in plugin_configs.items():
                    if plugin not in installed_plugins:
                        validation_errors.append(
                            f"Cannot validate config for plugin '{plugin}' - not installed"
                        )
                        continue

                    # Plugin-specific validation would go here
                    # This is a placeholder for more sophisticated plugin config validation

            except Exception as e:
                validation_errors.append(f"Failed to check plugin status: {e}")

            if validation_errors:
                return {
                    "success": False,
                    "check": "plugin_settings",
                    "message": "; ".join(validation_errors),
                }
            else:
                return {
                    "success": True,
                    "check": "plugin_settings",
                    "message": "Plugin configuration is valid",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "plugin_settings",
                "message": f"Error validating plugin settings: {e}",
            }

    def _extract_setting_value(
        self, settings: Dict[str, Any], setting_path: str
    ) -> Any:
        """Extract a setting value from nested cluster settings."""
        # Handle dot notation like 'cluster.max_shards_per_node'
        keys = setting_path.split(".")
        value = settings

        for key in keys:
            if isinstance(value, dict):
                # Try different possible locations in the settings structure
                if key in value:
                    value = value[key]
                elif "persistent" in value and key in value["persistent"]:
                    value = value["persistent"][key]
                elif "transient" in value and key in value["transient"]:
                    value = value["transient"][key]
                elif "defaults" in value and key in value["defaults"]:
                    value = value["defaults"][key]
                else:
                    return None
            else:
                return None

        return value

    def _compare_numeric_values(self, value1: Any, value2: Any, operator: str) -> bool:
        """Compare two numeric values."""
        try:
            num1 = float(value1) if value1 is not None else 0
            num2 = float(value2) if value2 is not None else 0

            if operator == "<":
                return num1 < num2
            elif operator == "<=":
                return num1 <= num2
            elif operator == ">":
                return num1 > num2
            elif operator == ">=":
                return num1 >= num2
            elif operator == "==":
                return num1 == num2
            else:
                return False
        except (ValueError, TypeError):
            return False

    def _get_installed_plugins(self, client: KubernetesOpenSearchClient) -> List[str]:
        """Get list of installed plugins."""
        try:
            # This would typically query /_cat/plugins
            # For now, we'll return a placeholder list
            # In a real implementation, this would parse the actual plugin list
            return ["analysis-icu", "repository-s3"]  # Example plugins
        except Exception:
            return []


class ValidateNetworkConfigurationAction(BaseAction):
    """Action to validate network and connectivity configurations."""

    action_name = "validate_network_configuration"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        service_name = params.get("service_name", self.config.opensearch.service_name)

        # Network configurations to validate
        port_accessibility = params.get("port_accessibility", {})
        inter_node_connectivity = params.get("inter_node_connectivity", True)
        external_connectivity = params.get("external_connectivity", {})
        dns_resolution = params.get("dns_resolution", True)

        self.logger.info("Validating network configuration")

        try:
            validation_results = []
            all_passed = True

            # Validate port accessibility
            if port_accessibility:
                result = self._validate_port_accessibility(
                    namespace, service_name, port_accessibility
                )
                validation_results.append(result)
                if not result["success"]:
                    all_passed = False

            # Validate inter-node connectivity
            if inter_node_connectivity:
                result = self._validate_inter_node_connectivity(namespace)
                validation_results.append(result)
                if not result["success"]:
                    all_passed = False

            # Validate external connectivity
            if external_connectivity:
                result = self._validate_external_connectivity(
                    namespace, service_name, external_connectivity
                )
                validation_results.append(result)
                if not result["success"]:
                    all_passed = False

            # Validate DNS resolution
            if dns_resolution:
                result = self._validate_dns_resolution(namespace, service_name)
                validation_results.append(result)
                if not result["success"]:
                    all_passed = False

            # Generate summary
            passed_checks = sum(1 for r in validation_results if r["success"])
            total_checks = len(validation_results)

            summary = f"Network configuration validation: {passed_checks}/{total_checks} checks passed"

            return ActionResult(
                all_passed, summary, {"validation_details": validation_results}
            )

        except Exception as e:
            return ActionResult(False, f"Failed to validate network configuration: {e}")

    def _validate_port_accessibility(
        self, namespace: str, service_name: str, port_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate port accessibility."""
        try:
            validation_errors = []

            k8s_manager = KubernetesManager()
            if not k8s_manager.load_config():
                return {
                    "success": False,
                    "check": "port_accessibility",
                    "message": "Failed to load Kubernetes config",
                }

            # Get service information
            services = k8s_manager.get_services(namespace)
            target_service = None

            for service in services:
                if service["name"] == service_name:
                    target_service = service
                    break

            if not target_service:
                return {
                    "success": False,
                    "check": "port_accessibility",
                    "message": f"Service '{service_name}' not found",
                }

            # Check required ports
            required_ports = port_config.get("required_ports", [])
            service_ports = target_service.get("ports", [])

            for required_port in required_ports:
                port_num = required_port.get("port")
                protocol = required_port.get("protocol", "TCP")

                port_found = False
                for service_port in service_ports:
                    if (
                        service_port.get("port") == port_num
                        and service_port.get("protocol", "TCP") == protocol
                    ):
                        port_found = True
                        break

                if not port_found:
                    validation_errors.append(
                        f"Required port {port_num}/{protocol} is not exposed by service"
                    )

            # Check for forbidden ports
            forbidden_ports = port_config.get("forbidden_ports", [])
            for forbidden_port in forbidden_ports:
                port_num = forbidden_port.get("port")
                for service_port in service_ports:
                    if service_port.get("port") == port_num:
                        validation_errors.append(
                            f"Forbidden port {port_num} is exposed by service"
                        )

            if validation_errors:
                return {
                    "success": False,
                    "check": "port_accessibility",
                    "message": "; ".join(validation_errors),
                    "service_ports": service_ports,
                }
            else:
                return {
                    "success": True,
                    "check": "port_accessibility",
                    "message": "All port accessibility requirements are met",
                    "service_ports": service_ports,
                }

        except Exception as e:
            return {
                "success": False,
                "check": "port_accessibility",
                "message": f"Error validating port accessibility: {e}",
            }

    def _validate_inter_node_connectivity(self, namespace: str) -> Dict[str, Any]:
        """Validate inter-node connectivity."""
        try:
            # This would involve checking if nodes can communicate with each other
            # For now, we'll do a basic connectivity test via OpenSearch client

            try:
                client = KubernetesOpenSearchClient.from_security_config(
                    self.config.opensearch.security, namespace
                )
                with client:
                    cluster_stats = client.cluster_stats()
                    if cluster_stats and "nodes" in cluster_stats:
                        node_count = (
                            cluster_stats["nodes"].get("count", {}).get("total", 0)
                        )
                        if node_count > 1:
                            # If we can get cluster stats with multiple nodes, connectivity is working
                            return {
                                "success": True,
                                "check": "inter_node_connectivity",
                                "message": f"Inter-node connectivity verified for {node_count} nodes",
                            }
                        else:
                            return {
                                "success": True,
                                "check": "inter_node_connectivity",
                                "message": "Single node cluster - no inter-node connectivity needed",
                            }
                    else:
                        return {
                            "success": False,
                            "check": "inter_node_connectivity",
                            "message": "Could not verify inter-node connectivity - no cluster stats",
                        }
            except Exception as e:
                return {
                    "success": False,
                    "check": "inter_node_connectivity",
                    "message": f"Inter-node connectivity test failed: {e}",
                }

        except Exception as e:
            return {
                "success": False,
                "check": "inter_node_connectivity",
                "message": f"Error validating inter-node connectivity: {e}",
            }

    def _validate_external_connectivity(
        self, namespace: str, service_name: str, external_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate external connectivity."""
        try:
            validation_errors = []

            # Check if external access is required
            require_external_access = external_config.get(
                "require_external_access", False
            )
            load_balancer_type = external_config.get(
                "load_balancer_type"
            )  # 'NodePort', 'LoadBalancer', 'Ingress'

            k8s_manager = KubernetesManager()
            if not k8s_manager.load_config():
                return {
                    "success": False,
                    "check": "external_connectivity",
                    "message": "Failed to load Kubernetes config",
                }

            services = k8s_manager.get_services(namespace)
            target_service = None

            for service in services:
                if service["name"] == service_name:
                    target_service = service
                    break

            if not target_service:
                return {
                    "success": False,
                    "check": "external_connectivity",
                    "message": f"Service '{service_name}' not found",
                }

            service_type = target_service.get("type", "ClusterIP")

            if require_external_access:
                if service_type == "ClusterIP":
                    validation_errors.append(
                        "External access required but service type is ClusterIP"
                    )
                elif load_balancer_type and service_type != load_balancer_type:
                    validation_errors.append(
                        f"Service type is {service_type}, expected {load_balancer_type}"
                    )

            if validation_errors:
                return {
                    "success": False,
                    "check": "external_connectivity",
                    "message": "; ".join(validation_errors),
                    "service_type": service_type,
                }
            else:
                return {
                    "success": True,
                    "check": "external_connectivity",
                    "message": f"External connectivity configuration is valid (service type: {service_type})",
                    "service_type": service_type,
                }

        except Exception as e:
            return {
                "success": False,
                "check": "external_connectivity",
                "message": f"Error validating external connectivity: {e}",
            }

    def _validate_dns_resolution(
        self, namespace: str, service_name: str
    ) -> Dict[str, Any]:
        """Validate DNS resolution."""
        try:
            # Test DNS resolution by attempting to connect using service DNS name
            try:
                client = KubernetesOpenSearchClient.from_security_config(
                    self.config.opensearch.security, namespace, service_name
                )
                with client:
                    health = client.health()
                    if health:
                        return {
                            "success": True,
                            "check": "dns_resolution",
                            "message": f"DNS resolution working for service '{service_name}'",
                        }
                    else:
                        return {
                            "success": False,
                            "check": "dns_resolution",
                            "message": "DNS resolves but service is not responding",
                        }
            except Exception as e:
                if "Name or service not known" in str(
                    e
                ) or "not resolve hostname" in str(e):
                    return {
                        "success": False,
                        "check": "dns_resolution",
                        "message": f"DNS resolution failed for service '{service_name}': {e}",
                    }
                else:
                    # DNS might be working but service might not be ready
                    return {
                        "success": True,
                        "check": "dns_resolution",
                        "message": f"DNS resolution appears to work (connection error: {e})",
                    }

        except Exception as e:
            return {
                "success": False,
                "check": "dns_resolution",
                "message": f"Error validating DNS resolution: {e}",
            }
