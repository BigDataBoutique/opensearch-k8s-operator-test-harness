"""Monitoring and debugging actions."""

import json
import os
import subprocess
import time
from datetime import datetime
from typing import Any, Dict

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.utils.opensearch_client import KubernetesOpenSearchClient


class CollectLogsAction(BaseAction):
    """Action to collect logs from various components."""

    action_name = "collect_logs"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        components = params.get("components", ["operator", "opensearch-pods"])
        since = params.get("since", "10m")
        output_dir = self._substitute_template_vars(
            params.get("output_dir", "./logs/{timestamp}")
        )
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)

        self.logger.info(f"Collecting logs for components: {', '.join(components)}")

        try:
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)

            collected_files = []

            for component in components:
                if component == "operator":
                    files = self._collect_operator_logs(namespace, since, output_dir)
                elif component == "opensearch-pods":
                    files = self._collect_opensearch_pod_logs(
                        namespace, since, output_dir
                    )
                elif component == "events":
                    files = self._collect_events(namespace, since, output_dir)
                else:
                    self.logger.warning(f"Unknown component: {component}")
                    continue

                collected_files.extend(files)

            return ActionResult(
                True, f"Collected logs to {output_dir}: {len(collected_files)} files"
            )

        except Exception as e:
            return ActionResult(False, f"Failed to collect logs: {e}")

    def _collect_operator_logs(
        self, namespace: str, since: str, output_dir: str
    ) -> list:
        """Collect operator logs."""
        files = []

        try:
            # Get operator pods
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    "app=opensearch-operator",
                    "-o",
                    "jsonpath={.items[*].metadata.name}",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                pod_names = result.stdout.strip().split()

                for pod_name in pod_names:
                    if pod_name:
                        log_file = os.path.join(output_dir, f"operator-{pod_name}.log")

                        with open(log_file, "w") as f:
                            subprocess.run(
                                [
                                    "kubectl",
                                    "logs",
                                    pod_name,
                                    "-n",
                                    namespace,
                                    "--since",
                                    since,
                                ],
                                stdout=f,
                                stderr=subprocess.STDOUT,
                            )

                        files.append(log_file)

        except Exception as e:
            self.logger.error(f"Failed to collect operator logs: {e}")

        return files

    def _collect_opensearch_pod_logs(
        self, namespace: str, since: str, output_dir: str
    ) -> list:
        """Collect OpenSearch pod logs."""
        files = []

        try:
            # Get OpenSearch pods
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    "app=opensearch",
                    "-o",
                    "jsonpath={.items[*].metadata.name}",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                pod_names = result.stdout.strip().split()

                for pod_name in pod_names:
                    if pod_name:
                        log_file = os.path.join(
                            output_dir, f"opensearch-{pod_name}.log"
                        )

                        with open(log_file, "w") as f:
                            subprocess.run(
                                [
                                    "kubectl",
                                    "logs",
                                    pod_name,
                                    "-n",
                                    namespace,
                                    "--since",
                                    since,
                                ],
                                stdout=f,
                                stderr=subprocess.STDOUT,
                            )

                        files.append(log_file)

        except Exception as e:
            self.logger.error(f"Failed to collect OpenSearch pod logs: {e}")

        return files

    def _collect_events(self, namespace: str, since: str, output_dir: str) -> list:
        """Collect Kubernetes events."""
        files = []

        try:
            event_file = os.path.join(output_dir, "events.yaml")

            with open(event_file, "w") as f:
                subprocess.run(
                    ["kubectl", "get", "events", "-n", namespace, "-o", "yaml"],
                    stdout=f,
                )

            files.append(event_file)

        except Exception as e:
            self.logger.error(f"Failed to collect events: {e}")

        return files


class CaptureMetricsAction(BaseAction):
    """Action to capture cluster metrics."""

    action_name = "capture_metrics"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        duration = params.get("duration", "5m")
        metrics = params.get("metrics", ["cluster_health", "node_stats", "index_stats"])
        output_file = self._substitute_template_vars(
            params.get("output_file", "metrics-{timestamp}.json")
        )
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        service_name = params.get("service_name", self.config.opensearch.service_name)

        self.logger.info(f"Capturing metrics for {duration}")

        try:
            captured_metrics = {}

            with KubernetesOpenSearchClient(
                namespace=namespace, service_name=service_name
            ) as client:
                # Capture initial metrics
                for metric_type in metrics:
                    captured_metrics[metric_type] = []

                    if metric_type == "cluster_health":
                        data = client.health()
                    elif metric_type == "node_stats":
                        data = client.node_stats()
                    elif metric_type == "index_stats":
                        # Get index stats
                        response = client.session.get(f"{client.base_url}/_stats")
                        response.raise_for_status()
                        data = response.json()
                    elif metric_type == "cluster_stats":
                        data = client.cluster_stats()
                    else:
                        self.logger.warning(f"Unknown metric type: {metric_type}")
                        continue

                    captured_metrics[metric_type].append(
                        {"timestamp": datetime.now().isoformat(), "data": data}
                    )

                # If duration is specified, capture metrics over time
                duration_seconds = self._parse_duration(duration)
                if (
                    duration_seconds > 60
                ):  # Only do periodic capture for longer durations
                    interval = min(
                        60, duration_seconds // 10
                    )  # Capture 10 times or every minute

                    for _ in range(duration_seconds // interval):
                        time.sleep(interval)

                        for metric_type in metrics:
                            try:
                                if metric_type == "cluster_health":
                                    data = client.health()
                                elif metric_type == "node_stats":
                                    data = client.node_stats()
                                elif metric_type == "index_stats":
                                    response = client.session.get(
                                        f"{client.base_url}/_stats"
                                    )
                                    response.raise_for_status()
                                    data = response.json()
                                elif metric_type == "cluster_stats":
                                    data = client.cluster_stats()
                                else:
                                    continue

                                captured_metrics[metric_type].append(
                                    {
                                        "timestamp": datetime.now().isoformat(),
                                        "data": data,
                                    }
                                )

                            except Exception as e:
                                self.logger.warning(
                                    f"Failed to capture {metric_type}: {e}"
                                )

            # Save metrics to file
            with open(output_file, "w") as f:
                json.dump(captured_metrics, f, indent=2)

            return ActionResult(True, f"Captured metrics saved to {output_file}")

        except Exception as e:
            return ActionResult(False, f"Failed to capture metrics: {e}")

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


class DebugPauseAction(BaseAction):
    """Action to pause execution for manual inspection."""

    action_name = "debug_pause"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        message = params.get("message", "Pausing for manual inspection")
        timeout_str = params.get("timeout", "5m")

        timeout = self._parse_duration(timeout_str)

        self.logger.info(f"Debug pause: {message}")
        self.logger.info(f"Will auto-continue after {timeout_str}")

        print(f"\n{'=' * 60}")
        print(f"DEBUG PAUSE: {message}")
        print(f"Auto-continue in {timeout_str}")
        print("Press Ctrl+C to abort, or Enter to continue immediately")
        print(f"{'=' * 60}")

        try:
            import select
            import sys

            start_time = time.time()

            while time.time() - start_time < timeout:
                # Check if input is available (Unix-like systems)
                if hasattr(select, "select"):
                    ready, _, _ = select.select([sys.stdin], [], [], 1)
                    if ready:
                        sys.stdin.readline()
                        break
                else:
                    # Windows fallback - just wait
                    time.sleep(1)

            return ActionResult(True, "Debug pause completed")

        except KeyboardInterrupt:
            return ActionResult(False, "Debug pause aborted by user")
        except Exception as e:
            self.logger.warning(f"Debug pause error: {e}")
            time.sleep(timeout)
            return ActionResult(True, "Debug pause completed (fallback)")

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
