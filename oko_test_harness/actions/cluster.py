"""Cluster management actions."""

import subprocess
import time
from typing import Any, Dict, List

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.utils.kubernetes import (
    KindClusterManager,
    MinikubeClusterManager,
    KubernetesManager,
)


class SetupClusterAction(BaseAction):
    """Action to set up a Kubernetes cluster."""

    action_name = "setup_cluster"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        cluster_name = params.get("cluster_name", self.config.kubernetes.cluster_name)
        provider = params.get("provider", self.config.kubernetes.provider)
        nodes = params.get("nodes", self.config.kubernetes.nodes)
        k8s_version = params.get(
            "kubernetes_version", self.config.kubernetes.kubernetes_version
        )
        extra_config = params.get("extra_config", self.config.kubernetes.extra_config)

        self.logger.info(f"Setting up {provider} cluster: {cluster_name}")

        if provider == "kind":
            return self._setup_kind_cluster(
                cluster_name, nodes, k8s_version, extra_config
            )
        elif provider == "minikube":
            return self._setup_minikube_cluster(
                cluster_name, nodes, k8s_version, extra_config
            )
        elif provider == "existing":
            return self._setup_existing_cluster()
        else:
            return ActionResult(False, f"Unsupported provider: {provider}")

    def _setup_kind_cluster(
        self, cluster_name: str, nodes: int, k8s_version: str, extra_config: str = None
    ) -> ActionResult:
        """Set up a Kind cluster."""
        kind_manager = KindClusterManager()

        # Check if cluster already exists
        if kind_manager.cluster_exists(cluster_name):
            self.logger.info(f"Kind cluster {cluster_name} already exists")
            return ActionResult(True, f"Cluster {cluster_name} already exists")

        # Generate Kind config if not provided
        config_content = extra_config or self._generate_kind_config(nodes, k8s_version)

        if kind_manager.create_cluster(cluster_name, config_content):
            # Wait for cluster to be ready
            time.sleep(10)

            # Pre-pull common images to avoid timeout issues during deployment
            self._prepull_common_images(cluster_name, "kind")

            return ActionResult(
                True, f"Successfully created Kind cluster {cluster_name}"
            )
        else:
            return ActionResult(False, f"Failed to create Kind cluster {cluster_name}")

    def _setup_minikube_cluster(
        self, cluster_name: str, nodes: int, k8s_version: str, extra_config: str = None
    ) -> ActionResult:
        """Set up a Minikube cluster."""
        minikube_manager = MinikubeClusterManager()

        # Check if cluster already exists
        if minikube_manager.cluster_exists(cluster_name):
            status = minikube_manager.get_status(cluster_name)
            if status == "Running":
                self.logger.info(
                    f"Minikube cluster {cluster_name} already exists and is running"
                )
                return ActionResult(True, f"Cluster {cluster_name} already exists")
            else:
                self.logger.info(
                    f"Minikube cluster {cluster_name} exists but is not running, starting it..."
                )

        if minikube_manager.create_cluster(
            cluster_name, nodes, k8s_version, extra_config
        ):
            # Wait for cluster to be ready
            time.sleep(15)

            # Pre-load common images to avoid timeout issues during deployment
            self._prepull_common_images(cluster_name, "minikube")

            return ActionResult(
                True, f"Successfully created Minikube cluster {cluster_name}"
            )
        else:
            return ActionResult(
                False, f"Failed to create Minikube cluster {cluster_name}"
            )

    def _setup_existing_cluster(self) -> ActionResult:
        """Use existing cluster."""
        k8s_manager = KubernetesManager()
        if k8s_manager.load_config():
            return ActionResult(True, "Using existing Kubernetes cluster")
        else:
            return ActionResult(False, "Failed to connect to existing cluster")

    def _generate_kind_config(self, nodes: int, k8s_version: str) -> str:
        """Generate Kind cluster configuration."""
        config = f"""kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: opensearch-test
nodes:
- role: control-plane
  image: kindest/node:{k8s_version}
"""

        # Add worker nodes
        for i in range(nodes - 1):
            config += f"""- role: worker
  image: kindest/node:{k8s_version}
"""

        return config

    def _prepull_common_images(self, cluster_name: str, provider: str = "kind") -> None:
        """Pre-pull commonly used images to avoid timeout issues during deployment."""
        import subprocess

        # Most commonly used images in OpenSearch deployments
        common_images = [
            "opensearchproject/opensearch:2.9.0",
            "opensearchproject/opensearch:3.0.0",
            "opensearchproject/opensearch:2.17.0",
            "opensearchproject/opensearch-dashboards:2.9.0",
            "opensearchproject/opensearch-dashboards:3.0.0",
            "opensearchproject/opensearch-dashboards:2.17.0",
            "busybox:latest",
            "curlimages/curl:latest",
        ]

        self.logger.info(f"Pre-loading common images for {provider} cluster...")

        for image in common_images:
            try:
                # Check if image exists locally
                check_result = subprocess.run(
                    ["docker", "image", "inspect", image],
                    capture_output=True,
                    text=True,
                )

                if check_result.returncode != 0:
                    self.logger.info(f"  Pulling {image}...")
                    # Pull to local Docker first
                    subprocess.run(
                        ["docker", "pull", image],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                else:
                    self.logger.info(f"  Using cached {image}")

                # Load into cluster based on provider
                if provider == "kind":
                    subprocess.run(
                        ["kind", "load", "docker-image", image, "--name", cluster_name],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                elif provider == "minikube":
                    # For minikube, use minikube image load command
                    subprocess.run(
                        ["minikube", "image", "load", image, "--profile", cluster_name],
                        check=True,
                        capture_output=True,
                        text=True,
                    )

                self.logger.info(f"  ✓ {image} loaded into {provider} cluster")

            except subprocess.CalledProcessError as e:
                # Don't fail cluster setup if image pre-pull fails
                self.logger.warning(f"  ⚠ Failed to pre-load {image}: {e}")
                continue

        self.logger.info("Image pre-loading completed")


class InstallOperatorAction(BaseAction):
    """Action to install the OpenSearch operator."""

    action_name = "install_operator"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        version = params.get("version", self.config.opensearch.operator_version)
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        helm_chart = params.get("helm_chart", "opensearch-operator/opensearch-operator")
        helm_repo_url = params.get(
            "helm_repo_url",
            "https://opensearch-project.github.io/opensearch-k8s-operator/",
        )
        local_chart_path = params.get("local_chart_path")  # For development versions
        git_chart_repo = params.get(
            "git_chart_repo"
        )  # For development versions from git
        git_chart_branch = params.get("git_chart_branch", "main")
        values_file = params.get("values_file")
        wait_timeout = params.get("wait_timeout", "5m")

        # Check if local operator path is configured (takes priority over other methods)
        local_operator_path = self.config.opensearch.local_operator_path
        if local_operator_path and local_operator_path.strip():
            self.logger.info(
                f"Installing OpenSearch operator from local source: {local_operator_path}"
            )
            return self._install_local_operator(
                local_operator_path, namespace, wait_timeout
            )

        self.logger.info(f"Installing OpenSearch operator version {version}")

        # Create namespace
        k8s_manager = KubernetesManager()
        if not k8s_manager.load_config():
            return ActionResult(False, "Failed to load Kubernetes config")

        if not k8s_manager.create_namespace(namespace):
            return ActionResult(False, f"Failed to create namespace {namespace}")

        # Install via Helm or kubectl
        if local_chart_path or git_chart_repo:
            return self._install_dev_helm_chart(
                local_chart_path,
                git_chart_repo,
                git_chart_branch,
                version,
                namespace,
                values_file,
                wait_timeout,
                self.config.opensearch.cluster_name,
            )
        elif helm_chart:
            return self._install_via_helm(
                helm_chart, helm_repo_url, version, namespace, values_file, wait_timeout
            )
        else:
            return self._install_via_kubectl(version, namespace)

    def _install_via_helm(
        self,
        chart: str,
        repo_url: str,
        version: str,
        namespace: str,
        values_file: str = None,
        timeout: str = "5m",
    ) -> ActionResult:
        """Install operator via Helm."""
        try:
            # Determine repository name from chart
            repo_name = chart.split("/")[0] if "/" in chart else "opensearch-operator"

            # Add OpenSearch operator Helm repository
            self.logger.info(f"Adding Helm repository: {repo_name} -> {repo_url}")
            repo_add_result = subprocess.run(
                ["helm", "repo", "add", repo_name, repo_url],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if (
                repo_add_result.returncode != 0
                and "already exists" not in repo_add_result.stderr
            ):
                return ActionResult(
                    False, f"Failed to add Helm repo: {repo_add_result.stderr}"
                )

            # Update repositories
            self.logger.info("Updating Helm repositories...")
            update_result = subprocess.run(
                ["helm", "repo", "update"], capture_output=True, text=True, timeout=60
            )

            if update_result.returncode != 0:
                return ActionResult(
                    False, f"Failed to update Helm repos: {update_result.stderr}"
                )

            # Install operator
            self.logger.info(f"Installing Helm chart: {chart}")
            cmd = [
                "helm",
                "install",
                "opensearch-operator",
                chart,
                "--namespace",
                namespace,
                "--create-namespace",
                "--wait",
                "--timeout",
                timeout,
            ]

            if version != "latest":
                cmd.extend(["--version", version])

            if values_file:
                cmd.extend(["-f", values_file])

            # Log the exact command being executed
            self.logger.info(f"Executing: {' '.join(cmd)}")

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600
            )  # 10 min timeout

            if result.returncode == 0:
                self.logger.debug("OpenSearch operator installed successfully via Helm")
                return ActionResult(True, "OpenSearch operator installed successfully")
            else:
                error_msg = f"Helm install failed: {result.stderr}"
                if result.stdout:
                    error_msg += f"\nStdout: {result.stdout}"
                return ActionResult(False, error_msg)

        except subprocess.TimeoutExpired as e:
            return ActionResult(False, f"Helm install timed out: {e}")
        except Exception as e:
            return ActionResult(
                False, f"Error installing OpenSearch operator via Helm: {e}"
            )

    def _install_dev_helm_chart(
        self,
        local_chart_path: str,
        git_chart_repo: str,
        git_chart_branch: str,
        version: str,
        namespace: str,
        values_file: str = None,
        timeout: str = "5m",
        release_name: str = "opensearch-cluster",
    ) -> ActionResult:
        """Install operator from development Helm chart (local or git)."""
        import tempfile
        import os

        try:
            chart_path = local_chart_path

            # If git repo provided, clone it to temporary directory
            if git_chart_repo and not local_chart_path:
                temp_dir = tempfile.mkdtemp()
                self.logger.info(
                    f"Cloning development chart from {git_chart_repo} (branch: {git_chart_branch})"
                )

                clone_cmd = [
                    "git",
                    "clone",
                    "--branch",
                    git_chart_branch,
                    "--depth",
                    "1",
                    git_chart_repo,
                    temp_dir,
                ]
                result = subprocess.run(clone_cmd, capture_output=True, text=True)

                if result.returncode != 0:
                    return ActionResult(
                        False, f"Failed to clone chart repository: {result.stderr}"
                    )

                # Look for chart directory (common patterns)
                chart_candidates = [
                    os.path.join(temp_dir, "charts", "opensearch"),
                    os.path.join(temp_dir, "opensearch"),
                    os.path.join(temp_dir, "chart"),
                    temp_dir,
                ]

                chart_path = None
                for candidate in chart_candidates:
                    if os.path.exists(os.path.join(candidate, "Chart.yaml")):
                        chart_path = candidate
                        break

                if not chart_path:
                    return ActionResult(
                        False, "Could not find Chart.yaml in the cloned repository"
                    )

            if not chart_path or not os.path.exists(chart_path):
                return ActionResult(False, f"Chart path does not exist: {chart_path}")

            self.logger.info(f"Installing development chart from: {chart_path}")

            # Install from local chart
            cmd = [
                "helm",
                "install",
                release_name,
                chart_path,
                "--namespace",
                namespace,
                "--create-namespace",
                "--wait",
                "--timeout",
                timeout,
            ]

            if values_file:
                cmd.extend(["-f", values_file])

            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                self.logger.debug("Development OpenSearch chart installed successfully")
                return ActionResult(
                    True, f"Development chart installed successfully from {chart_path}"
                )
            else:
                return ActionResult(False, f"Helm install failed: {result.stderr}")

        except Exception as e:
            return ActionResult(False, f"Error installing development chart: {e}")

    def _install_via_kubectl(self, version: str, namespace: str) -> ActionResult:
        """Install operator via kubectl."""
        try:
            # This would typically download and apply the operator manifests
            # For now, we'll simulate this with a placeholder
            manifest_url = f"https://github.com/opensearch-project/opensearch-k8s-operator/releases/download/v{version}/opensearch-operator.yaml"

            cmd = ["kubectl", "apply", "-f", manifest_url, "-n", namespace]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                self.logger.debug(
                    "OpenSearch operator installed successfully via kubectl"
                )
                return ActionResult(True, "Operator installed successfully")
            else:
                return ActionResult(False, f"kubectl apply failed: {result.stderr}")

        except Exception as e:
            return ActionResult(False, f"Error installing operator via kubectl: {e}")

    def _install_local_operator(
        self, local_path: str, namespace: str, timeout: str = "5m"
    ) -> ActionResult:
        """Install operator from local source code."""
        import os

        try:
            # Validate local operator path
            if not os.path.exists(local_path):
                return ActionResult(
                    False, f"Local operator path does not exist: {local_path}"
                )

            # Look for key indicator files to confirm this is an operator repository
            required_files = ["Dockerfile", "Makefile"]
            go_files = ["main.go", "go.mod"]

            has_dockerfile = any(
                os.path.exists(os.path.join(local_path, f)) for f in required_files
            )
            has_go = any(os.path.exists(os.path.join(local_path, f)) for f in go_files)

            if not (has_dockerfile and has_go):
                return ActionResult(
                    False,
                    f"Local path does not appear to be an OpenSearch operator repository (missing Dockerfile, Makefile, or Go files): {local_path}",
                )

            self.logger.info("Building and deploying operator from local source...")

            # Create namespace first
            k8s_manager = KubernetesManager()
            if not k8s_manager.load_config():
                return ActionResult(False, "Failed to load Kubernetes config")

            if not k8s_manager.create_namespace(namespace):
                return ActionResult(False, f"Failed to create namespace {namespace}")

            # Change to operator directory
            original_dir = os.getcwd()
            os.chdir(local_path)

            try:
                # Check if we're in a Kind cluster (different approach needed for Kind)
                kind_cluster_result = subprocess.run(
                    ["kubectl", "config", "current-context"],
                    capture_output=True,
                    text=True,
                )
                is_kind_cluster = (
                    kind_cluster_result.returncode == 0
                    and "kind-" in kind_cluster_result.stdout
                )

                if is_kind_cluster:
                    return self._install_local_operator_kind(
                        local_path, namespace, timeout
                    )
                else:
                    return self._install_local_operator_standard(
                        local_path, namespace, timeout
                    )

            finally:
                os.chdir(original_dir)

        except Exception as e:
            return ActionResult(False, f"Error installing local operator: {e}")

    def _install_local_operator_kind(
        self, local_path: str, namespace: str, timeout: str
    ) -> ActionResult:
        """Install local operator in Kind cluster (requires loading image into Kind)."""
        try:
            # Get current Kind cluster name
            context_result = subprocess.run(
                ["kubectl", "config", "current-context"], capture_output=True, text=True
            )
            if context_result.returncode != 0:
                return ActionResult(False, "Failed to get current kubectl context")

            cluster_name = context_result.stdout.strip().replace("kind-", "")
            image_tag = "opensearch-operator:dev-local"

            self.logger.debug(
                f"Building operator image for Kind cluster: {cluster_name}"
            )

            # Build the operator image
            build_result = subprocess.run(
                ["make", "docker-build", f"IMG={image_tag}"],
                capture_output=True,
                text=True,
                timeout=600,
            )

            if build_result.returncode != 0:
                return ActionResult(
                    False, f"Failed to build operator image: {build_result.stderr}"
                )

            self.logger.info("Loading operator image into Kind cluster...")

            # Load image into Kind cluster
            load_result = subprocess.run(
                ["kind", "load", "docker-image", image_tag, "--name", cluster_name],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if load_result.returncode != 0:
                return ActionResult(
                    False, f"Failed to load image into Kind: {load_result.stderr}"
                )

            # Deploy the operator
            self.logger.info("Deploying operator to cluster...")
            deploy_result = subprocess.run(
                ["make", "deploy", f"IMG={image_tag}", f"NAMESPACE={namespace}"],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if deploy_result.returncode != 0:
                return ActionResult(
                    False, f"Failed to deploy operator: {deploy_result.stderr}"
                )

            self.logger.debug("Local operator installed successfully in Kind cluster")
            return ActionResult(True, f"Local operator installed from {local_path}")

        except subprocess.TimeoutExpired:
            return ActionResult(False, "Timeout building or deploying local operator")
        except Exception as e:
            return ActionResult(False, f"Error installing local operator in Kind: {e}")

    def _install_local_operator_standard(
        self, local_path: str, namespace: str, timeout: str
    ) -> ActionResult:
        """Install local operator in standard Kubernetes cluster."""
        try:
            image_tag = "opensearch-operator:dev-local"

            self.logger.debug("Building and pushing operator image...")

            # Build and push the operator image (assumes registry access is configured)
            build_push_result = subprocess.run(
                ["make", "docker-build-push", f"IMG={image_tag}"],
                capture_output=True,
                text=True,
                timeout=600,
            )

            if build_push_result.returncode != 0:
                # Try alternative make targets
                self.logger.debug("Trying alternative build approach...")
                build_result = subprocess.run(
                    ["make", "docker-build", f"IMG={image_tag}"],
                    capture_output=True,
                    text=True,
                    timeout=600,
                )

                if build_result.returncode != 0:
                    return ActionResult(
                        False, f"Failed to build operator image: {build_result.stderr}"
                    )

                # Try to push separately
                push_result = subprocess.run(
                    ["docker", "push", image_tag],
                    capture_output=True,
                    text=True,
                    timeout=300,
                )

                if push_result.returncode != 0:
                    self.logger.warning(
                        f"Failed to push image {image_tag}, assuming local registry: {push_result.stderr}"
                    )

            # Deploy the operator
            self.logger.info("Deploying operator to cluster...")
            deploy_result = subprocess.run(
                ["make", "deploy", f"IMG={image_tag}", f"NAMESPACE={namespace}"],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if deploy_result.returncode != 0:
                return ActionResult(
                    False, f"Failed to deploy operator: {deploy_result.stderr}"
                )

            self.logger.debug("Local operator installed successfully")
            return ActionResult(True, f"Local operator installed from {local_path}")

        except subprocess.TimeoutExpired:
            return ActionResult(False, "Timeout building or deploying local operator")
        except Exception as e:
            return ActionResult(False, f"Error installing local operator: {e}")


class DeployClusterAction(BaseAction):
    """Action to deploy an OpenSearch cluster."""

    action_name = "deploy_cluster"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        cluster_name = params.get("cluster_name", self.config.opensearch.cluster_name)
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        version = params.get("version", self.config.opensearch.opensearch_version)
        # Support both old format and new configurable node pools
        nodes = params.get(
            "nodes", {"master": 1, "data": 1}
        )  # Minimal defaults for Kind
        node_pools = params.get("node_pools", [])
        storage = params.get(
            "storage", {"class": "standard", "size": "20Gi"}
        )  # Smaller default
        template_file = params.get("template_file")
        # Get security config from parameters or use config default
        security_config = params.get("security")
        if security_config is None:
            # Use the security config from the global configuration
            security_config = {
                "enabled": self.config.opensearch.security.enabled,
                "username": self.config.opensearch.security.username,
                "password": self.config.opensearch.security.password,
                "use_ssl": self.config.opensearch.security.use_ssl,
                "admin_secret_name": self.config.opensearch.security.admin_secret_name,
                "security_config_secret_name": self.config.opensearch.security.security_config_secret_name,
            }
            self.logger.info(
                f"Using default security config: enabled={security_config['enabled']}, username={security_config['username']}"
            )
        elif not isinstance(security_config, dict):
            # Handle simple boolean for backwards compatibility
            security_config = {"enabled": bool(security_config)}
        dashboards_config = params.get(
            "dashboards", {"enabled": False}
        )  # Disable dashboards by default
        cluster_settings = params.get("cluster_settings", {})

        self.logger.info(f"Deploying OpenSearch cluster: {cluster_name}")

        # Create namespace for the cluster
        k8s_manager = KubernetesManager()
        if not k8s_manager.load_config():
            return ActionResult(False, "Failed to load Kubernetes config")

        if not k8s_manager.create_namespace(namespace):
            return ActionResult(False, f"Failed to create namespace {namespace}")

        # Create security secrets if security is enabled
        if security_config.get("enabled", False):
            self.logger.info(
                f"Security is enabled, creating security secrets for namespace {namespace}"
            )
            if not self._create_security_secrets(
                k8s_manager, namespace, security_config
            ):
                return ActionResult(False, "Failed to create security secrets")
        else:
            self.logger.warning(f"Security is disabled: {security_config}")

        # Generate or load cluster manifest
        if template_file:
            with open(template_file, "r") as f:
                manifest = f.read()
        else:
            manifest = self._generate_cluster_manifest(
                cluster_name,
                namespace,
                version,
                nodes,
                node_pools,
                storage,
                security_config,
                dashboards_config,
                cluster_settings,
            )

        # Apply the manifest
        if k8s_manager.apply_yaml_manifest(manifest, namespace):
            return ActionResult(
                True, f"OpenSearch cluster {cluster_name} deployed successfully"
            )
        else:
            return ActionResult(
                False, f"Failed to deploy OpenSearch cluster {cluster_name}"
            )

    def _generate_cluster_manifest(
        self,
        name: str,
        namespace: str,
        version: str,
        nodes: Dict[str, int],
        node_pools: List[Dict[str, Any]],
        storage: Dict[str, str],
        security_config: Dict[str, Any],
        dashboards_config: Dict[str, Any],
        cluster_settings: Dict[str, Any] = None,
    ) -> str:
        """Generate OpenSearch cluster manifest with configurable resources and security."""

        # Security and dashboards configuration
        security_enabled = security_config.get("enabled", False)
        dashboards_enabled = dashboards_config.get("enabled", False)

        manifest = f"""
apiVersion: opensearch.opster.io/v1
kind: OpenSearchCluster
metadata:
  name: {name}
  namespace: {namespace}
spec:
  general:
    httpPort: 9200
    vendor: opensearch
    version: {version}
    serviceName: {name}
    setVMMaxMapCount: true"""

        # Add cluster settings if provided
        if cluster_settings:
            manifest += """
    additionalConfig:"""
            for setting_key, setting_value in cluster_settings.items():
                manifest += f"""
      "{setting_key}": "{setting_value}" """

        # Add security configuration
        if security_enabled:
            admin_secret_name = security_config.get(
                "admin_secret_name", "admin-credentials-secret"
            )
            security_secret_name = security_config.get(
                "security_config_secret_name", "securityconfig-secret"
            )
            manifest += f"""
  security:
    config:
      securityConfigSecret:
        name: {security_secret_name}
      adminCredentialsSecret:
        name: {admin_secret_name}
      adminSecret:
        name: {name}-admin-cert
    tls:
      transport:
        generate: true
      http:
        generate: true"""
        else:
            # Explicitly disable security
            manifest += """
  security:
    config:
      disable: true"""

        # Add dashboards configuration if enabled
        if dashboards_enabled:
            manifest += f"""
  dashboards:
    enable: true
    version: {version}
    replicas: {dashboards_config.get("replicas", 1)}
    resources:
      requests:
        memory: "{dashboards_config.get("resources", {}).get("requests", {}).get("memory", "256Mi")}"
        cpu: "{dashboards_config.get("resources", {}).get("requests", {}).get("cpu", "50m")}"
      limits:
        memory: "{dashboards_config.get("resources", {}).get("limits", {}).get("memory", "512Mi")}"
        cpu: "{dashboards_config.get("resources", {}).get("limits", {}).get("cpu", "200m")}"
"""
        else:
            # Even when disabled, some operators require these fields
            manifest += f"""
  dashboards:
    enable: false
    version: {version}
    replicas: {dashboards_config.get("replicas", 0)}
"""

        manifest += """
  nodePools:
"""

        # Use custom node pools if provided, otherwise use defaults
        if node_pools:
            for pool in node_pools:
                component = pool.get("component", "nodes")
                replicas = pool.get("replicas", 1)
                roles = pool.get("roles", ["master", "data", "ingest"])
                # Use global config values as defaults, but allow pool-specific overrides
                default_resources = self.config.opensearch.node_resources or {
                    "requests": {"memory": "1Gi", "cpu": "100m"},
                    "limits": {"memory": "2Gi", "cpu": "500m"},
                }
                resources = pool.get("resources", default_resources)
                default_jvm = self.config.opensearch.jvm_heap or "-Xmx1g -Xms1g"
                jvm = pool.get("jvm", default_jvm)

                manifest += f"""
  - component: {component}
    replicas: {replicas}
    diskSize: {storage["size"]}
    resources:
      requests:
        memory: "{resources["requests"]["memory"]}"
        cpu: "{resources["requests"]["cpu"]}"
      limits:
        memory: "{resources["limits"]["memory"]}"
        cpu: "{resources["limits"]["cpu"]}"
    jvm: "{jvm}"
    env:
      - name: OPENSEARCH_INITIAL_ADMIN_PASSWORD
        valueFrom:
          secretKeyRef:
            name: admin-credentials-secret
            key: password
    roles:
"""
                for role in roles:
                    manifest += f'      - "{role}"\n'
        else:
            # Default minimal configuration for Kind - optimized for resource constraints
            # Use global config values if available
            default_resources = self.config.opensearch.node_resources or {
                "requests": {"memory": "1Gi", "cpu": "100m"},
                "limits": {"memory": "2Gi", "cpu": "500m"},
            }
            default_jvm = self.config.opensearch.jvm_heap or "-Xmx1g -Xms1g"

            if "master" in nodes:
                manifest += f"""
  - component: masters
    replicas: {nodes["master"]}
    diskSize: {storage["size"]}
    resources:
      requests:
        memory: "{default_resources["requests"]["memory"]}"
        cpu: "{default_resources["requests"]["cpu"]}"
      limits:
        memory: "{default_resources["limits"]["memory"]}"
        cpu: "{default_resources["limits"]["cpu"]}"
    jvm: "{default_jvm}"
    env:
      - name: OPENSEARCH_INITIAL_ADMIN_PASSWORD
        valueFrom:
          secretKeyRef:
            name: admin-credentials-secret
            key: password
    roles:
      - "master"
"""

            if "data" in nodes:
                manifest += f"""
  - component: data
    replicas: {nodes["data"]}
    diskSize: {storage["size"]}
    resources:
      requests:
        memory: "{default_resources["requests"]["memory"]}"
        cpu: "{default_resources["requests"]["cpu"]}"
      limits:
        memory: "{default_resources["limits"]["memory"]}"
        cpu: "{default_resources["limits"]["cpu"]}"
    jvm: "{default_jvm}"
    env:
      - name: OPENSEARCH_INITIAL_ADMIN_PASSWORD
        valueFrom:
          secretKeyRef:
            name: admin-credentials-secret
            key: password
    roles:
      - "data"
      - "ingest"
"""

        return manifest

    def _create_security_secrets(
        self,
        k8s_manager: KubernetesManager,
        namespace: str,
        security_config: Dict[str, Any],
    ) -> bool:
        """Create security secrets for OpenSearch cluster."""
        try:
            import base64

            # Create admin credentials secret
            admin_secret_name = security_config.get(
                "admin_secret_name", "admin-credentials-secret"
            )
            username = security_config.get("username", "admin")

            # Generate secure password if not provided
            if "password" not in security_config:
                password = self._generate_secure_password()
                self.logger.info(f"Generated secure admin password: {password}")
            else:
                password = security_config["password"]

            admin_secret_manifest = f"""
apiVersion: v1
kind: Secret
metadata:
  name: {admin_secret_name}
  namespace: {namespace}
type: Opaque
data:
  username: {base64.b64encode(username.encode()).decode()}
  password: {base64.b64encode(password.encode()).decode()}
"""

            if not k8s_manager.apply_yaml_manifest(admin_secret_manifest, namespace):
                self.logger.error(
                    f"Failed to create admin credentials secret {admin_secret_name}"
                )
                return False

            self.logger.info(f"Created admin credentials secret {admin_secret_name}")

            # Create basic security config secret (minimal config)
            security_secret_name = security_config.get(
                "security_config_secret_name", "securityconfig-secret"
            )

            # Generate password hash for the actual password
            import bcrypt

            password_hash = bcrypt.hashpw(
                password.encode("utf-8"), bcrypt.gensalt(rounds=12)
            ).decode("utf-8")

            # Basic security configuration
            security_config_yaml = {
                "config.yml": """
_meta:
  type: "config"
  config_version: 2

config:
  dynamic:
    authc:
      basic_internal_auth_domain:
        description: "Authenticate via HTTP Basic against internal users database"
        http_enabled: true
        transport_enabled: true
        order: 4
        http_authenticator:
          type: basic
          challenge: true
        authentication_backend:
          type: intern
""",
                "internal_users.yml": f'''
_meta:
  type: "internalusers"
  config_version: 2

{username}:
  hash: "{password_hash}"
  reserved: true
  backend_roles:
  - "admin"
  description: "Admin user with secure password"
''',
                "roles.yml": """
_meta:
  type: "roles"
  config_version: 2

# OpenSearch 3.0.0 uses built-in static roles - don't override them
# Just provide minimal custom roles if needed
""",
                "roles_mapping.yml": f'''
_meta:
  type: "rolesmapping"
  config_version: 2

# Map admin user to built-in OpenSearch roles
all_access:
  reserved: false
  backend_roles:
  - "admin"
  users:
  - "{username}"

security_rest_api_access:
  reserved: false
  backend_roles:
  - "admin"
  users:
  - "{username}"
''',
                "action_groups.yml": """
_meta:
  type: "actiongroups"
  config_version: 2

# OpenSearch 3.0.0 uses built-in action groups - don't override them
""",
                "tenants.yml": """
_meta:
  type: "tenants"
  config_version: 2

admin_tenant:
  reserved: true
  description: "Admin tenant"
""",
                "nodes_dn.yml": """
_meta:
  type: "nodesdn"
  config_version: 2
""",
                "whitelist.yml": """
_meta:
  type: "whitelist"
  config_version: 2

config:
  enabled: false
""",
            }

            security_secret_data = {}
            for filename, content in security_config_yaml.items():
                security_secret_data[filename] = base64.b64encode(
                    content.encode()
                ).decode()

            security_secret_manifest = f"""
apiVersion: v1
kind: Secret
metadata:
  name: {security_secret_name}
  namespace: {namespace}
type: Opaque
data:
"""
            for key, value in security_secret_data.items():
                security_secret_manifest += f"  {key}: {value}\n"

            if not k8s_manager.apply_yaml_manifest(security_secret_manifest, namespace):
                self.logger.error(
                    f"Failed to create security config secret {security_secret_name}"
                )
                return False

            self.logger.info(f"Created security config secret {security_secret_name}")
            return True

        except Exception as e:
            self.logger.error(f"Error creating security secrets: {e}")
            return False

    def _generate_secure_password(self) -> str:
        """Generate a secure password that meets OpenSearch requirements.

        Requirements:
        - Minimum 8 characters
        - At least one uppercase letter
        - At least one lowercase letter
        - At least one digit
        - At least one special character
        """
        import secrets
        import string

        # Define character sets
        uppercase = string.ascii_uppercase
        lowercase = string.ascii_lowercase
        digits = string.digits
        special = "!@#$%^&*()_+-=[]{}|;:,.<>?"

        # Ensure at least one character from each required set
        password_chars = [
            secrets.choice(uppercase),
            secrets.choice(lowercase),
            secrets.choice(digits),
            secrets.choice(special),
        ]

        # Fill remaining positions with random characters from all sets
        all_chars = uppercase + lowercase + digits + special
        for _ in range(8):  # Total length will be 12 characters
            password_chars.append(secrets.choice(all_chars))

        # Shuffle the password to avoid predictable patterns
        secrets.SystemRandom().shuffle(password_chars)

        return "".join(password_chars)

    def _fix_cluster_allocation_settings(
        self, cluster_name: str, namespace: str, security_config: Dict[str, Any]
    ) -> bool:
        """Fix cluster allocation settings after deployment to enable replica allocation."""
        import time
        import subprocess
        import json

        try:
            # Wait for cluster to be accessible
            max_retries = 30
            retry_delay = 10

            username = security_config.get("username", "admin")
            password = security_config.get("password", "Admin123!")

            self.logger.info(
                "Waiting for cluster to be accessible before fixing allocation settings..."
            )

            for attempt in range(max_retries):
                try:
                    # Check if cluster is accessible (only check for successful response, ignore health status)
                    check_cmd = [
                        "kubectl",
                        "exec",
                        f"{cluster_name}-nodes-0",
                        "-n",
                        namespace,
                        "--",
                        "curl",
                        "-k",
                        "-u",
                        f"{username}:{password}",
                        "--silent",
                        "--fail",
                        "--max-time",
                        "10",
                        "https://localhost:9200/_cluster/health",
                    ]

                    result = subprocess.run(
                        check_cmd, capture_output=True, text=True, timeout=15
                    )

                    if result.returncode == 0:
                        self.logger.info(
                            f"Cluster is accessible (attempt {attempt + 1})"
                        )
                        break
                    else:
                        self.logger.debug(
                            f"Cluster not yet accessible (attempt {attempt + 1}/{max_retries}), waiting {retry_delay}s..."
                        )
                        time.sleep(retry_delay)

                except subprocess.TimeoutExpired:
                    self.logger.debug(
                        f"Timeout checking cluster accessibility (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_delay)

            else:
                self.logger.error(
                    "Cluster did not become accessible within timeout period"
                )
                return False

            # Check current allocation settings
            self.logger.info("Checking current cluster allocation settings...")

            get_settings_cmd = [
                "kubectl",
                "exec",
                f"{cluster_name}-nodes-0",
                "-n",
                namespace,
                "--",
                "curl",
                "-k",
                "-u",
                f"{username}:{password}",
                "--silent",
                "--max-time",
                "10",
                "https://localhost:9200/_cluster/settings",
            ]

            result = subprocess.run(
                get_settings_cmd, capture_output=True, text=True, timeout=15
            )

            if result.returncode != 0:
                self.logger.error(f"Failed to get cluster settings: {result.stderr}")
                return False

            try:
                settings = json.loads(result.stdout)
                current_enable = (
                    settings.get("transient", {})
                    .get("cluster", {})
                    .get("routing", {})
                    .get("allocation", {})
                    .get("enable")
                )

                if current_enable == "primaries":
                    self.logger.info(
                        "Found allocation.enable=primaries, fixing to enable replica allocation..."
                    )

                    # Enable full allocation
                    fix_cmd = [
                        "kubectl",
                        "exec",
                        f"{cluster_name}-nodes-0",
                        "-n",
                        namespace,
                        "--",
                        "curl",
                        "-k",
                        "-u",
                        f"{username}:{password}",
                        "-X",
                        "PUT",
                        "--silent",
                        "--max-time",
                        "10",
                        "https://localhost:9200/_cluster/settings",
                        "-H",
                        "Content-Type: application/json",
                        "-d",
                        '{"transient":{"cluster.routing.allocation.enable":"all"}}',
                    ]

                    result = subprocess.run(
                        fix_cmd, capture_output=True, text=True, timeout=15
                    )

                    if result.returncode == 0:
                        self.logger.info("Successfully enabled replica allocation")

                        # Verify cluster health improved
                        time.sleep(5)
                        health_cmd = [
                            "kubectl",
                            "exec",
                            f"{cluster_name}-nodes-0",
                            "-n",
                            namespace,
                            "--",
                            "curl",
                            "-k",
                            "-u",
                            f"{username}:{password}",
                            "--silent",
                            "--max-time",
                            "10",
                            "https://localhost:9200/_cluster/health",
                        ]

                        health_result = subprocess.run(
                            health_cmd, capture_output=True, text=True, timeout=15
                        )
                        if health_result.returncode == 0:
                            try:
                                health_data = json.loads(health_result.stdout)
                                status = health_data.get("status", "unknown")
                                self.logger.info(
                                    f"Cluster health after allocation fix: {status}"
                                )
                            except json.JSONDecodeError:
                                self.logger.warning(
                                    "Could not parse cluster health response"
                                )

                        return True
                    else:
                        self.logger.error(
                            f"Failed to enable replica allocation: {result.stderr}"
                        )
                        return False

                else:
                    self.logger.info(
                        f"Allocation setting is already correct: {current_enable}"
                    )
                    return True

            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse cluster settings response: {e}")
                return False

        except Exception as e:
            self.logger.error(
                f"Exception while fixing cluster allocation settings: {e}"
            )
            return False


class UpdateClusterAllocationSettingsAction(BaseAction):
    """Action to update cluster allocation settings."""

    action_name = "update_cluster_allocation_settings"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        cluster_name = self.config.opensearch.cluster_name
        namespace = self.config.opensearch.operator_namespace

        # Get security config from global config
        security_config = {
            "enabled": self.config.opensearch.security.enabled,
            "username": self.config.opensearch.security.username,
            "password": self.config.opensearch.security.password,
            "use_ssl": self.config.opensearch.security.use_ssl,
        }

        # Get allocation settings from parameters
        allocation_enable = params.get("allocation_enable", "all")
        disk_watermarks = params.get("disk_watermarks", {})
        concurrent_recoveries = params.get("concurrent_recoveries")
        recovery_speed = params.get("recovery_speed")
        allow_rebalance = params.get("allow_rebalance")

        # Only proceed if security is enabled (needed for authentication)
        if not security_config.get("enabled", False):
            return ActionResult(
                True, "Security is disabled, skipping allocation settings update"
            )

        self.logger.info(
            f"Updating cluster allocation settings (enable: {allocation_enable})"
        )

        try:
            from oko_test_harness.utils.opensearch_client import (
                KubernetesOpenSearchClient,
            )

            client = KubernetesOpenSearchClient.from_security_config(
                self.config.opensearch.security, namespace, cluster_name
            )

            with client:
                # Build settings to update
                settings_data = {"transient": {}}

                # Set allocation enable setting
                settings_data["transient"]["cluster.routing.allocation.enable"] = (
                    allocation_enable
                )

                # Set disk watermarks if provided
                if disk_watermarks:
                    if "low" in disk_watermarks:
                        settings_data["transient"][
                            "cluster.routing.allocation.disk.watermark.low"
                        ] = disk_watermarks["low"]
                    if "high" in disk_watermarks:
                        settings_data["transient"][
                            "cluster.routing.allocation.disk.watermark.high"
                        ] = disk_watermarks["high"]
                    if "flood_stage" in disk_watermarks:
                        settings_data["transient"][
                            "cluster.routing.allocation.disk.watermark.flood_stage"
                        ] = disk_watermarks["flood_stage"]

                # Set concurrent recoveries if provided
                if concurrent_recoveries is not None:
                    settings_data["transient"][
                        "cluster.routing.allocation.node_concurrent_recoveries"
                    ] = concurrent_recoveries

                # Set recovery speed if provided
                if recovery_speed is not None:
                    settings_data["transient"]["indices.recovery.max_bytes_per_sec"] = (
                        recovery_speed
                    )

                # Set allow rebalance if provided
                if allow_rebalance is not None:
                    settings_data["transient"]["cluster.routing.rebalance.enable"] = (
                        allow_rebalance
                    )

                # Update cluster settings
                if client.update_cluster_settings(settings_data):
                    # Verify settings were applied
                    import time

                    time.sleep(2)
                    current_settings = client.get_cluster_settings()
                    current_enable = (
                        current_settings.get("transient", {})
                        .get("cluster", {})
                        .get("routing", {})
                        .get("allocation", {})
                        .get("enable")
                    )
                    self.logger.info(
                        f"Verified allocation settings - enable: {current_enable}"
                    )

                    return ActionResult(
                        True,
                        f"Cluster allocation settings updated successfully (enable: {allocation_enable})",
                    )
                else:
                    return ActionResult(
                        False, "Failed to update cluster allocation settings"
                    )

        except Exception as e:
            return ActionResult(
                False, f"Error updating cluster allocation settings: {e}"
            )


class DeleteClusterAction(BaseAction):
    """Action to delete an OpenSearch cluster."""

    action_name = "delete_cluster"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        cluster_name = params.get("cluster_name", self.config.opensearch.cluster_name)
        namespace = params.get("namespace", self.config.opensearch.operator_namespace)
        force = params.get("force", False)
        wait_for_completion = params.get("wait_for_completion", True)
        timeout = params.get("timeout", "5m")

        self.logger.info(f"Deleting OpenSearch cluster: {cluster_name}")

        k8s_manager = KubernetesManager()
        if not k8s_manager.load_config():
            return ActionResult(False, "Failed to load Kubernetes config")

        try:
            # Delete the OpenSearch cluster resource
            cmd = [
                "kubectl",
                "delete",
                "opensearchcluster",
                cluster_name,
                "-n",
                namespace,
            ]
            if force:
                cmd.append("--force")

            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                self.logger.info(
                    f"OpenSearch cluster {cluster_name} deleted successfully"
                )

                # Also clean up security secrets if they exist
                self._cleanup_security_secrets(namespace)

                return ActionResult(
                    True, f"Cluster {cluster_name} deleted successfully"
                )
            else:
                return ActionResult(False, f"Failed to delete cluster: {result.stderr}")

        except Exception as e:
            return ActionResult(False, f"Error deleting cluster: {e}")

    def _cleanup_security_secrets(self, namespace: str) -> None:
        """Clean up security-related secrets."""
        try:
            security_secrets = [
                "admin-credentials-secret",
                "securityconfig-secret",
                f"{self.config.opensearch.cluster_name}-admin-password",
            ]

            for secret_name in security_secrets:
                try:
                    result = subprocess.run(
                        [
                            "kubectl",
                            "delete",
                            "secret",
                            secret_name,
                            "-n",
                            namespace,
                            "--ignore-not-found=true",
                        ],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )

                    if result.returncode == 0:
                        self.logger.info(f"Cleaned up security secret: {secret_name}")
                    elif result.stderr and "not found" not in result.stderr.lower():
                        self.logger.warning(
                            f"Failed to delete secret {secret_name}: {result.stderr}"
                        )

                except subprocess.TimeoutExpired:
                    self.logger.warning(f"Timeout deleting secret {secret_name}")
                except Exception as e:
                    self.logger.warning(f"Error deleting secret {secret_name}: {e}")

        except Exception as e:
            self.logger.warning(f"Error during security secrets cleanup: {e}")


class CleanupClusterAction(BaseAction):
    """Action to clean up test resources."""

    action_name = "cleanup_cluster"

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)

        remove_cluster = params.get("remove_cluster", True)
        cleanup_docker = params.get("cleanup_docker", True)
        cluster_name = params.get("cluster_name", self.config.kubernetes.cluster_name)
        provider = params.get("provider", self.config.kubernetes.provider)

        self.logger.info("Cleaning up test resources")

        success = True
        messages = []

        if remove_cluster:
            if provider == "kind":
                kind_manager = KindClusterManager()
                if kind_manager.delete_cluster(cluster_name):
                    messages.append(f"Deleted Kind cluster {cluster_name}")
                else:
                    success = False
                    messages.append(f"Failed to delete Kind cluster {cluster_name}")
            elif provider == "minikube":
                minikube_manager = MinikubeClusterManager()
                if minikube_manager.delete_cluster(cluster_name):
                    messages.append(f"Deleted Minikube cluster {cluster_name}")
                else:
                    success = False
                    messages.append(f"Failed to delete Minikube cluster {cluster_name}")
            else:
                messages.append(f"Cleanup not supported for provider: {provider}")

        if cleanup_docker:
            try:
                # Clean up any dangling containers/volumes
                subprocess.run(
                    ["docker", "system", "prune", "-f"], capture_output=True, check=True
                )
                messages.append("Cleaned up Docker resources")
            except Exception as e:
                self.logger.warning(f"Docker cleanup failed: {e}")

        return ActionResult(success, "; ".join(messages))
