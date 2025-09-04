"""Kubernetes utilities and helpers."""

import subprocess
import time
from typing import Optional, Dict, Any, List
from loguru import logger
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException


class KubernetesManager:
    """Manages Kubernetes cluster operations."""
    
    _config_loaded = False
    _config_load_error = None

    def __init__(self):
        self.v1_client: Optional[client.CoreV1Api] = None
        self.apps_v1_client: Optional[client.AppsV1Api] = None
        self.custom_objects_client: Optional[client.CustomObjectsApi] = None
        
    def load_config(self) -> bool:
        """Load Kubernetes configuration (only once per process)."""
        if self._config_loaded:
            self._initialize_clients()
            return True
            
        if self._config_load_error:
            return False
            
        try:
            k8s_config.load_kube_config()
            KubernetesManager._config_loaded = True
            self._initialize_clients()
            logger.debug("Kubernetes config loaded successfully")
            return True
        except Exception as e:
            KubernetesManager._config_load_error = e
            logger.error(f"Failed to load Kubernetes config: {e}")
            return False
    
    def _initialize_clients(self):
        """Initialize Kubernetes API clients."""
        self.v1_client = client.CoreV1Api()
        self.apps_v1_client = client.AppsV1Api()
        self.custom_objects_client = client.CustomObjectsApi()
    
    def create_namespace(self, name: str) -> bool:
        """Create a Kubernetes namespace."""
        try:
            namespace = client.V1Namespace(
                metadata=client.V1ObjectMeta(name=name)
            )
            self.v1_client.create_namespace(namespace)
            logger.info(f"Created namespace: {name}")
            return True
        except ApiException as e:
            if e.status == 409:  # Already exists
                logger.info(f"Namespace {name} already exists")
                return True
            logger.error(f"Failed to create namespace {name}: {e}")
            return False
    
    def delete_namespace(self, name: str, wait_for_deletion: bool = True) -> bool:
        """Delete a Kubernetes namespace."""
        try:
            self.v1_client.delete_namespace(name)
            logger.info(f"Deleted namespace: {name}")
            
            if wait_for_deletion:
                return self.wait_for_namespace_deletion(name)
            return True
        except ApiException as e:
            if e.status == 404:  # Not found
                logger.info(f"Namespace {name} not found")
                return True
            logger.error(f"Failed to delete namespace {name}: {e}")
            return False
    
    def wait_for_namespace_deletion(self, name: str, timeout: int = 300) -> bool:
        """Wait for a namespace to be deleted."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                self.v1_client.read_namespace(name)
                time.sleep(5)
            except ApiException as e:
                if e.status == 404:
                    logger.info(f"Namespace {name} successfully deleted")
                    return True
                logger.error(f"Error checking namespace {name}: {e}")
                return False
        
        logger.error(f"Timeout waiting for namespace {name} deletion")
        return False
    
    def apply_yaml_manifest(self, yaml_content: str, namespace: Optional[str] = None) -> bool:
        """Apply a YAML manifest to the cluster."""
        import tempfile
        import os
        
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(yaml_content)
                temp_file = f.name
            
            cmd = ['kubectl', 'apply', '-f', temp_file]
            if namespace:
                cmd.extend(['-n', namespace])
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.debug("Successfully applied YAML manifest")
                return True
            else:
                logger.error(f"Failed to apply YAML manifest: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error applying YAML manifest: {e}")
            return False
        finally:
            if 'temp_file' in locals():
                os.unlink(temp_file)
    
    def wait_for_deployment_ready(self, name: str, namespace: str, timeout: int = 600) -> bool:
        """Wait for a deployment to be ready."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                deployment = self.apps_v1_client.read_namespaced_deployment(name, namespace)
                
                if (deployment.status.ready_replicas == deployment.spec.replicas and
                    deployment.status.replicas == deployment.spec.replicas):
                    logger.info(f"Deployment {name} is ready")
                    return True
                    
                time.sleep(10)
            except ApiException as e:
                if e.status != 404:
                    logger.error(f"Error checking deployment {name}: {e}")
                    return False
                time.sleep(5)
        
        logger.error(f"Timeout waiting for deployment {name} to be ready")
        return False
    
    def get_pods(self, namespace: str, label_selector: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get pods in a namespace."""
        try:
            pods = self.v1_client.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector
            )
            
            return [{
                'name': pod.metadata.name,
                'status': pod.status.phase,
                'ready': all(c.ready for c in pod.status.container_statuses or []),
                'labels': pod.metadata.labels,
                'restart_count': sum(c.restart_count for c in pod.status.container_statuses or []),
                'crash_loop_backoff': any(
                    c.state and c.state.waiting and c.state.waiting.reason == 'CrashLoopBackOff' 
                    for c in pod.status.container_statuses or []
                ),
                'containers': [{
                    'name': container.name,
                    'resources': {
                        'requests': container.resources.requests or {} if container.resources else {},
                        'limits': container.resources.limits or {} if container.resources else {}
                    }
                } for container in pod.spec.containers] if pod.spec.containers else []
            } for pod in pods.items]
            
        except ApiException as e:
            logger.error(f"Error getting pods in namespace {namespace}: {e}")
            return []
    
    def exec_in_pod(self, namespace: str, pod_name: str, command: List[str], container: Optional[str] = None) -> Dict[str, Any]:
        """Execute a command in a pod."""
        try:
            from kubernetes.stream import stream
            
            resp = stream(
                self.v1_client.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                command=command,
                container=container,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False
            )
            
            return {
                'success': True,
                'output': resp,
                'error': None
            }
            
        except ApiException as e:
            logger.error(f"Error executing command in pod {pod_name}: {e}")
            return {
                'success': False,
                'output': None,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Error executing command in pod {pod_name}: {e}")
            return {
                'success': False,
                'output': None,
                'error': str(e)
            }
    
    def get_pvcs(self, namespace: str) -> List[Dict[str, Any]]:
        """Get persistent volume claims in a namespace."""
        try:
            pvcs = self.v1_client.list_namespaced_persistent_volume_claim(namespace)
            
            return [{
                'name': pvc.metadata.name,
                'size': pvc.spec.resources.requests.get('storage', '0Gi') if pvc.spec.resources.requests else '0Gi',
                'storage_class': pvc.spec.storage_class_name,
                'status': pvc.status.phase,
                'labels': pvc.metadata.labels
            } for pvc in pvcs.items]
            
        except ApiException as e:
            logger.error(f"Error getting PVCs in namespace {namespace}: {e}")
            return []
    
    def get_services(self, namespace: str) -> List[Dict[str, Any]]:
        """Get services in a namespace."""
        try:
            services = self.v1_client.list_namespaced_service(namespace)
            
            return [{
                'name': service.metadata.name,
                'type': service.spec.type,
                'ports': [{
                    'port': port.port,
                    'target_port': port.target_port,
                    'protocol': port.protocol,
                    'name': port.name
                } for port in service.spec.ports or []],
                'labels': service.metadata.labels
            } for service in services.items]
            
        except ApiException as e:
            logger.error(f"Error getting services in namespace {namespace}: {e}")
            return []
    
    def check_crash_loops(self, namespace: str, label_selector: Optional[str] = None, 
                         max_restart_count: int = 5) -> Dict[str, Any]:
        """Check for crash loops in pods."""
        try:
            pods = self.v1_client.list_namespaced_pod(
                namespace=namespace,
                label_selector=label_selector
            )
            
            crash_loops = []
            warnings = []
            
            for pod in pods.items:
                pod_name = pod.metadata.name
                pod_status = pod.status.phase
                
                if not pod.status.container_statuses:
                    continue
                
                total_restarts = sum(c.restart_count for c in pod.status.container_statuses)
                has_crash_loop = any(
                    c.state and c.state.waiting and c.state.waiting.reason == 'CrashLoopBackOff' 
                    for c in pod.status.container_statuses
                )
                
                # Get last termination reason for error details
                last_error = None
                for container_status in pod.status.container_statuses:
                    if (container_status.last_state and 
                        container_status.last_state.terminated and 
                        container_status.last_state.terminated.message):
                        last_error = container_status.last_state.terminated.message
                        break
                
                crash_loop_info = {
                    'pod_name': pod_name,
                    'namespace': namespace,
                    'restart_count': total_restarts,
                    'status': pod_status,
                    'crash_loop_backoff': has_crash_loop,
                    'last_error': last_error
                }
                
                if has_crash_loop or total_restarts > max_restart_count:
                    crash_loops.append(crash_loop_info)
                elif total_restarts > 0:
                    warnings.append(crash_loop_info)
            
            return {
                'crash_loops': crash_loops,
                'warnings': warnings,
                'total_pods': len(pods.items),
                'has_crash_loops': len(crash_loops) > 0
            }
            
        except ApiException as e:
            logger.error(f"Error checking crash loops in namespace {namespace}: {e}")
            return {
                'crash_loops': [],
                'warnings': [],
                'total_pods': 0,
                'has_crash_loops': False,
                'error': str(e)
            }


class MinikubeClusterManager:
    """Manages Minikube clusters."""

    def _validate_system_resources(self) -> bool:
        """Validate system has adequate resources for Minikube cluster."""
        try:
            # Check available memory (rough check)
            result = subprocess.run(['free', '-m'], capture_output=True, text=True)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
                    memory_line = lines[1].split()
                    available_mb = int(memory_line[6]) if len(memory_line) > 6 else int(memory_line[3])
                    if available_mb < 6144:  # 6GB minimum (4GB for cluster + 2GB buffer)
                        logger.warning(f"Low available memory: {available_mb}MB (recommended: >6144MB)")
                        return False
            
            # Check Docker is running
            result = subprocess.run(['docker', 'info'], capture_output=True, text=True)
            if result.returncode != 0:
                logger.error("Docker is not running or accessible")
                return False
            
            return True
        except Exception as e:
            logger.warning(f"Could not validate system resources: {e}")
            return True  # Don't block on validation failure
    
    def create_cluster(self, cluster_name: str, nodes: int = 1, k8s_version: str = "v1.28.0", 
                      extra_config: Optional[str] = None, max_retries: int = 2) -> bool:
        """Create a Minikube cluster with retry logic."""
        # Validate system resources first
        if not self._validate_system_resources():
            logger.error("System resource validation failed")
            return False
        
        for attempt in range(max_retries):
            try:
                # Clean up any existing partial cluster first
                if self.cluster_exists(cluster_name):
                    logger.info(f"Cleaning up existing cluster: {cluster_name}")
                    self.delete_cluster(cluster_name)
                    time.sleep(5)
                
                cmd = ['minikube', 'start', '--profile', cluster_name]
                
                if nodes > 1:
                    cmd.extend(['--nodes', str(nodes)])
                
                if k8s_version:
                    cmd.extend(['--kubernetes-version', k8s_version])
                
                # Add resource configuration for OpenSearch workloads
                cmd.extend(['--memory=4096', '--cpus=2'])
                
                # Enable necessary addons
                cmd.extend(['--addons=storage-provisioner,default-storageclass'])
                
                logger.info(f"Creating Minikube cluster: {cluster_name} (attempt {attempt + 1}/{max_retries})")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
                
                if result.returncode == 0:
                    logger.info(f"Created Minikube cluster: {cluster_name}")
                    return True
                else:
                    logger.error(f"Failed to create Minikube cluster: {result.stderr}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying cluster creation in 10 seconds...")
                        time.sleep(10)
                    
            except subprocess.TimeoutExpired:
                logger.error(f"Timeout creating Minikube cluster (attempt {attempt + 1}/{max_retries})")
                # Cleanup failed cluster attempt
                try:
                    self.delete_cluster(cluster_name)
                except Exception:
                    pass
                
                if attempt < max_retries - 1:
                    logger.info(f"Retrying cluster creation in 10 seconds...")
                    time.sleep(10)
                    
            except Exception as e:
                logger.error(f"Error creating Minikube cluster (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying cluster creation in 10 seconds...")
                    time.sleep(10)
        
        logger.error(f"Failed to create Minikube cluster after {max_retries} attempts")
        return False
    
    def delete_cluster(self, cluster_name: str) -> bool:
        """Delete a Minikube cluster."""
        try:
            result = subprocess.run(
                ['minikube', 'delete', '--profile', cluster_name],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                logger.info(f"Deleted Minikube cluster: {cluster_name}")
                return True
            else:
                logger.error(f"Failed to delete Minikube cluster: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting Minikube cluster: {e}")
            return False
    
    def cluster_exists(self, cluster_name: str) -> bool:
        """Check if a Minikube cluster exists."""
        try:
            result = subprocess.run(
                ['minikube', 'profile', 'list', '--output', 'json'],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                import json
                try:
                    profiles = json.loads(result.stdout)
                    valid_profiles = profiles.get('valid', [])
                    return any(profile.get('Name') == cluster_name for profile in valid_profiles)
                except (json.JSONDecodeError, KeyError):
                    # Fallback to simple string check
                    return cluster_name in result.stdout
            return False
            
        except Exception as e:
            logger.error(f"Error checking Minikube cluster existence: {e}")
            return False
    
    def get_status(self, cluster_name: str) -> str:
        """Get the status of a Minikube cluster."""
        try:
            result = subprocess.run(
                ['minikube', 'status', '--profile', cluster_name],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                return "Running"
            else:
                return "Stopped"
                
        except Exception as e:
            logger.error(f"Error getting Minikube cluster status: {e}")
            return "Unknown"


class KindClusterManager:
    """Manages Kind (Kubernetes in Docker) clusters."""

    def _prepare_docker_space(self) -> None:
        """Prepare Docker with adequate space for OpenSearch workloads."""
        try:
            # Clean up Docker to free space
            logger.info("Cleaning up Docker to ensure adequate space...")
            subprocess.run(['docker', 'system', 'prune', '-f'], 
                         capture_output=True, text=True)
            subprocess.run(['docker', 'image', 'prune', '-a', '-f'], 
                         capture_output=True, text=True)
            subprocess.run(['docker', 'volume', 'prune', '-f'], 
                         capture_output=True, text=True)
            
            logger.info("Docker cleanup completed")
            
        except Exception as e:
            logger.warning(f"Docker cleanup failed (non-fatal): {e}")

    def create_cluster(self, cluster_name: str, config_content: Optional[str] = None) -> bool:
        """Create a Kind cluster with expanded disk space."""
        # Increase Docker's disk space allocation for Kind nodes
        self._prepare_docker_space()
        
        cmd = ['kind', 'create', 'cluster', '--name', cluster_name]
        
        if config_content:
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(config_content)
                config_file = f.name
                
            cmd.extend(['--config', config_file])
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Created Kind cluster: {cluster_name}")
                return True
            else:
                logger.error(f"Failed to create Kind cluster: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating Kind cluster: {e}")
            return False
        finally:
            if 'config_file' in locals():
                os.unlink(config_file)
    
    def delete_cluster(self, cluster_name: str) -> bool:
        """Delete a Kind cluster."""
        try:
            result = subprocess.run(
                ['kind', 'delete', 'cluster', '--name', cluster_name],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                logger.info(f"Deleted Kind cluster: {cluster_name}")
                return True
            else:
                logger.error(f"Failed to delete Kind cluster: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting Kind cluster: {e}")
            return False
    
    def cluster_exists(self, cluster_name: str) -> bool:
        """Check if a Kind cluster exists."""
        try:
            result = subprocess.run(
                ['kind', 'get', 'clusters'],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                clusters = result.stdout.strip().split('\n')
                return cluster_name in clusters
            return False
            
        except Exception as e:
            logger.error(f"Error checking Kind cluster existence: {e}")
            return False