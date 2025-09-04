"""OpenSearch client utilities using simple HTTP requests."""

import json
import requests
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib3.exceptions import InsecureRequestWarning
import subprocess
from loguru import logger

# Suppress SSL warnings for testing
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class OpenSearchClient:
    """Simple HTTP-based OpenSearch client."""
    
    def __init__(self, host: str = "localhost", port: int = 9200,
                 username: str = "admin", password: str = "Admin123!",
                 use_ssl: bool = True, verify_ssl: bool = False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.verify_ssl = verify_ssl
        
        protocol = "https" if use_ssl else "http"
        self.base_url = f"{protocol}://{host}:{port}"
        
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = verify_ssl
        
    def health(self) -> Dict[str, Any]:
        """Get cluster health."""
        response = self.session.get(f"{self.base_url}/_cluster/health")
        response.raise_for_status()
        return response.json()
    
    def cluster_stats(self) -> Dict[str, Any]:
        """Get cluster statistics."""
        response = self.session.get(f"{self.base_url}/_cluster/stats")
        response.raise_for_status()
        return response.json()
    
    def node_stats(self) -> Dict[str, Any]:
        """Get node statistics."""
        response = self.session.get(f"{self.base_url}/_nodes/stats")
        response.raise_for_status()
        return response.json()
    
    def create_index(self, index_name: str, mapping: Optional[Dict] = None, 
                     replicas: int = 0, shards: int = 1) -> bool:
        """Create an index with configurable settings."""
        try:
            # Default mapping for typical log-like documents
            if mapping is None:
                mapping = {
                    "settings": {
                        "number_of_shards": shards,
                        "number_of_replicas": replicas
                    },
                    "mappings": {
                        "properties": {
                            "timestamp": {"type": "date"},
                            "message": {"type": "text"},
                            "level": {"type": "keyword"},  # Use keyword for exact matching
                            "service": {"type": "keyword"},
                            "value": {"type": "long"}
                        }
                    }
                }
            else:
                # Ensure settings are included if not already present
                if "settings" not in mapping:
                    mapping["settings"] = {}
                if "number_of_replicas" not in mapping["settings"]:
                    mapping["settings"]["number_of_replicas"] = replicas
                if "number_of_shards" not in mapping["settings"]:
                    mapping["settings"]["number_of_shards"] = shards
            
            response = self.session.put(
                f"{self.base_url}/{index_name}",
                json=mapping
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Created index '{index_name}' with {shards} shard(s) and {replicas} replica(s)")
                return True
            else:
                logger.error(f"Failed to create index {index_name}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e}")
            return False
    
    def delete_index(self, index_name: str) -> bool:
        """Delete an index."""
        try:
            response = self.session.delete(f"{self.base_url}/{index_name}")
            return response.status_code in [200, 404]  # 404 means already deleted
        except Exception as e:
            logger.error(f"Failed to delete index {index_name}: {e}")
            return False
    
    def index_document(self, index_name: str, document: Dict[str, Any], 
                      doc_id: Optional[str] = None) -> bool:
        """Index a single document."""
        try:
            if doc_id:
                response = self.session.put(
                    f"{self.base_url}/{index_name}/_doc/{doc_id}",
                    json=document
                )
            else:
                response = self.session.post(
                    f"{self.base_url}/{index_name}/_doc",
                    json=document
                )
            return response.status_code in [200, 201]
        except Exception as e:
            logger.error(f"Failed to index document: {e}")
            return False
    
    def bulk_index(self, index_name: str, documents: List[Dict[str, Any]], 
                   chunk_size: int = 100) -> Tuple[int, int]:
        """Bulk index documents. Returns (successful, failed) counts."""
        successful = 0
        failed = 0
        
        for i in range(0, len(documents), chunk_size):
            chunk = documents[i:i + chunk_size]
            bulk_body = []
            
            for doc in chunk:
                bulk_body.append({"index": {"_index": index_name}})
                bulk_body.append(doc)
            
            # Convert to newline-delimited JSON
            bulk_data = "\n".join(json.dumps(item) for item in bulk_body) + "\n"
            
            try:
                response = self.session.post(
                    f"{self.base_url}/_bulk",
                    data=bulk_data,
                    headers={"Content-Type": "application/x-ndjson"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    for item in result.get("items", []):
                        if "index" in item:
                            if item["index"].get("status") in [200, 201]:
                                successful += 1
                            else:
                                failed += 1
                else:
                    failed += len(chunk)
                    
            except Exception as e:
                logger.error(f"Bulk indexing failed: {e}")
                failed += len(chunk)
        
        # Refresh the index to make documents available for search
        if successful > 0:
            self.refresh_index(index_name)
        
        return successful, failed
    
    def refresh_index(self, index_name: str) -> bool:
        """Refresh an index to make documents available for search."""
        try:
            response = self.session.post(f"{self.base_url}/{index_name}/_refresh")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Failed to refresh index {index_name}: {e}")
            return False
    
    def get_index_mapping(self, index_name: str) -> Dict[str, Any]:
        """Get index mapping."""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}/_mapping")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get index mapping for {index_name}: {e}")
            return {}
    
    def get_index_settings(self, index_name: str) -> Dict[str, Any]:
        """Get index settings."""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}/_settings")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get index settings for {index_name}: {e}")
            return {}
    
    def list_indices(self) -> List[Dict[str, Any]]:
        """List all indices with their basic info."""
        try:
            response = self.session.get(f"{self.base_url}/_cat/indices?format=json")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to list indices: {e}")
            return []
    
    def update_index_replicas(self, index_name: str, replicas: int) -> bool:
        """Update replica count for an existing index."""
        try:
            settings = {
                "settings": {
                    "number_of_replicas": replicas
                }
            }
            response = self.session.put(f"{self.base_url}/{index_name}/_settings", json=settings)
            if response.status_code == 200:
                logger.info(f"Updated index '{index_name}' to {replicas} replicas")
                return True
            else:
                logger.error(f"Failed to update replicas for {index_name}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Failed to update replicas for {index_name}: {e}")
            return False
    
    def search(self, index_name: str, query: Dict[str, Any], 
              size: int = 10) -> Dict[str, Any]:
        """Search documents."""
        search_body = {
            "query": query,
            "size": size
        }
        
        response = self.session.post(
            f"{self.base_url}/{index_name}/_search",
            json=search_body
        )
        response.raise_for_status()
        return response.json()
    
    def count(self, index_name: str, query: Optional[Dict[str, Any]] = None) -> int:
        """Count documents."""
        body = {"query": query} if query else {}
        
        response = self.session.post(
            f"{self.base_url}/{index_name}/_count",
            json=body
        )
        response.raise_for_status()
        return response.json()["count"]
    
    def wait_for_cluster_health(self, status: str = "yellow", timeout: int = 300) -> bool:
        """Wait for cluster to reach specific health status with detailed reporting."""
        start_time = time.time()
        last_reported_status = None
        
        while time.time() - start_time < timeout:
            try:
                health = self.health()
                current_status = health["status"]
                
                # Report status changes or every 30 seconds
                if current_status != last_reported_status or (time.time() - start_time) % 30 < 5:
                    logger.info(f"Cluster health: {current_status} (target: {status})")
                    logger.info(f"Nodes: {health.get('number_of_nodes', 0)}, "
                              f"Data nodes: {health.get('number_of_data_nodes', 0)}")
                    logger.info(f"Active shards: {health.get('active_shards', 0)}, "
                              f"Relocating: {health.get('relocating_shards', 0)}, "
                              f"Unassigned: {health.get('unassigned_shards', 0)}")
                    last_reported_status = current_status
                
                if self._health_status_reached(current_status, status):
                    logger.info(f"Cluster health reached target: {current_status}")
                    return True
                time.sleep(5)
            except Exception as e:
                logger.warning(f"Error checking cluster health: {e}")
                time.sleep(5)
        
        # Final status report on timeout
        try:
            health = self.health()
            logger.error(f"Timeout waiting for cluster health {status}. Final status: {health['status']}")
            logger.error(f"Final cluster state - Nodes: {health.get('number_of_nodes', 0)}, "
                        f"Active shards: {health.get('active_shards', 0)}, "
                        f"Unassigned shards: {health.get('unassigned_shards', 0)}")
        except Exception as e:
            logger.error(f"Timeout waiting for cluster health {status}. Could not get final status: {e}")
        
        return False
    
    def _health_status_reached(self, current: str, target: str) -> bool:
        """Check if current health status meets or exceeds target."""
        status_order = {"red": 0, "yellow": 1, "green": 2}
        return status_order.get(current, 0) >= status_order.get(target, 0)
    
    def get_cluster_settings(self) -> Dict[str, Any]:
        """Get cluster settings."""
        try:
            response = self.session.get(f"{self.base_url}/_cluster/settings")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get cluster settings: {e}")
            return {}

    def update_cluster_settings(self, settings: Dict[str, Any]) -> bool:
        """Update cluster settings."""
        try:
            response = self.session.put(
                f"{self.base_url}/_cluster/settings",
                json=settings,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            logger.info(f"Successfully updated cluster settings: {settings}")
            return True
        except Exception as e:
            logger.error(f"Failed to update cluster settings: {e}")
            return False
    
    def get_nodes_info(self) -> Dict[str, Any]:
        """Get detailed node information."""
        try:
            response = self.session.get(f"{self.base_url}/_nodes")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get nodes info: {e}")
            return {}
    
    def using_auth(self) -> bool:
        """Check if client is using authentication."""
        return self.username is not None and self.password is not None
    
    def using_ssl(self) -> bool:
        """Check if client is using SSL."""
        return self.use_ssl


class KubernetesOpenSearchClient(OpenSearchClient):
    """OpenSearch client that connects via Kubernetes port-forward."""
    
    def __init__(self, namespace: str = "opensearch", service_name: str = "opensearch-cluster",
                 port: int = 9200, username: str = "admin", password: str = "Admin123!",
                 use_ssl: bool = None, verify_ssl: bool = False):
        self.namespace = namespace
        self.service_name = service_name
        self.k8s_port = port
        self.port_forward_process = None
        self.local_port = self._find_free_port()
        
        # Auto-detect SSL if not explicitly set
        if use_ssl is None:
            use_ssl = self._auto_detect_ssl()
        
        super().__init__(
            host="localhost", 
            port=self.local_port,
            username=username,
            password=password,
            use_ssl=use_ssl,
            verify_ssl=verify_ssl
        )
    
    def _auto_detect_ssl(self) -> bool:
        """Auto-detect if SSL is enabled by trying both protocols."""
        # This is a placeholder - in practice, we should check cluster configuration
        # For now, default to True as most production clusters use SSL
        return True
    
    @classmethod
    def from_security_config(cls, security_config, namespace: str = "opensearch", 
                           service_name: str = "opensearch-cluster", port: int = 9200):
        """Create client from security configuration object."""
        return cls(
            namespace=namespace,
            service_name=service_name,
            port=port,
            username=security_config.username,
            password=security_config.password,
            use_ssl=security_config.use_ssl,
            verify_ssl=security_config.verify_ssl
        )
    
    def _find_free_port(self) -> int:
        """Find a free local port for port forwarding."""
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]
    
    def connect(self, quiet: bool = False) -> bool:
        """Establish port-forward connection to OpenSearch."""
        try:
            # Start kubectl port-forward
            self.port_forward_process = subprocess.Popen([
                'kubectl', 'port-forward',
                f'service/{self.service_name}',
                f'{self.local_port}:{self.k8s_port}',
                '-n', self.namespace
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Wait a moment for port-forward to establish
            time.sleep(3)
            
            # Test connection
            try:
                health = self.health()
                logger.debug(f"Connected to OpenSearch cluster via port-forward (port {self.local_port})")
                return True
            except Exception as e:
                if not quiet:
                    logger.error(f"Failed to connect to OpenSearch: {e}")
                self.disconnect()
                return False
                
        except Exception as e:
            if not quiet:
                logger.error(f"Failed to establish port-forward: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close port-forward connection."""
        if self.port_forward_process:
            self.port_forward_process.terminate()
            self.port_forward_process.wait()
            self.port_forward_process = None
            logger.debug("Port-forward connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        if self.connect():
            return self
        raise ConnectionError("Failed to connect to OpenSearch")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()