"""Data operations actions."""

import json
import random
import time
from typing import Any, Dict, List
from datetime import datetime

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult
from oko_test_harness.utils.opensearch_client import KubernetesOpenSearchClient


class IndexDocumentsAction(BaseAction):
    """Action to index documents into OpenSearch."""
    
    action_name = "index_documents"
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)
        
        count = params.get('count', 1000)
        index = self._substitute_template_vars(params.get('index', 'test-index'))
        document_template = params.get('document_template')
        bulk_size = params.get('bulk_size', 100)
        threads = params.get('threads', 1)
        rate_limit = params.get('rate_limit')
        replicas = params.get('replicas', 0)  # Default to 0 replicas for testing
        shards = params.get('shards', 1)
        namespace = params.get('namespace', self.config.opensearch.operator_namespace)
        service_name = params.get('service_name', self.config.opensearch.service_name)
        
        self.logger.info(f"Indexing {count} documents to index '{index}'")
        
        try:
            client = KubernetesOpenSearchClient.from_security_config(self.config.opensearch.security, namespace, service_name)
            with client:
                # Delete and recreate index to ensure proper mapping
                try:
                    client.delete_index(index)
                    self.logger.info(f"Deleted existing index '{index}' to ensure clean mapping")
                except Exception:
                    pass  # Index might not exist, which is fine
                
                # Create index with proper mapping and configurable replicas
                if not client.create_index(index, replicas=replicas, shards=shards):
                    return ActionResult(False, f"Failed to create index '{index}'")
                
                # Generate documents
                documents = self._generate_documents(count, document_template)
                
                # Log first few documents for debugging
                if len(documents) > 0:
                    self.logger.info(f"Sample documents to be indexed: {documents[:3]}")
                
                # Index documents
                successful, failed = client.bulk_index(index, documents, bulk_size)
                
                if failed == 0:
                    return ActionResult(True, f"Successfully indexed {successful} documents")
                else:
                    return ActionResult(
                        successful > failed,
                        f"Indexed {successful} documents, {failed} failed"
                    )
                    
        except Exception as e:
            return ActionResult(False, f"Failed to index documents: {e}")
    
    def _generate_documents(self, count: int, template: str = None) -> List[Dict[str, Any]]:
        """Generate test documents."""
        documents = []
        
        for i in range(count):
            if template:
                doc = self._generate_from_template(template, i)
            else:
                doc = {
                    "id": i,
                    "timestamp": datetime.now().isoformat(),
                    "message": f"Test document {i}",
                    "level": random.choice(["INFO", "WARN", "ERROR"]),
                    "service": random.choice(["web", "api", "db"]),
                    "value": random.randint(1, 1000)
                }
            documents.append(doc)
        
        return documents
    
    def _generate_from_template(self, template: str, doc_id: int) -> Dict[str, Any]:
        """Generate document from JSON template."""
        # Replace template variables
        content = template.replace('{id}', str(doc_id))
        content = content.replace('{timestamp}', datetime.now().isoformat())
        
        # Handle random choices
        import re
        def replace_random(match):
            choices = match.group(1).split(',')
            return random.choice([c.strip() for c in choices])
        
        content = re.sub(r'\{random:([^}]+)\}', replace_random, content)
        
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # Fallback to simple document
            return {"content": content, "id": doc_id}


class QueryDocumentsAction(BaseAction):
    """Action to query documents from OpenSearch."""
    
    action_name = "query_documents"
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)
        
        index = params.get('index', 'test-index-*')
        query = params.get('query', {'match_all': {}})
        expected_count = params.get('expected_count')
        timeout_str = params.get('timeout', '30s')
        namespace = params.get('namespace', self.config.opensearch.operator_namespace)
        service_name = params.get('service_name', self.config.opensearch.service_name)
        
        # Parse query if it's a string
        if isinstance(query, str):
            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                return ActionResult(False, f"Invalid query JSON: {query}")
        
        self.logger.info(f"Querying index '{index}'")
        
        try:
            client = KubernetesOpenSearchClient.from_security_config(self.config.opensearch.security, namespace, service_name)
            with client:
                # Execute search
                result = client.search(index, query, size=0)  # Just get count
                hit_count = result["hits"]["total"]["value"]
                
                if expected_count is not None:
                    if hit_count == expected_count:
                        return ActionResult(True, f"Query returned {hit_count} documents as expected")
                    else:
                        return ActionResult(
                            False, 
                            f"Expected {expected_count} documents, got {hit_count}"
                        )
                else:
                    return ActionResult(True, f"Query returned {hit_count} documents")
                    
        except Exception as e:
            return ActionResult(False, f"Failed to query documents: {e}")


class CreateSnapshotAction(BaseAction):
    """Action to create a snapshot."""
    
    action_name = "create_snapshot"
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)
        
        repository = params.get('repository', 'test-repo')
        snapshot_name = self._substitute_template_vars(
            params.get('snapshot_name', f'snapshot-{int(time.time())}')
        )
        indices = params.get('indices', ['*'])
        include_global_state = params.get('include_global_state', False)
        namespace = params.get('namespace', self.config.opensearch.operator_namespace)
        service_name = params.get('service_name', self.config.opensearch.service_name)
        
        self.logger.info(f"Creating snapshot '{snapshot_name}' in repository '{repository}'")
        
        try:
            client = KubernetesOpenSearchClient.from_security_config(self.config.opensearch.security, namespace, service_name)
            with client:
                # First, register the snapshot repository if it doesn't exist
                repo_body = {
                    "type": "fs",
                    "settings": {
                        "location": f"/usr/share/opensearch/data/snapshots/{repository}",
                        "compress": True
                    }
                }
                
                # Try to register the repository
                response = client.session.put(
                    f"{client.base_url}/_snapshot/{repository}",
                    json=repo_body
                )
                
                if response.status_code not in [200, 201]:
                    self.logger.warning(f"Repository registration returned status {response.status_code}: {response.text}")
                    # Try to check if repository already exists
                    check_response = client.session.get(f"{client.base_url}/_snapshot/{repository}")
                    if check_response.status_code != 200:
                        return ActionResult(False, f"Failed to register repository: {response.text}")
                    else:
                        self.logger.info(f"Repository '{repository}' already exists")
                
                # Wait a moment for repository to be ready
                time.sleep(1)
                
                # Create snapshot
                snapshot_body = {
                    "indices": ",".join(indices) if isinstance(indices, list) else indices,
                    "include_global_state": include_global_state
                }
                
                response = client.session.put(
                    f"{client.base_url}/_snapshot/{repository}/{snapshot_name}",
                    json=snapshot_body
                )
                
                if response.status_code in [200, 201]:
                    return ActionResult(True, f"Snapshot '{snapshot_name}' created successfully")
                else:
                    return ActionResult(False, f"Failed to create snapshot: {response.text}")
                    
        except Exception as e:
            return ActionResult(False, f"Failed to create snapshot: {e}")


class RestoreSnapshotAction(BaseAction):
    """Action to restore a snapshot."""
    
    action_name = "restore_snapshot"
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        params = self._merge_params(params)
        
        repository = params.get('repository', 'test-repo')
        snapshot_name = params.get('snapshot_name')
        target_indices = params.get('target_indices')
        rename_pattern = params.get('rename_pattern')
        rename_replacement = params.get('rename_replacement')
        namespace = params.get('namespace', self.config.opensearch.operator_namespace)
        service_name = params.get('service_name', self.config.opensearch.service_name)
        
        if not snapshot_name:
            return ActionResult(False, "snapshot_name is required")
        
        self.logger.info(f"Restoring snapshot '{snapshot_name}' from repository '{repository}'")
        
        try:
            client = KubernetesOpenSearchClient.from_security_config(self.config.opensearch.security, namespace, service_name)
            with client:
                restore_body = {}
                
                if target_indices:
                    restore_body["indices"] = ",".join(target_indices) if isinstance(target_indices, list) else target_indices
                
                if rename_pattern and rename_replacement:
                    restore_body["rename_pattern"] = rename_pattern
                    restore_body["rename_replacement"] = rename_replacement
                
                response = client.session.post(
                    f"{client.base_url}/_snapshot/{repository}/{snapshot_name}/_restore",
                    json=restore_body
                )
                
                if response.status_code == 200:
                    return ActionResult(True, f"Snapshot '{snapshot_name}' restored successfully")
                else:
                    return ActionResult(False, f"Failed to restore snapshot: {response.text}")
                    
        except Exception as e:
            return ActionResult(False, f"Failed to restore snapshot: {e}")