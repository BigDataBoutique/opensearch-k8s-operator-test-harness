# OKO Test Harness

A comprehensive, declarative test framework for validating OpenSearch Kubernetes operator functionality, resilience, and reliability across various scenarios.

## 🎯 Overview

This test harness enables thorough testing of the OpenSearch Kubernetes operator through declarative YAML playbooks that define test scenarios. It focuses on:

- **Correctness**: Validates that the operator performs the right actions
- **Resilience**: Tests recovery from failures and unexpected conditions  
- **Reliability**: Ensures consistent behavior across different environments
- **Upgrade Safety**: Validates seamless version upgrades with zero data loss
- **Performance**: Tests behavior under load and resource constraints

## 🚀 Quick Start

### Prerequisites

- **Python 3.9+**
- **Poetry** (for dependency management)
- **Docker** (for Kind clusters)
- **kubectl** (Kubernetes CLI)
- **Kind** (optional, for local testing)
- **Helm** (required, for OpenSearch installation)
- **Git** (optional, for development chart installations)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd opensearch-k8s-operator-test-harness

# Option 1: Install with Poetry (recommended)
poetry install
poetry shell
oko-test --help

# Option 2: Run without installation
python -m oko_test_harness --help
```

### Helm Chart Setup

The test harness requires Helm for OpenSearch installation. Set up the OpenSearch Helm repository:

```bash
# Add the OpenSearch operator Helm repository
helm repo add opensearch-operator https://opensearch-project.github.io/opensearch-k8s-operator/
helm repo update

# Add the OpenSearch application Helm repository (for direct cluster deployments)
helm repo add opensearch https://opensearch-project.github.io/helm-charts/
helm repo update

# Verify available charts
helm search repo opensearch-operator
helm search repo opensearch
```

**Available Installation Methods:**

1. **Operator Charts (Default)**
   - Uses official OpenSearch Kubernetes Operator from `opensearch-operator/opensearch-operator`
   - Installs the operator that manages OpenSearch clusters
   - Default behavior in playbooks

2. **Direct Application Charts**
   - Uses OpenSearch application charts from `opensearch/opensearch`
   - Installs OpenSearch directly without an operator
   - Useful for simple deployments

3. **Development Charts**
   - Use local Helm chart directories for testing custom modifications
   - Clone from Git repositories for testing development branches
   - Perfect for testing unreleased features or custom operators

4. **Local Operator Source Code**
   - Use local operator source code for development and testing
   - Automatically builds and deploys from your local operator repository
   - Perfect for testing code changes, bug fixes, and new features

**Operator Installation (Default):**
```yaml
# In playbooks - installs OpenSearch Kubernetes Operator
- action: install_operator
  params:
    version: "2.8.0"
    namespace: "opensearch-system"
    helm_chart: "opensearch-operator/opensearch-operator"  # default
```

**Direct Application Installation:**
```yaml
# In playbooks - installs OpenSearch directly
- action: install_operator
  params:
    version: "3.2.1"
    namespace: "opensearch-system"
    helm_chart: "opensearch/opensearch"
    helm_repo_url: "https://opensearch-project.github.io/helm-charts/"
```

**Development Chart Usage:**

```yaml
# Option 1: Local chart directory
- action: install_operator
  params:
    local_chart_path: "/path/to/local/opensearch-chart"
    namespace: "opensearch-system"
    values_file: "custom-values.yaml"

# Option 2: Git repository (clones automatically)
- action: install_operator
  params:
    git_chart_repo: "https://github.com/opensearch-project/helm-charts.git"
    git_chart_branch: "feature-branch"  # optional, defaults to 'main'
    namespace: "opensearch-system"

# Option 3: Custom Helm repository
- action: install_operator
  params:
    helm_chart: "myrepo/opensearch"
    helm_repo_url: "https://my-custom-helm-repo.com/charts"
    version: "dev-1.0.0"
    namespace: "opensearch-system"
```

**Local Operator Development:**

Configure the test harness to use your local operator source code by editing `config.yaml`:

```yaml
opensearch:
  namespace: "opensearch-system"
  operator_version: "latest"
  opensearch_version: "2.11.0"
  # Set the path to your local operator source
  local_operator_path: "/Users/itamarsyn-hershko/code/opensearch-k8s-operator"
```

When `local_operator_path` is configured, the test harness will:
1. **Validate** the path contains operator source code (Dockerfile, Makefile, Go files)
2. **Build** the operator image using `make docker-build`
3. **Load** the image into Kind clusters automatically
4. **Deploy** the operator using `make deploy`

This allows you to test your local changes immediately without manual builds or registry pushes.

**Custom Values Files:**

Create custom Helm values for specific test scenarios:

```yaml
# custom-values.yaml
cluster:
  name: "test-cluster"
  
singleNode: false

masterNode:
  enabled: true
  replicas: 3

dataNode:
  enabled: true
  replicas: 2

resources:
  requests:
    cpu: "100m"
    memory: "512Mi"
```

Use in playbooks:
```yaml
- action: install_operator
  params:
    values_file: "path/to/custom-values.yaml"
```

### First Test Run

```bash
# Initialize example playbooks
oko-test init-examples

# Validate a playbook
oko-test validate playbooks/basic-deployment.yaml

# Run a simple test (requires Kubernetes cluster)
oko-test run playbooks/basic-deployment.yaml

# Run all playbooks in sequence (default behavior)
oko-test run
```

## 📋 Playbook Structure

Playbooks are declarative YAML files that define test scenarios:

```yaml
metadata:
  name: "my-test-scenario"
  description: "Description of what this test does"
  version: "1.0.0"
  author: "your-name"
  tags: ["basic", "smoke-test", "deployment"]

config:
  kubernetes:
    cluster_name: "test-cluster"
    provider: "kind"  # kind, minikube, existing
    nodes: 3
    cleanup_on_failure: true
    cleanup_on_success: true
  
  opensearch:
    namespace: "opensearch-system"
    operator_version: "latest"
    opensearch_version: "2.11.0"
  
  timeouts:
    deployment: "10m"
    upgrade: "15m"
    scaling: "5m"
    recovery: "20m"

phases:
  - name: "setup"
    description: "Initialize test environment"
    steps:
      - action: setup_cluster
        params:
          provider: "kind"
          nodes: 3
      - action: install_operator
        params:
          version: "latest"
          namespace: "opensearch-system"

  - name: "deploy_test"
    description: "Deploy and test cluster"
    steps:
      - action: deploy_cluster
        params:
          cluster_name: "test-cluster"
          nodes:
            master: 3
            data: 2
          storage:
            class: "standard"
            size: "10Gi"
      
      - action: wait_for_cluster_ready
        params:
          timeout: "10m"
          conditions:
            - cluster_health: "green"
            - all_nodes_ready: true

  - name: "cleanup"
    description: "Clean up resources"
    steps:
      - action: delete_cluster
        params:
          cluster_name: "test-cluster"
      - action: cleanup_cluster
```

### Key Playbook Components

1. **Metadata**: Name, description, version, tags for organization
2. **Config**: Global configuration for Kubernetes, OpenSearch, timeouts
3. **Phases**: Logical groupings of test steps that execute sequentially
4. **Steps**: Individual actions with parameters

## 🎬 Available Actions

### Cluster Management
| Action | Description | Key Parameters |
|--------|-------------|----------------|
| `setup_cluster` | Create Kubernetes cluster | `provider`, `nodes`, `kubernetes_version` |
| `install_operator` | Install OpenSearch operator | `version`, `namespace`, `helm_chart` |
| `deploy_cluster` | Deploy OpenSearch cluster | `cluster_name`, `nodes`, `storage`, `version` |
| `delete_cluster` | Remove OpenSearch cluster | `cluster_name`, `namespace`, `force` |
| `cleanup_cluster` | Clean up test resources | `remove_cluster`, `cleanup_docker` |

### Data Operations
| Action | Description | Key Parameters |
|--------|-------------|----------------|
| `index_documents` | Index test documents | `count`, `index`, `bulk_size`, `document_template` |
| `query_documents` | Query and validate data | `index`, `query`, `expected_count` |
| `create_snapshot` | Create cluster snapshots | `repository`, `snapshot_name`, `indices` |
| `restore_snapshot` | Restore from snapshots | `repository`, `snapshot_name`, `target_indices` |

### Validation & Monitoring
| Action | Description | Key Parameters |
|--------|-------------|----------------|
| `validate_cluster_health` | Check cluster health | `expected_status`, `timeout`, `check_nodes` |
| `validate_data_integrity` | Verify data consistency | `indices`, `expected_documents`, `sample_queries` |
| `wait_for_cluster_ready` | Wait for readiness | `timeout`, `conditions` |
| `validate_operator_status` | Check operator status | `expected_phase`, `check_events` |
| `collect_logs` | Gather component logs | `components`, `since`, `output_dir` |
| `capture_metrics` | Capture cluster metrics | `duration`, `metrics`, `output_file` |

### Scaling & Upgrades
| Action | Description | Key Parameters |
|--------|-------------|----------------|
| `scale_cluster` | Scale cluster up | `node_type`, `target_count`, `strategy` |
| `scale_down_cluster` | Scale cluster down | `node_type`, `target_count`, `drain_data` |
| `upgrade_cluster` | Upgrade OpenSearch version | `target_version`, `strategy`, `rollback_on_failure` |
| `upgrade_operator` | Upgrade operator version | `target_version`, `strategy`, `backup_before` |

### Chaos Engineering
| Action | Description | Key Parameters |
|--------|-------------|----------------|
| `inject_pod_failure` | Simulate pod failures | `target`, `count`, `method`, `recovery_wait` |
| `inject_node_failure` | Simulate node failures | `node_selector`, `count`, `method` |
| `inject_network_partition` | Network isolation testing | `target`, `duration`, `partition_type` |
| `inject_resource_pressure` | Resource constraint testing | `target`, `resource`, `limit`, `duration` |

### Debugging
| Action | Description | Key Parameters |
|--------|-------------|----------------|
| `debug_pause` | Pause for manual inspection | `message`, `timeout` |

## 📖 Writing New Playbooks

### Basic Playbook Template

```yaml
metadata:
  name: "my-new-test"
  description: "What this test validates"
  tags: ["category", "type"]

config:
  kubernetes:
    cluster_name: "my-test-cluster"
    provider: "kind"
  opensearch:
    namespace: "opensearch-system"

phases:
  - name: "setup"
    steps:
      - action: setup_cluster
      - action: install_operator
      # Add setup steps

  - name: "test"
    steps:
      # Add test steps
      - action: deploy_cluster
        params:
          cluster_name: "test-cluster"
      - action: validate_cluster_health

  - name: "cleanup"
    steps:
      - action: delete_cluster
      - action: cleanup_cluster
```

### Template Variables & Substitution

Use Jinja2 templating for dynamic values:

```yaml
# Built-in template functions
steps:
  - action: index_documents
    params:
      count: "{{ document_count | default(1000) }}"
      index: "test-data-{{ timestamp() }}"
      document_template: |
        {
          "id": "{{ counter() }}",
          "timestamp": "{{ now() }}",
          "random_value": "{{ random(['A', 'B', 'C']) }}"
        }

# Environment variable substitution
config:
  opensearch:
    version: "${OPENSEARCH_VERSION:-2.11.0}"
    namespace: "${TEST_NAMESPACE:-opensearch-test}"
```

### Document Templates for Data Operations

```yaml
- action: index_documents
  params:
    count: 10000
    index: "test-data-{timestamp}"
    document_template: |
      {
        "id": {id},
        "timestamp": "{timestamp}",
        "message": "Test document {id}",
        "level": "{random:INFO,WARN,ERROR,DEBUG}",
        "service": "{random:web,api,db,cache}",
        "metrics": {
          "cpu": {id},
          "memory": {id},
          "requests": {id}
        },
        "tags": ["{random:production,staging,development}"]
      }
    bulk_size: 200
    threads: 4
```

### Advanced Validation Patterns

```yaml
- action: validate_data_integrity
  params:
    indices: ["test-data-*", "metrics-*"]
    expected_documents: 15000
    checksum_validation: true
    sample_queries:
      - query: |
          {
            "query": {
              "bool": {
                "must": [
                  {"term": {"level": "ERROR"}},
                  {"range": {"timestamp": {"gte": "now-1h"}}}
                ]
              }
            }
          }
        min_hits: 100
      - query: '{"match_all": {}}'
        expected_hits: 15000
```

### Conditional Logic & Error Handling

```yaml
# Continue on failure
- action: inject_pod_failure
  params:
    target: "data-nodes"
    count: 1
  continue_on_error: true

# Conditional steps based on variables
phases:
  - name: "optional_chaos"
    condition: "{{ enable_chaos | default(false) }}"
    steps:
      - action: inject_pod_failure
```

## 🛠 CLI Usage

### Basic Commands

```bash
# Run a specific playbook
oko-test run playbook.yaml

# Run all playbooks in playbooks directory (default)
oko-test run

# Run with variables
oko-test run playbook.yaml -V cluster_name=my-test -V node_count=5

# Run with variables file
oko-test run playbook.yaml --var-file variables.yaml

# Dry run (validate without executing)
oko-test run playbook.yaml --dry-run

# Verbose output
oko-test --verbose run playbook.yaml

# Continue on errors
oko-test run playbook.yaml --continue-on-error

# Alternative command format (no installation needed)
python -m oko_test_harness run playbook.yaml
```

### Validation & Debugging

```bash
# Validate playbook structure
oko-test validate playbook.yaml

# List all available actions
oko-test list-actions

# Initialize example playbooks
oko-test init-examples --output ./my-tests
```

### Quick Cluster Management

```bash
# Setup test cluster
oko-test setup-cluster --cluster-name test --provider kind --nodes 3

# Cleanup specific cluster
oko-test cleanup-cluster --cluster-name test --provider kind

# Comprehensive cleanup (recommended between test runs)
oko-test cleanup-all

# Force cleanup without confirmation
oko-test cleanup-all --force

# Selective cleanup (keep Docker or Helm resources)
oko-test cleanup-all --keep-docker --keep-helm
```

## 📝 Example Test Scenarios

### 1. Basic Deployment Test

```bash
oko-test run playbooks/basic-deployment.yaml
```

**What it tests:**
- Kind cluster creation
- Operator installation
- Basic cluster deployment
- Data indexing and querying
- Health validation
- Resource cleanup

### 2. Resilience Test

```bash
oko-test run playbooks/resilience-test.yaml
```

**What it tests:**
- Pod failure recovery
- Network partition handling
- Resource pressure tolerance
- Data integrity under failures
- Rolling restart resilience
- Load testing during failures

### 3. Upgrade Test

```bash
oko-test run playbooks/upgrade-test.yaml
```

**What it tests:**
- Rolling cluster upgrades
- Operator upgrades
- Data preservation during upgrades
- Rollback capabilities
- Upgrade under load

## 🔧 Advanced Configuration

### Variables Files

Create `variables.yaml`:
```yaml
cluster_name: "production-test"
opensearch_version: "2.12.0"
operator_version: "2.4.0"
document_count: 50000
chaos_enabled: true
node_count: 5
```

Use with: `oko-test run playbook.yaml --var-file variables.yaml`

### Environment Variables

```bash
export OPENSEARCH_VERSION=2.12.0
export TEST_NAMESPACE=my-tests
export DOCUMENT_COUNT=10000
export CLUSTER_NAME=my-cluster

oko-test run playbook.yaml
```

### Custom Node Configurations

```yaml
- action: deploy_cluster
  params:
    cluster_name: "custom-cluster"
    nodes:
      master: 3
      data: 5
      ingest: 2
      coordinating: 1
    storage:
      class: "fast-ssd"
      size: "100Gi"
    memory:
      heap_size: "4g"
    config:
      plugins: ["analysis-icu", "repository-s3", "alerting"]
      settings:
        "cluster.max_shards_per_node": 2000
        "indices.recovery.max_bytes_per_sec": "500mb"
```

## 🐛 Troubleshooting

### Common Issues

1. **Kind cluster creation fails**
   ```bash
   # Check Docker is running
   docker ps
   
   # Verify Kind installation
   kind version
   
   # Check available resources
   docker system df
   ```

2. **Helm chart installation fails**
   ```bash
   # Common error: "chart matching not found"
   # Solution: Add OpenSearch repository and update
   helm repo add opensearch https://opensearch-project.github.io/helm-charts/
   helm repo update
   
   # Verify chart availability
   helm search repo opensearch
   
   # Check available versions
   helm search repo opensearch/opensearch --versions
   ```

3. **Operator installation fails**
   ```bash
   # Verify kubectl context
   kubectl config current-context
   
   # Check Helm repositories
   helm repo list
   
   # Verify cluster connectivity
   kubectl cluster-info
   
   # Check namespace creation
   kubectl get namespaces | grep opensearch
   ```

4. **OpenSearch connection issues**
   ```bash
   # Check service endpoints
   kubectl get svc -n opensearch
   
   # Verify pods are running
   kubectl get pods -n opensearch
   
   # Check logs
   kubectl logs -n opensearch -l app=opensearch
   ```

5. **Port-forward connection problems**
   ```bash
   # Manual port-forward test
   kubectl port-forward svc/opensearch-cluster 9200:9200 -n opensearch
   
   # Test connectivity
   curl -k -u admin:admin https://localhost:9200/_cluster/health
   ```

6. **Test runs failing due to leftover resources**
   ```bash
   # Comprehensive cleanup before retrying
   oko-test cleanup-all
   
   # Or force cleanup without confirmation
   oko-test cleanup-all --force
   
   # Then retry your test
   oko-test run playbooks/basic-deployment.yaml
   ```

### Debug Mode

Enable verbose logging and save to file:

```bash
oko-test --verbose --log-file debug.log run playbook.yaml
```

Add debug pauses in playbooks:

```yaml
steps:
  - action: debug_pause
    params:
      message: "Check cluster state before proceeding"
      timeout: "5m"
```

### Log Collection

Automatically collect logs on issues:

```yaml
- action: collect_logs
  params:
    components: ["operator", "opensearch-pods", "events"]
    since: "30m"
    output_dir: "./logs/{timestamp}"
```

## 🏗 Architecture & Extension

### Project Structure

```
oko_test_harness/
├── actions/           # Test action implementations
│   ├── base.py       # Base action class
│   ├── cluster.py    # Cluster management actions
│   ├── data.py       # Data operations
│   ├── validation.py # Validation actions
│   ├── chaos.py      # Chaos engineering
│   ├── scaling.py    # Scaling operations
│   ├── upgrade.py    # Upgrade operations
│   └── monitoring.py # Monitoring & debugging
├── models/           # Data models
│   └── playbook.py   # Playbook structure definitions
├── utils/            # Utilities
│   ├── kubernetes.py # Kubernetes client
│   ├── opensearch_client.py # OpenSearch HTTP client
│   └── playbook_parser.py   # YAML parsing
├── cli.py            # Command line interface
└── executor.py       # Playbook execution engine
```

### Creating Custom Actions

1. **Create action class:**

```python
# oko_test_harness/actions/custom.py
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult

class MyCustomAction(BaseAction):
    action_name = "my_custom_action"
    
    def execute(self, params):
        # Your implementation here
        param_value = params.get('my_param', 'default')
        
        try:
            # Perform action logic
            result = do_something(param_value)
            return ActionResult(True, f"Action completed: {result}")
        except Exception as e:
            return ActionResult(False, f"Action failed: {e}")
```

2. **Register in executor:**

```python
# oko_test_harness/executor.py
def _register_actions(self):
    # ... existing imports ...
    from oko_test_harness.actions.custom import MyCustomAction
    
    actions = [
        # ... existing actions ...
        MyCustomAction,
    ]
```

3. **Use in playbooks:**

```yaml
steps:
  - action: my_custom_action
    params:
      my_param: "custom_value"
```

### HTTP Client Design

The test harness uses a simple HTTP client for OpenSearch interactions to avoid version compatibility issues:

```python
# Direct HTTP requests instead of client libraries
response = requests.get(f"{base_url}/_cluster/health", 
                       auth=(username, password), verify=False)
health_data = response.json()
```

This approach ensures compatibility across different OpenSearch versions without client library constraints.

## 📊 Best Practices

### Playbook Design

1. **Use descriptive names and phases**
2. **Include proper cleanup phases**
3. **Add validation after each major operation**
4. **Use template variables for reusability**
5. **Document expected behavior in descriptions**

### Test Data Management

1. **Use timestamp-based index names**
2. **Include document IDs for tracking**
3. **Test with realistic data volumes**
4. **Validate data integrity after operations**

### Error Handling

1. **Set appropriate timeouts**
2. **Use `continue_on_error` judiciously**
3. **Include debug pauses for investigation**
4. **Collect logs on failures**

### Resource Management

1. **Always include cleanup phases**
2. **Use `cleanup_on_failure: true` in config**
3. **Monitor resource usage during tests**
4. **Clean up Docker resources periodically**

## 📄 License

[Specify License]

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-action`
3. Add tests for new functionality
4. Ensure all tests pass: `poetry run pytest`
5. Format code: `poetry run black . && poetry run isort .`
6. Submit pull request

### Development Setup

```bash
# Install with development dependencies
poetry install --with dev

# Run tests
poetry run pytest

# Format code
poetry run black oko_test_harness/
poetry run isort oko_test_harness/

# Type checking
poetry run mypy oko_test_harness/

# Linting
poetry run flake8 oko_test_harness/
```

## 📞 Support

For questions, issues, or contributions:
- **GitHub Issues**: [Repository Issues URL]
- **Documentation**: [Documentation URL]
- **Discussions**: [Discussions URL]

---

**Happy Testing!** 🚀