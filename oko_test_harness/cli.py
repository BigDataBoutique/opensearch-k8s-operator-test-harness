"""Command line interface for the OpenSearch test harness."""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

import click
from loguru import logger

from oko_test_harness.utils.playbook_parser import PlaybookParser
from oko_test_harness.executor import PlaybookExecutor
from oko_test_harness.models.playbook import ExecutionStatus


def setup_logging(verbose: bool = False, log_file: Optional[str] = None):
    """Setup logging configuration."""
    # Remove default logger
    logger.remove()
    
    # Console logging
    log_level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=True
    )
    
    # File logging
    if log_file:
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level="DEBUG",
            rotation="10 MB"
        )


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--log-file', '-l', help='Log file path')
@click.pass_context
def cli(ctx, verbose: bool, log_file: Optional[str]):
    """OpenSearch Kubernetes Operator Test Harness
    
    A comprehensive test framework for validating OpenSearch operator functionality,
    resilience, and reliability across various scenarios.
    """
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['log_file'] = log_file
    setup_logging(verbose, log_file)


@cli.command()
@click.argument('playbook_path', type=click.Path(path_type=Path), required=False)
@click.option('--var', '-V', multiple=True, help='Set variables (format: key=value)')
@click.option('--var-file', type=click.Path(exists=True), help='Variables file (JSON/YAML)')
@click.option('--dry-run', is_flag=True, help='Parse and validate playbook without executing')
@click.option('--continue-on-error', is_flag=True, help='Continue execution on step failures')
@click.pass_context
def run(ctx, playbook_path: Optional[Path], var: tuple, var_file: Optional[str], 
        dry_run: bool, continue_on_error: bool):
    """Run a test playbook. If no playbook is specified, runs all playbooks in the playbooks directory."""
    
    # If no playbook specified, find all playbooks in the playbooks directory
    if not playbook_path:
        playbook_dir = Path('playbooks')
        if not playbook_dir.exists():
            logger.error("No playbooks directory found. Run from the project root or specify a playbook path.")
            sys.exit(1)
        
        playbook_files = list(playbook_dir.glob('*.yaml')) + list(playbook_dir.glob('*.yml'))
        if not playbook_files:
            logger.error("No YAML playbook files found in playbooks directory.")
            sys.exit(1)
        
        # Sort for consistent execution order
        playbook_files.sort()
        logger.info(f"Found {len(playbook_files)} playbooks, running all sequentially...")
        
        overall_success = True
        for playbook_file in playbook_files:
            logger.info(f"Running playbook: {playbook_file}")
            success = _run_single_playbook(ctx, playbook_file, var, var_file, dry_run, continue_on_error)
            if not success:
                overall_success = False
                if not continue_on_error:
                    logger.error(f"Playbook {playbook_file} failed, stopping execution.")
                    sys.exit(1)
                else:
                    logger.warning(f"Playbook {playbook_file} failed, continuing due to --continue-on-error flag.")
        
        if not overall_success:
            logger.error("Some playbooks failed during execution.")
            sys.exit(1)
        else:
            logger.info("All playbooks completed successfully!")
        return
    
    # Single playbook execution
    logger.info(f"Running playbook: {playbook_path}")
    success = _run_single_playbook(ctx, playbook_path, var, var_file, dry_run, continue_on_error)
    if not success:
        sys.exit(1)


def _run_single_playbook(ctx, playbook_path: Path, var: tuple, var_file: Optional[str], 
                        dry_run: bool, continue_on_error: bool) -> bool:
    """Run a single playbook and return success status."""
    try:
        # Parse variables
        variables = {}
        
        # Load variables from file
        if var_file:
            import json
            import yaml
            
            with open(var_file, 'r') as f:
                if var_file.endswith('.json'):
                    variables.update(json.load(f))
                else:
                    variables.update(yaml.safe_load(f))
        
        # Parse command line variables
        for v in var:
            if '=' in v:
                key, value = v.split('=', 1)
                variables[key] = value
            else:
                logger.warning(f"Invalid variable format: {v}")
        
        # Parse playbook
        parser = PlaybookParser()
        playbook = parser.parse_file(str(playbook_path), variables)
        
        # Validate playbook
        if not parser.validate_playbook(playbook):
            logger.error("Playbook validation failed")
            return False
        
        if dry_run:
            logger.info("Playbook validation successful (dry run)")
            _print_playbook_summary(playbook)
            return True
        
        # Execute playbook
        executor = PlaybookExecutor()
        context = executor.execute(playbook, variables)
        
        # Print results
        _print_execution_results(context)
        
        return context.status == ExecutionStatus.SUCCESS
            
    except Exception as e:
        logger.error(f"Failed to run playbook: {e}")
        if ctx.obj['verbose']:
            raise
        return False


@cli.command()
@click.argument('playbook_path', type=click.Path(exists=True, path_type=Path))
@click.pass_context
def validate(ctx, playbook_path: Path):
    """Validate a playbook without executing it."""
    logger.info(f"Validating playbook: {playbook_path}")
    
    try:
        parser = PlaybookParser()
        playbook = parser.parse_file(str(playbook_path))
        
        if parser.validate_playbook(playbook):
            logger.info("Playbook validation successful")
            _print_playbook_summary(playbook)
        else:
            logger.error("Playbook validation failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to validate playbook: {e}")
        if ctx.obj['verbose']:
            raise
        sys.exit(1)


@cli.command()
def list_actions():
    """List all available actions."""
    from oko_test_harness.executor import PlaybookExecutor
    
    executor = PlaybookExecutor()
    
    click.echo("Available Actions:")
    click.echo("=" * 50)
    
    categories = {
        'Cluster Management': [
            'setup_cluster', 'install_operator', 'deploy_cluster', 
            'delete_cluster', 'cleanup_cluster'
        ],
        'Data Operations': [
            'index_documents', 'query_documents', 'create_snapshot', 'restore_snapshot'
        ],
        'Validation': [
            'validate_cluster_health', 'validate_data_integrity', 
            'wait_for_cluster_ready', 'validate_operator_status'
        ],
        'Scaling': [
            'scale_cluster', 'scale_down_cluster'
        ],
        'Upgrades': [
            'upgrade_cluster', 'upgrade_operator'
        ],
        'Chaos Engineering': [
            'inject_pod_failure', 'inject_node_failure', 
            'inject_network_partition', 'inject_resource_pressure'
        ],
        'Monitoring': [
            'collect_logs', 'capture_metrics', 'debug_pause'
        ]
    }
    
    for category, actions in categories.items():
        click.echo(f"\n{category}:")
        for action in actions:
            if action in executor.actions:
                action_class = executor.actions[action]
                click.echo(f"  • {action}")
            else:
                click.echo(f"  • {action} (not implemented)")


@cli.command()
@click.option('--output', '-o', default='./examples', help='Output directory')
def init_examples(output: str):
    """Initialize example playbooks in the specified directory."""
    output_path = Path(output)
    examples_dir = output_path / 'playbooks'
    examples_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy example playbooks
    source_examples = Path(__file__).parent.parent / 'playbooks'
    
    if source_examples.exists():
        import shutil
        for example_file in source_examples.glob('*.yaml'):
            target_file = examples_dir / example_file.name
            shutil.copy2(example_file, target_file)
            logger.info(f"Created example: {target_file}")
    
    # Create a basic example if source doesn't exist
    basic_example = examples_dir / 'basic-example.yaml'
    if not basic_example.exists():
        basic_content = '''metadata:
  description: "Basic OpenSearch deployment test"

config:
  opensearch:
    namespace: "opensearch-system"
    cluster_name: "test-cluster"

phases:
  - name: "setup"
    steps:
      - action: setup_cluster
      - action: install_operator
      - action: deploy_cluster
        params:
          cluster_name: "test-cluster"
      - action: wait_for_cluster_ready
      - action: validate_cluster_health
'''
        with open(basic_example, 'w') as f:
            f.write(basic_content)
        logger.info(f"Created basic example: {basic_example}")
    
    logger.info(f"Example playbooks initialized in {examples_dir}")


@cli.command()
@click.option('--cluster-name', default='opensearch-test', help='Kubernetes cluster name')
@click.option('--provider', default='kind', help='Kubernetes provider (kind, existing)')
@click.option('--nodes', default=3, help='Number of nodes')
def setup_cluster(cluster_name: str, provider: str, nodes: int):
    """Quickly setup a test cluster."""
    from oko_test_harness.actions.cluster import SetupClusterAction
    from oko_test_harness.models.playbook import Config
    
    logger.info(f"Setting up {provider} cluster: {cluster_name}")
    
    config = Config()
    config.kubernetes.cluster_name = cluster_name
    config.kubernetes.provider = provider
    config.kubernetes.nodes = nodes
    
    action = SetupClusterAction(config)
    result = action.execute({})
    
    if result.success:
        logger.info(f"Cluster setup successful: {result.message}")
    else:
        logger.error(f"Cluster setup failed: {result.message}")
        sys.exit(1)


@cli.command()
@click.option('--cluster-name', default='opensearch-test', help='Kubernetes cluster name')
@click.option('--provider', default='kind', help='Kubernetes provider (kind, existing)')
def cleanup_cluster(cluster_name: str, provider: str):
    """Cleanup a test cluster."""
    from oko_test_harness.actions.cluster import CleanupClusterAction
    from oko_test_harness.models.playbook import Config
    
    logger.info(f"Cleaning up {provider} cluster: {cluster_name}")
    
    config = Config()
    config.kubernetes.cluster_name = cluster_name
    config.kubernetes.provider = provider
    
    action = CleanupClusterAction(config)
    result = action.execute({'cluster_name': cluster_name})
    
    if result.success:
        logger.info(f"Cluster cleanup successful: {result.message}")
    else:
        logger.error(f"Cluster cleanup failed: {result.message}")
        sys.exit(1)


@cli.command()
@click.option('--force', '-f', is_flag=True, help='Skip confirmation prompts')
@click.option('--keep-docker', is_flag=True, help='Skip Docker cleanup')
@click.option('--keep-helm', is_flag=True, help='Skip Helm cleanup')
@click.option('--namespace', '-n', help='Specific namespace to delete (for existing clusters)')
@click.option('--remove-operator', is_flag=True, help='Also remove the operator deployment and namespace (for existing clusters)')
def cleanup_all(force: bool, keep_docker: bool, keep_helm: bool, namespace: str, remove_operator: bool):
    """Comprehensive cleanup of all test resources."""
    import subprocess
    from oko_test_harness.utils.playbook_parser import PlaybookParser
    from oko_test_harness.models.playbook import Config
    
    # Load config to detect provider
    try:
        parser = PlaybookParser()
        config = parser.get_global_config()
        provider = config.kubernetes.provider
    except Exception as e:
        logger.warning(f"Could not load config, assuming kind provider: {e}")
        provider = "kind"
    
    logger.info(f"Detected provider: {provider}")
    
    # Always show what will be cleaned up for transparency
    click.echo("This will clean up:")
    if provider == "kind":
        click.echo("• All Kind clusters")
        click.echo("• Docker containers, networks, and volumes (unless --keep-docker)")
        click.echo("• Docker images (unless --keep-docker)")
    elif provider == "minikube":
        click.echo("• All Minikube clusters (stopped, not deleted)")
        click.echo("• Note: Minikube images and cluster will be preserved")
    elif provider == "existing":
        if namespace:
            click.echo(f"• Specific namespace: {namespace}")
        else:
            click.echo("• OpenSearch cluster namespaces (opensearch-* prefixed)")
            if remove_operator:
                click.echo("• OpenSearch operator namespace and deployment")
            else:
                click.echo("• Note: Operator deployment will be preserved (use --remove-operator to delete)")
    else:
        click.echo("• Resources in existing cluster only")
    
    click.echo("• All Helm releases in opensearch-* namespaces")
    click.echo("• Kubernetes contexts for deleted clusters")
    if not keep_helm:
        click.echo("• Helm repository cache")
    
    # Removed confirmation prompt - cleanup-all is assumed to be intentional
    
    success = True
    
    # 1. Provider-specific cluster cleanup
    if provider == "kind":
        success = success and _cleanup_kind_clusters()
    elif provider == "minikube":
        success = success and _cleanup_minikube_clusters()
    elif provider == "existing":
        success = success and _cleanup_existing_cluster_resources(namespace, remove_operator)
    else:
        logger.info(f"Provider '{provider}' - skipping cluster cleanup")
    
    # 2. Cleanup Helm releases
    if not keep_helm:
        success = success and _cleanup_helm_releases()
    
    # 3. Provider-specific Docker cleanup
    if not keep_docker:
        if provider == "kind":
            success = success and _cleanup_docker_resources()
        elif provider == "minikube":
            logger.info("Skipping Docker cleanup for minikube (images preserved)")
        else:
            logger.info(f"Provider '{provider}' - skipping Docker cleanup")
    
    # 4. Cleanup kubectl contexts
    success = success and _cleanup_kubectl_contexts(provider)
    
    if success:
        logger.info("✅ Comprehensive cleanup completed successfully!")
        logger.info("You can now run fresh tests without conflicts.")
    else:
        logger.warning("⚠️  Cleanup completed with some errors. Check logs above.")
        logger.info("Most resources should be cleaned up. You can try running tests.")
    
    return success


def _cleanup_kind_clusters() -> bool:
    """Clean up all Kind clusters."""
    import subprocess
    
    try:
        logger.info("Finding all Kind clusters...")
        result = subprocess.run(['kind', 'get', 'clusters'], capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            clusters = result.stdout.strip().split('\n')
            logger.info(f"Found {len(clusters)} Kind clusters: {', '.join(clusters)}")
            
            for cluster in clusters:
                logger.info(f"Deleting Kind cluster: {cluster}")
                delete_result = subprocess.run(['kind', 'delete', 'cluster', '--name', cluster], 
                                             capture_output=True, text=True)
                if delete_result.returncode == 0:
                    logger.info(f"Successfully deleted cluster: {cluster}")
                else:
                    logger.error(f"Failed to delete cluster {cluster}: {delete_result.stderr}")
                    return False
        else:
            logger.info("No Kind clusters found")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during Kind cluster cleanup: {e}")
        return False


def _cleanup_minikube_clusters() -> bool:
    """Clean up Minikube clusters (stop but don't delete)."""
    import subprocess
    import json
    
    try:
        logger.info("Finding all Minikube profiles...")
        result = subprocess.run(['minikube', 'profile', 'list', '--output', 'json'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            try:
                profiles_data = json.loads(result.stdout)
                profiles = profiles_data.get('valid', [])
                
                if profiles:
                    logger.info(f"Found {len(profiles)} Minikube profiles")
                    
                    for profile in profiles:
                        profile_name = profile.get('Name')
                        status = profile.get('Status')
                        
                        logger.info(f"Minikube profile: {profile_name} (status: {status})")
                        
                        if status == "Running":
                            logger.info(f"Stopping Minikube profile: {profile_name}")
                            stop_result = subprocess.run(['minikube', 'stop', '--profile', profile_name], 
                                                       capture_output=True, text=True)
                            if stop_result.returncode == 0:
                                logger.info(f"Successfully stopped profile: {profile_name}")
                            else:
                                logger.warning(f"Failed to stop profile {profile_name}: {stop_result.stderr}")
                        else:
                            logger.info(f"Profile {profile_name} is already stopped")
                else:
                    logger.info("No Minikube profiles found")
                    
            except json.JSONDecodeError:
                logger.warning("Could not parse minikube profile list, trying fallback approach")
                return True
        else:
            logger.info("No Minikube profiles found or minikube not available")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during Minikube cleanup: {e}")
        return False


def _cleanup_existing_cluster_resources(target_namespace: str = None, remove_operator: bool = False) -> bool:
    """Clean up test resources from an existing cluster."""
    import subprocess
    
    try:
        logger.info("Cleaning up test resources from existing cluster...")
        
        # Load config to get the actual namespaces being used
        try:
            parser = PlaybookParser()
            config = parser.get_global_config()
            operator_namespace = config.opensearch.operator_namespace or "opensearch-operator-system"
        except Exception as e:
            logger.warning(f"Could not load namespace config, using defaults: {e}")
            operator_namespace = "opensearch-operator-system"
        
        # If a specific namespace is provided, only delete that one
        if target_namespace:
            logger.info(f"Cleaning up OpenSearch resources in namespace: {target_namespace}")
            # First delete OpenSearch custom resources to allow proper pod termination
            logger.info("Deleting OpenSearch custom resources...")
            try:
                # First try to remove finalizers from any opensearch clusters
                clusters_result = subprocess.run(['kubectl', 'get', 'opensearchclusters', '-n', target_namespace, '-o', 'name'], 
                                               capture_output=True, text=True, timeout=10)
                if clusters_result.returncode == 0 and clusters_result.stdout.strip():
                    for cluster_line in clusters_result.stdout.strip().split('\n'):
                        cluster_name = cluster_line.split('/')[-1]
                        logger.info(f"Removing finalizers from {cluster_name}")
                        subprocess.run(['kubectl', 'patch', 'opensearchcluster', cluster_name, '-n', target_namespace, 
                                      '-p', '{"metadata":{"finalizers":[]}}', '--type=merge'], 
                                     capture_output=True, text=True, timeout=10)
                
                # Then delete the resources
                result = subprocess.run(['kubectl', 'delete', 'opensearchclusters', '--all', '-n', target_namespace, 
                                       '--ignore-not-found=true', '--timeout=60s'], 
                                      capture_output=True, text=True, timeout=70)
                if result.returncode != 0 and result.stderr:
                    logger.warning(f"Error deleting OpenSearch resources: {result.stderr}")
            except subprocess.TimeoutExpired:
                logger.warning(f"Timeout while cleaning up OpenSearch resources in {target_namespace}")
            except Exception as e:
                logger.warning(f"Error cleaning up OpenSearch resources in {target_namespace}: {e}")
            
            logger.info(f"Deleting namespace: {target_namespace}")
            delete_result = subprocess.run(['kubectl', 'delete', 'namespace', target_namespace, '--ignore-not-found=true'], 
                                         capture_output=True, text=True)
            if delete_result.returncode == 0:
                logger.info(f"Successfully deleted namespace: {target_namespace}")
            else:
                logger.warning(f"Failed to delete namespace {target_namespace}: {delete_result.stderr}")
        else:
            # Get all namespaces that match opensearch test patterns
            ns_result = subprocess.run(['kubectl', 'get', 'namespaces', '-o', 'name'], 
                                     capture_output=True, text=True)
            
            if ns_result.returncode == 0:
                namespaces = ns_result.stdout.strip().split('\n')
                opensearch_namespaces = []
                
                for ns_line in namespaces:
                    # Extract namespace name from "namespace/name" format
                    if '/' in ns_line:
                        ns_name = ns_line.split('/', 1)[1]
                        # Look for opensearch cluster namespaces (starting with 'opensearch-')
                        # but exclude operator namespaces unless explicitly requested
                        if ns_name.startswith('opensearch-'):
                            # Check if it's an operator namespace
                            is_operator_ns = (ns_name == operator_namespace or 
                                            ns_name == 'opensearch-operator-system' or
                                            'operator' in ns_name)
                            
                            # Include operator namespaces only if remove_operator is True
                            if is_operator_ns and not remove_operator:
                                logger.info(f"Skipping operator namespace: {ns_name} (use --remove-operator to delete)")
                                continue
                            
                            opensearch_namespaces.append(ns_name)
                        elif ns_name == 'opensearch-system':
                            # Also include the default opensearch-system if it exists
                            opensearch_namespaces.append(ns_name)
                
                if opensearch_namespaces:
                    logger.info(f"Found {len(opensearch_namespaces)} opensearch namespaces to clean up: {', '.join(opensearch_namespaces)}")
                    
                    # First delete OpenSearch custom resources in all namespaces to allow proper pod termination
                    logger.info("Deleting OpenSearch custom resources in all target namespaces...")
                    for namespace in opensearch_namespaces:
                        logger.info(f"Deleting OpenSearch resources in namespace: {namespace}")
                        try:
                            # First try to remove finalizers from any opensearch clusters
                            clusters_result = subprocess.run(['kubectl', 'get', 'opensearchclusters', '-n', namespace, '-o', 'name'], 
                                                           capture_output=True, text=True, timeout=10)
                            if clusters_result.returncode == 0 and clusters_result.stdout.strip():
                                for cluster_line in clusters_result.stdout.strip().split('\n'):
                                    cluster_name = cluster_line.split('/')[-1]
                                    logger.info(f"Removing finalizers from {cluster_name}")
                                    subprocess.run(['kubectl', 'patch', 'opensearchcluster', cluster_name, '-n', namespace, 
                                                  '-p', '{"metadata":{"finalizers":[]}}', '--type=merge'], 
                                                 capture_output=True, text=True, timeout=10)
                            
                            # Then delete the resources
                            result = subprocess.run(['kubectl', 'delete', 'opensearchclusters', '--all', '-n', namespace, 
                                                   '--ignore-not-found=true', '--timeout=60s'], 
                                                  capture_output=True, text=True, timeout=70)
                            if result.returncode != 0 and result.stderr:
                                logger.warning(f"Error deleting OpenSearch resources in {namespace}: {result.stderr}")
                        except subprocess.TimeoutExpired:
                            logger.warning(f"Timeout while cleaning up OpenSearch resources in {namespace}")
                        except Exception as e:
                            logger.warning(f"Error cleaning up OpenSearch resources in {namespace}: {e}")
                    
                    # Then delete the namespaces
                    for namespace in opensearch_namespaces:
                        logger.info(f"Deleting namespace: {namespace}")
                        delete_result = subprocess.run(['kubectl', 'delete', 'namespace', namespace, '--ignore-not-found=true'], 
                                                     capture_output=True, text=True)
                        if delete_result.returncode == 0:
                            logger.info(f"Successfully deleted namespace: {namespace}")
                        else:
                            logger.warning(f"Failed to delete namespace {namespace}: {delete_result.stderr}")
                else:
                    logger.info("No opensearch test namespaces found to clean up")
        
        # Clean up CRDs and cluster-level resources only if removing operator or no specific namespace was targeted
        if remove_operator or target_namespace is None:
            if remove_operator:
                logger.info("Cleaning up OpenSearch CRDs (operator removal requested)...")
            else:
                logger.info("Cleaning up OpenSearch CRDs...")
            
            subprocess.run(['kubectl', 'delete', 'crd', '--ignore-not-found=true', '-l', 'app.kubernetes.io/name=opensearch-operator'], 
                          capture_output=True, text=True)
            
            # Clean up any cluster roles and bindings
            logger.info("Cleaning up OpenSearch cluster roles and bindings...")
            subprocess.run(['kubectl', 'delete', 'clusterrole', '--ignore-not-found=true', '-l', 'app.kubernetes.io/name=opensearch-operator'], 
                          capture_output=True, text=True)
            subprocess.run(['kubectl', 'delete', 'clusterrolebinding', '--ignore-not-found=true', '-l', 'app.kubernetes.io/name=opensearch-operator'], 
                          capture_output=True, text=True)
        else:
            logger.info("Skipping CRD and cluster role cleanup (specific namespace targeted)")
        
        logger.info("Existing cluster resource cleanup completed")
        return True
        
    except Exception as e:
        logger.error(f"Error during existing cluster cleanup: {e}")
        return False


def _cleanup_helm_releases() -> bool:
    """Clean up Helm releases."""
    import subprocess
    
    try:
        logger.info("Cleaning up Helm releases...")
        
        # List all releases in opensearch-* namespaces
        list_result = subprocess.run(['helm', 'list', '--all-namespaces', '--short'], 
                                   capture_output=True, text=True)
        
        if list_result.returncode == 0 and list_result.stdout.strip():
            releases = list_result.stdout.strip().split('\n')
            opensearch_releases = [r for r in releases if 'opensearch' in r.lower()]
            
            for release_line in opensearch_releases:
                parts = release_line.split('\t')
                if len(parts) >= 2:
                    release_name = parts[0]
                    namespace = parts[1]
                    logger.info(f"Uninstalling Helm release: {release_name} (namespace: {namespace})")
                    
                    uninstall_result = subprocess.run(['helm', 'uninstall', release_name, '-n', namespace], 
                                                    capture_output=True, text=True)
                    if uninstall_result.returncode == 0:
                        logger.info(f"Successfully uninstalled: {release_name}")
                    else:
                        logger.warning(f"Failed to uninstall {release_name}: {uninstall_result.stderr}")
        
        # Clean up Helm repository cache
        logger.info("Cleaning Helm repository cache...")
        subprocess.run(['helm', 'repo', 'update'], capture_output=True)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during Helm cleanup: {e}")
        return False


def _cleanup_docker_resources() -> bool:
    """Clean up Docker resources."""
    import subprocess
    
    try:
        logger.info("Cleaning up Docker resources...")
        
        # Remove dangling containers
        subprocess.run(['docker', 'container', 'prune', '-f'], capture_output=True)
        
        # Remove unused networks
        subprocess.run(['docker', 'network', 'prune', '-f'], capture_output=True)
        
        # Remove unused volumes
        subprocess.run(['docker', 'volume', 'prune', '-f'], capture_output=True)
        
        # Remove unused images (be careful with this)
        subprocess.run(['docker', 'image', 'prune', '-f'], capture_output=True)
        
        logger.info("Docker cleanup completed")
        return True
        
    except Exception as e:
        logger.warning(f"Docker cleanup encountered issues: {e}")
        # Don't fail the entire cleanup for Docker issues
        return True


def _cleanup_kubectl_contexts(provider: str) -> bool:
    """Clean up kubectl contexts for deleted clusters."""
    import subprocess
    
    try:
        logger.info("Cleaning up kubectl contexts...")
        
        # Get current contexts
        contexts_result = subprocess.run(['kubectl', 'config', 'get-contexts', '-o', 'name'], 
                                       capture_output=True, text=True)
        
        if contexts_result.returncode == 0:
            contexts = contexts_result.stdout.strip().split('\n')
            
            if provider == "kind":
                kind_contexts = [c for c in contexts if c.startswith('kind-')]
                
                for context in kind_contexts:
                    # Extract cluster name from context (kind-clustername)
                    cluster_name = context.replace('kind-', '', 1)
                    
                    # Check if the Kind cluster still exists
                    check_result = subprocess.run(['kind', 'get', 'clusters'], capture_output=True, text=True)
                    if check_result.returncode == 0:
                        existing_clusters = check_result.stdout.strip().split('\n') if check_result.stdout.strip() else []
                        
                        if cluster_name not in existing_clusters:
                            logger.info(f"Removing stale kubectl context: {context}")
                            subprocess.run(['kubectl', 'config', 'delete-context', context], capture_output=True)
                            subprocess.run(['kubectl', 'config', 'delete-cluster', context], capture_output=True)
                            subprocess.run(['kubectl', 'config', 'unset', f'users.{context}'], capture_output=True)
            
            elif provider == "minikube":
                # For minikube, we don't remove contexts since clusters are preserved
                logger.info("Preserving minikube kubectl contexts (clusters not deleted)")
        
        return True
        
    except Exception as e:
        logger.warning(f"kubectl context cleanup encountered issues: {e}")
        return True


def _print_playbook_summary(playbook):
    """Print a summary of the playbook."""
    click.echo(f"\nPlaybook: {playbook.metadata.name or 'Unnamed'}")
    if playbook.metadata.description:
        click.echo(f"Description: {playbook.metadata.description}")
    
    click.echo(f"\nPhases ({len(playbook.phases)}):")
    for phase in playbook.phases:
        click.echo(f"  • {phase.name} ({len(phase.steps)} steps)")
        if phase.description:
            click.echo(f"    {phase.description}")


def _print_execution_results(context):
    """Print execution results."""
    click.echo(f"\nExecution Summary:")
    click.echo(f"Status: {context.status.value}")
    click.echo(f"Duration: {context.end_time - context.start_time:.2f}s")
    
    if context.results:
        click.echo(f"\nStep Results:")
        for step_id, result in context.results.items():
            status = "✓" if result.success else "✗"
            click.echo(f"  {status} {step_id}: {result.message}")


def main():
    """Main entry point."""
    cli()


if __name__ == '__main__':
    main()