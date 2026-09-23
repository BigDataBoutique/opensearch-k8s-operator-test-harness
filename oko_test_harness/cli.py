"""Command line interface."""

import sys
import time
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from oko_test_harness import k8s
from oko_test_harness.executor import PlaybookExecutor
from oko_test_harness.models.playbook import ExecutionStatus
from oko_test_harness.playbook import load_playbook


def setup_logging(verbose: bool, log_file: Optional[str]) -> None:
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if verbose else "INFO", backtrace=False, diagnose=False, format="<green>{time:HH:mm:ss}</green> <level>{level:<7}</level> {message}")
    if log_file:
        logger.add(log_file, level="DEBUG", backtrace=False, diagnose=False, format="{time:YYYY-MM-DD HH:mm:ss} {level:<7} {name}:{line} {message}")


@click.group()
@click.option("--verbose", "-v", is_flag=True)
@click.option("--log-file", "-l", type=click.Path())
def cli(verbose: bool, log_file: Optional[str]):
    """OpenSearch Kubernetes Operator test harness."""
    setup_logging(verbose, log_file)


def _parse_vars(var) -> dict:
    out = {}
    for v in var:
        if "=" not in v:
            raise click.BadParameter(f"--var expects key=value, got {v!r}")
        k, val = v.split("=", 1)
        out[k] = val
    return out


@cli.command()
@click.argument("playbooks", nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option("--var", "-V", multiple=True, help="Variable for ${VAR} substitution (key=value)")
@click.option("--continue-on-error", is_flag=True, help="Run remaining playbooks after a failure")
def run(playbooks, var, continue_on_error):
    """Run one or more playbooks (default: every playbooks/*.yaml)."""
    files = list(playbooks) or sorted(Path("playbooks").glob("*.yaml"))
    if not files:
        raise click.ClickException("no playbooks found")
    variables = _parse_vars(var)
    outcomes = {}
    for path in files:
        logger.info(f"##### Playbook {path}")
        try:
            pb = load_playbook(str(path), variables)
        except Exception as e:  # noqa: BLE001
            logger.error(f"{path}: cannot load: {e}")
            outcomes[str(path)] = f"invalid: {e}"
            if not continue_on_error:
                break
            continue
        ctx = PlaybookExecutor().execute(pb, variables)
        _print_results(ctx)
        outcomes[str(path)] = ctx.status.value + (f" ({ctx.failure})" if ctx.failure and ctx.status != ExecutionStatus.SUCCESS else "")
        if ctx.status != ExecutionStatus.SUCCESS and not continue_on_error:
            break
    click.echo("\nSummary:")
    for name, outcome in outcomes.items():
        click.echo(f"  {'PASS' if outcome == 'success' else 'FAIL'}  {name}: {outcome}")
    if any(o != "success" for o in outcomes.values()) or len(outcomes) < len(files):
        sys.exit(1)


@cli.command()
@click.argument("playbooks", nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option("--var", "-V", multiple=True)
def validate(playbooks, var):
    """Parse playbooks and check every step's action and params without touching a cluster."""
    files = list(playbooks) or sorted(Path("playbooks").glob("*.yaml"))
    actions = PlaybookExecutor().actions
    failed = False
    for path in files:
        try:
            pb = load_playbook(str(path), _parse_vars(var))
            for phase in pb.phases:
                for step in phase.steps:
                    cls = actions.get(step.action)
                    if not cls:
                        raise ValueError(f"phase {phase.name}: unknown action {step.action}")
                    unknown = set(step.params) - cls.params - cls.common_params
                    if unknown:
                        raise ValueError(f"phase {phase.name}, {step.action}: unknown params {sorted(unknown)}")
            click.echo(f"OK    {path}: {len(pb.phases)} phases, {sum(len(p.steps) for p in pb.phases)} steps")
        except Exception as e:  # noqa: BLE001
            failed = True
            click.echo(f"ERROR {path}: {e}")
    sys.exit(1 if failed else 0)


@cli.command("list-actions")
def list_actions():
    """List actions and their parameters."""
    for name, cls in sorted(PlaybookExecutor().actions.items()):
        doc = (cls.__doc__ or "").strip().splitlines()[0] if cls.__doc__ else ""
        click.echo(f"{name}: {doc}\n    params: {', '.join(sorted(cls.params | cls.common_params))}")


@cli.command()
@click.option("--all-clusters", is_flag=True, help="Also delete k3d/kind clusters named in config")
def cleanup(all_clusters):
    """Delete every namespace the harness created (label oko-test=true) and their OpenSearch clusters."""
    nss = [i["metadata"]["name"] for i in k8s.get_json("ns", "-l", f"{k8s.HARNESS_LABEL}=true").get("items", [])]
    for ns in nss:
        for group in ("opensearch.org", "opensearch.opster.io"):
            k8s.kubectl("delete", k8s.cluster_resource(group), "--all", "-n", ns, "--ignore-not-found", "--wait=false", check=False)
    for ns in nss:
        k8s.kubectl("delete", "ns", ns, "--wait=false", check=False)
    click.echo(f"Deleted namespaces: {nss or 'none'}")
    # a namespace stays Terminating while operator CRs keep their finalizers (operator gone, or its reconciler never lets go:
    # FINDINGS-round5 N34); after a grace period strip them so the next playbook can replace the operator
    deadline = time.time() + 180
    while time.time() < deadline and any(k8s.run(["kubectl", "get", "ns", ns], check=False).returncode == 0 for ns in nss):
        time.sleep(10)
    for ns in nss:
        if k8s.run(["kubectl", "get", "ns", ns], check=False).returncode == 0:
            freed = k8s.strip_operator_finalizers(ns)
            click.echo(f"Namespace {ns} still Terminating after 3 min; stripped finalizers from {freed or 'no CRs'} (see FINDINGS-round5 N34)")
    if all_clusters:
        from oko_test_harness.playbook import load_global_config

        k = load_global_config().get("kubernetes", {})
        tool = {"k3d": ["k3d", "cluster", "delete", k.get("cluster_name", "oko")], "kind": ["kind", "delete", "cluster", "--name", k.get("cluster_name", "oko")]}.get(k.get("provider", "k3d"))
        if tool:
            k8s.run(tool, check=False)
            click.echo(f"Ran: {' '.join(tool)}")


@cli.command("free-crs")
@click.argument("namespace")
def free_crs(namespace):
    """Delete only the OpenSearchCluster CR(s) in NAMESPACE (both API groups) -- unlike `cleanup`, the
    namespace, its pods, PVCs and events are left untouched for inspection. Used by the runner between
    solo/migration playbooks: a failed playbook's leftover cluster otherwise blocks every later solo
    playbook's 'operator CRs exist' guard (they run strictly sequentially with no cleanup between them),
    and the failure's diagnostics are already captured to logs/<pb>-<ts>/ by the time this runs."""
    entries = k8s.operator_crs(namespace)
    if not entries:
        click.echo(f"No operator CRs in {namespace}")
        return
    for entry in entries:
        kind, ref = entry.split(" ", 1)
        name = ref.split("/", 1)[1]
        k8s.kubectl("delete", kind, name, "-n", namespace, "--ignore-not-found", "--wait=false", check=False)
    deadline = time.time() + 60
    while time.time() < deadline and k8s.operator_crs(namespace):
        time.sleep(5)
    remaining = k8s.operator_crs(namespace)
    if remaining:
        freed = k8s.strip_operator_finalizers(namespace)
        click.echo(f"{namespace} CRs still present after 60s (finalizer stuck, see FINDINGS-round5 N34); stripped {freed}")
    click.echo(f"Freed CRs in {namespace}: {entries}")


def _print_results(ctx) -> None:
    click.echo(f"\n{ctx.playbook.metadata.name}: {ctx.status.value} in {int((ctx.end_time or time.time()) - ctx.start_time)}s")
    for step_id, result in ctx.results.items():
        click.echo(f"  {'✓' if result.success else '✗'} {step_id}: {result.message[:300]}")


def main():
    cli()


if __name__ == "__main__":
    main()
