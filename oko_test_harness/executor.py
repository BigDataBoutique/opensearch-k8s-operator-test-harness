"""Playbook executor for running test scenarios."""

import time
from typing import Dict, Any, Type
from loguru import logger

from oko_test_harness.models.playbook import (
    Playbook,
    ExecutionContext,
    ExecutionStatus,
    ActionResult,
    ActionStep,
)
from oko_test_harness.actions.base import BaseAction


class PlaybookExecutor:
    """Executes playbook phases and steps."""

    def __init__(self):
        self.actions: Dict[str, Type[BaseAction]] = {}
        self._register_actions()

    def _register_actions(self) -> None:
        """Register all available actions."""
        from oko_test_harness.actions.cluster import (
            SetupClusterAction,
            InstallOperatorAction,
            DeployClusterAction,
            DeleteClusterAction,
            CleanupClusterAction,
            UpdateClusterAllocationSettingsAction,
        )
        from oko_test_harness.actions.data import (
            IndexDocumentsAction,
            QueryDocumentsAction,
            CreateSnapshotAction,
            RestoreSnapshotAction,
        )
        from oko_test_harness.actions.validation import (
            ValidateClusterHealthAction,
            ValidateDataIntegrityAction,
            WaitForClusterReadyAction,
            ValidateOperatorStatusAction,
            ValidateClusterConfigurationAction,
            ValidateNodeConfigurationAction,
        )
        from oko_test_harness.actions.chaos import (
            InjectPodFailureAction,
            InjectNodeFailureAction,
            InjectNetworkPartitionAction,
            InjectResourcePressureAction,
        )
        from oko_test_harness.actions.upgrade import (
            UpgradeClusterAction,
            UpgradeOperatorAction,
        )
        from oko_test_harness.actions.scaling import (
            ScaleClusterAction,
            ScaleDownClusterAction,
        )
        from oko_test_harness.actions.monitoring import (
            CollectLogsAction,
            CaptureMetricsAction,
            DebugPauseAction,
        )

        actions = [
            SetupClusterAction,
            InstallOperatorAction,
            DeployClusterAction,
            DeleteClusterAction,
            CleanupClusterAction,
            UpdateClusterAllocationSettingsAction,
            IndexDocumentsAction,
            QueryDocumentsAction,
            CreateSnapshotAction,
            RestoreSnapshotAction,
            ValidateClusterHealthAction,
            ValidateDataIntegrityAction,
            WaitForClusterReadyAction,
            ValidateOperatorStatusAction,
            ValidateClusterConfigurationAction,
            ValidateNodeConfigurationAction,
            InjectPodFailureAction,
            InjectNodeFailureAction,
            InjectNetworkPartitionAction,
            InjectResourcePressureAction,
            UpgradeClusterAction,
            UpgradeOperatorAction,
            ScaleClusterAction,
            ScaleDownClusterAction,
            CollectLogsAction,
            CaptureMetricsAction,
            DebugPauseAction,
        ]

        for action_class in actions:
            self.actions[action_class.action_name] = action_class

    def execute(
        self, playbook: Playbook, variables: Dict[str, Any] = None
    ) -> ExecutionContext:
        """Execute a complete playbook."""
        logger.debug(
            f"Starting playbook execution: {playbook.metadata.name or 'unnamed'}"
        )

        context = ExecutionContext(
            playbook=playbook,
            variables=variables or {},
            status=ExecutionStatus.RUNNING,
            start_time=time.time(),
        )

        try:
            for phase in playbook.phases:
                logger.info(f"Executing phase: {phase.name}")
                context.current_phase = phase.name

                if not self._execute_phase(phase, context):
                    context.status = ExecutionStatus.FAILED
                    break

            if context.status == ExecutionStatus.RUNNING:
                context.status = ExecutionStatus.SUCCESS
                logger.info("Playbook execution completed successfully")

                # Perform automatic cleanup for successful runs
                self._perform_automatic_cleanup(context)

        except Exception as e:
            logger.error(f"Playbook execution failed: {e}")
            context.status = ExecutionStatus.FAILED

        finally:
            context.end_time = time.time()
            context.current_phase = None
            context.current_step = None

        return context

    def _execute_phase(self, phase, context: ExecutionContext) -> bool:
        """Execute a single phase."""
        for step in phase.steps:
            context.current_step = step.action
            logger.info(f"Executing step: {step.action}")

            if not self._execute_step(step, context):
                logger.error(f"Step '{step.action}' failed in phase '{phase.name}'")
                return False

        return True

    def _execute_step(self, step: ActionStep, context: ExecutionContext) -> bool:
        """Execute a single step."""
        action_class = self.actions.get(step.action)
        if not action_class:
            logger.error(f"Unknown action: {step.action}")
            return False

        try:
            action = action_class(context.playbook.config, context.variables)
            result = action.execute(step.params)

            # Store result
            step_id = f"{context.current_phase}.{step.action}"
            context.results[step_id] = result

            if not result.success:
                logger.error(f"Action '{step.action}' failed: {result.message}")
                return False

            logger.info(f"Action '{step.action}' completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error executing action '{step.action}': {e}")
            context.results[f"{context.current_phase}.{step.action}"] = ActionResult(
                success=False, message=str(e)
            )
            return False

    def _perform_automatic_cleanup(self, context: ExecutionContext) -> None:
        """Perform automatic cleanup after successful playbook execution."""
        logger.info("Performing automatic cleanup after successful run")

        try:
            # Delete OpenSearch cluster
            cluster_name = context.playbook.config.opensearch.cluster_name
            namespace = context.playbook.config.opensearch.operator_namespace

            logger.info(f"Cleaning up OpenSearch cluster: {cluster_name}")
            delete_action = self.actions.get("delete_cluster")
            if delete_action:
                delete_result = delete_action(
                    context.playbook.config, context.variables
                ).execute(
                    {
                        "cluster_name": cluster_name,
                        "namespace": namespace,
                        "force": False,
                        "wait_for_completion": True,
                    }
                )
                if delete_result.success:
                    logger.info("OpenSearch cluster cleanup completed")
                else:
                    logger.warning(
                        f"OpenSearch cluster cleanup failed: {delete_result.message}"
                    )

            # Only cleanup the Kind cluster if cleanup_on_success is True
            if context.playbook.config.kubernetes.cleanup_on_success:
                logger.info("Cleaning up Kind cluster and Docker resources")
                cleanup_action = self.actions.get("cleanup_cluster")
                if cleanup_action:
                    cleanup_result = cleanup_action(
                        context.playbook.config, context.variables
                    ).execute(
                        {
                            "cluster_name": context.playbook.config.kubernetes.cluster_name,
                            "remove_cluster": True,
                            "cleanup_docker": True,
                        }
                    )
                    if cleanup_result.success:
                        logger.info("Full cleanup completed successfully")
                    else:
                        logger.warning(f"Full cleanup failed: {cleanup_result.message}")
            else:
                logger.info("Skipping Kind cluster cleanup (cleanup_on_success=False)")

        except Exception as e:
            logger.error(f"Automatic cleanup failed: {e}")
            # Don't fail the playbook execution just because cleanup failed
