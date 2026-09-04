"""Playbook executor."""

import signal
import threading
import time
from typing import Dict, List, Tuple, Type

from loguru import logger

from oko_test_harness.actions.base import BaseAction, parse_duration
from oko_test_harness.models.playbook import ActionResult, ActionStep, ExecutionContext, ExecutionStatus, Playbook


def all_actions() -> List[Type[BaseAction]]:
    from oko_test_harness.actions import chaos, cluster, data, features, monitoring, regressions, scaling, upgrade, validation

    found = []
    for mod in (cluster, data, validation, upgrade, scaling, chaos, monitoring, features, regressions):
        for obj in vars(mod).values():
            if isinstance(obj, type) and issubclass(obj, BaseAction) and obj is not BaseAction and obj.action_name:
                found.append(obj)
    return found


class PlaybookExecutor:
    def __init__(self):
        self.actions: Dict[str, Type[BaseAction]] = {a.action_name: a for a in all_actions()}

    def execute(self, playbook: Playbook, variables=None) -> ExecutionContext:
        ctx = ExecutionContext(playbook=playbook, variables=variables or {}, status=ExecutionStatus.RUNNING, start_time=time.time())
        try:
            for phase in playbook.phases:
                ctx.current_phase = phase.name
                logger.info(f"=== Phase: {phase.name}" + (f" - {phase.description}" if phase.description else ""))
                if not self._execute_phase(phase, ctx):
                    ctx.status = ExecutionStatus.FAILED
                    break
            if ctx.status == ExecutionStatus.RUNNING:
                ctx.status = ExecutionStatus.SUCCESS
        except Exception as e:  # noqa: BLE001
            logger.exception("Playbook execution crashed")
            ctx.status, ctx.failure = ExecutionStatus.FAILED, str(e)
        finally:
            ctx.end_time = time.time()
            self._teardown(ctx)
        return ctx

    def _execute_phase(self, phase, ctx: ExecutionContext) -> bool:
        background: List[Tuple[ActionStep, threading.Thread, list]] = []
        ok = True
        for step in phase.steps:
            if step.background:
                holder: list = []
                t = threading.Thread(target=lambda s=step, h=holder: h.append(self._execute_step(s, ctx)), daemon=True)
                t.start()
                background.append((step, t, holder))
                logger.info(f"Started background step: {step.action}")
                continue
            result = self._execute_step(step, ctx)
            if not result.success:
                if step.continue_on_error:
                    logger.warning(f"Step '{step.action}' failed (continue_on_error): {result.message}")
                    continue
                ok = False
                break
        for step, t, holder in background:
            t.join()
            result = holder[0] if holder else ActionResult(False, "background step produced no result")
            if not result.success and not step.continue_on_error:
                logger.error(f"Background step '{step.action}' failed: {result.message}")
                ctx.failure = ctx.failure or f"{ctx.current_phase}.{step.action} (background): {result.message}"
                ok = False
        return ok

    def _execute_step(self, step: ActionStep, ctx: ExecutionContext) -> ActionResult:
        logger.info(f"--- Step: {step.action} {step.params if step.params else ''}")
        action_class = self.actions.get(step.action)
        if not action_class:
            result = ActionResult(False, f"Unknown action: {step.action}. Known: {sorted(self.actions)}")
        else:
            result = self._run_with_backstop(action_class(ctx.playbook.config, ctx.variables), step, ctx)
        key = f"{ctx.current_phase}.{step.action}"
        n = 2
        while key in ctx.results:
            key, n = f"{ctx.current_phase}.{step.action}#{n}", n + 1
        ctx.results[key] = result
        if result.success:
            logger.success(f"{step.action}: {result.message}")
        else:
            logger.error(f"{step.action} FAILED: {result.message}")
            if not step.continue_on_error:
                ctx.failure = ctx.failure or f"{key}: {result.message}"
        return result

    def _run_with_backstop(self, action: BaseAction, step: ActionStep, ctx: ExecutionContext) -> ActionResult:
        """Foreground steps get a SIGALRM backstop (timeouts.step, or the step's own timeout + 5m): a step that
        hangs past it is a bug somewhere and fails the playbook instead of running forever."""
        limit = parse_duration(ctx.playbook.config.timeouts.step)
        if "timeout" in step.params:
            limit = min(limit, parse_duration(step.params["timeout"]) + 300)
        if threading.current_thread() is not threading.main_thread():
            return action.run(step.params)

        def on_alarm(signum, frame):
            raise TimeoutError(f"step {step.action} exceeded the {limit}s backstop")

        old = signal.signal(signal.SIGALRM, on_alarm)
        signal.alarm(limit)
        try:
            return action.run(step.params)
        except TimeoutError as e:
            return ActionResult(False, str(e))
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old)

    def _teardown(self, ctx: ExecutionContext) -> None:
        cfg = ctx.playbook.config
        failed = ctx.status != ExecutionStatus.SUCCESS
        if failed:
            self._run(ctx, "collect_logs", {"output_dir": f"./logs/{ctx.playbook.metadata.name}-{int(ctx.start_time)}"})
        if failed and not cfg.kubernetes.cleanup_on_failure:
            logger.info(f"Leaving namespace {cfg.opensearch.namespace} in place for inspection (cleanup_on_failure=false); k8s context {cfg.kubernetes.provider}/{cfg.kubernetes.cluster_name}")
            return
        # the namespace always goes on success; the k8s cluster itself only when cleanup_on_success is set
        self._run(ctx, "delete_cluster", {"delete_namespace": True})
        if cfg.kubernetes.provider != "existing" and (cfg.kubernetes.cleanup_on_success if not failed else cfg.kubernetes.cleanup_on_failure):
            self._run(ctx, "cleanup_cluster", {})

    def _run(self, ctx, action: str, params: Dict) -> None:
        try:
            result = self.actions[action](ctx.playbook.config, ctx.variables).run(params)
            (logger.info if result.success else logger.warning)(f"teardown {action}: {result.message}")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"teardown {action} crashed: {e}")
