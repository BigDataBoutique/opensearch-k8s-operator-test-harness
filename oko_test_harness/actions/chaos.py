"""Chaos actions. Each one breaks something, then waits for the operator to bring the cluster back
and asserts no data was lost. `delay` lets a chaos step (run with background: true) strike in the
middle of another step such as an upgrade."""

import random
import subprocess
import time

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction, parse_duration
from oko_test_harness.actions.cluster import OPERATOR_SELECTOR, sh
from oko_test_harness.models.playbook import ActionResult


class InjectPodFailureAction(BaseAction):
    """Delete or SIGKILL `count` OpenSearch pods (of a component, or the master), then wait for recovery."""

    action_name = "inject_pod_failure"
    params = {"component", "count", "method", "delay", "target", "wait", "min_health", "indices"}

    def execute(self, params):
        if params.get("delay"):
            time.sleep(parse_duration(params["delay"]))
        count = int(params.get("count", 1))
        method = params.get("method", "delete")
        pods = self.pods(params.get("component"))
        if params.get("target") == "master":
            with self.os_client() as c:
                master = c.get("/_cat/cluster_manager?format=json")[0]["node"]
            pods = [p for p in pods if p["name"] == master]
        if len(pods) < count:
            return ActionResult(False, f"Only {len(pods)} candidate pods, need {count}")
        victims = random.sample(pods, count)
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)
        for p in victims:
            if method == "delete":
                k8s.delete_pod(self.namespace, p["name"])
            elif method == "force_delete":
                k8s.delete_pod(self.namespace, p["name"], force=True)
            elif method == "kill":
                self._sigkill(p)
            else:
                obs.stop()
                return ActionResult(False, f"Unknown method {method}; use delete, force_delete or kill")
        names = [p["name"] for p in victims]
        if not params.get("wait", True):
            obs.stop()
            return ActionResult(True, f"{method} {names}")
        time.sleep(15)
        self.wait_cluster_running(self.timeout(self.config.timeouts.recovery))
        k8s.wait_for("health green", lambda: self._green(), self.timeout(self.config.timeouts.recovery), 10)
        after = {p["name"]: p for p in self.pods()}
        if method == "kill":
            not_restarted = [p["name"] for p in victims if after.get(p["name"], {}).get("restarts", 0) <= p["restarts"]]
            if not_restarted:
                obs.stop()
                return ActionResult(False, f"Containers in {not_restarted} did not restart after SIGKILL")
        else:
            not_recreated = [p["name"] for p in victims if after.get(p["name"], {}).get("uid") == p["uid"]]
            if not_recreated:
                obs.stop()
                return ActionResult(False, f"Pods {not_recreated} were not recreated")
        return self.finish_observed(obs, f"{method} {names}; cluster recovered", params.get("min_health", "red" if params.get("target") == "master" or count > 1 else "yellow"), max_unready_pods=count)

    def _green(self):
        with self.os_client() as c:
            h = c.health()
        return h["status"] == "green" and not h["relocating_shards"]

    def _sigkill(self, pod):
        """SIGKILL the OpenSearch java process. PID 1 ignores signals sent from inside its own pid namespace, so on
        k3d/kind we resolve the container's host pid via crictl on the node and kill from there."""
        ctx = k8s.current_context()
        cid = k8s.kubectl("get", "pod", pod["name"], "-n", self.namespace, "-o", "jsonpath={.status.containerStatuses[?(@.name=='opensearch')].containerID}").split("//")[-1]
        if cid and (ctx.startswith("k3d-") or ctx.startswith("kind-")):
            pid = sh(["docker", "exec", pod["node"], "crictl", "inspect", "-o", "go-template", "--template", "{{.info.pid}}", cid]).strip()
            sh(["docker", "exec", pod["node"], "kill", "-9", pid])
            self.logger.info(f"SIGKILLed pid {pid} of {pod['name']} on node {pod['node']}")
        else:
            # best effort from inside: SIGSEGV is handled by the JVM and aborts it
            r = k8s.exec_in_pod(self.namespace, pod["name"], "kill", "-SEGV", "1")
            self.logger.info(f"kill -SEGV 1 in {pod['name']}: rc={r.returncode} {r.stderr.strip()[:200]}")


class KillOperatorAction(BaseAction):
    """Delete the operator pod (after `delay`) and verify it comes back and the cluster ends RUNNING."""

    action_name = "kill_operator"
    params = {"delay", "wait"}

    def execute(self, params):
        if params.get("delay"):
            time.sleep(parse_duration(params["delay"]))
        ns = self.config.opensearch.operator_namespace
        pods = k8s.get_pods(ns, OPERATOR_SELECTOR)
        if not pods:
            return ActionResult(False, "No operator pod found")
        cr_phase = self.cr().get("status", {}).get("phase")
        for p in pods:
            k8s.delete_pod(ns, p["name"], force=True)
        k8s.wait_for("operator pod back", lambda: [p for p in k8s.get_pods(ns, OPERATOR_SELECTOR) if p["ready"] and p["uid"] not in {q["uid"] for q in pods}], 300, 5)
        if params.get("wait", True):
            self.wait_cluster_running(self.timeout(self.config.timeouts.recovery))
        return ActionResult(True, f"Killed operator pod(s) {[p['name'] for p in pods]} while cluster phase was {cr_phase}; operator back" + ("; cluster RUNNING" if params.get("wait", True) else ""))


class InjectNodeFailureAction(BaseAction):
    """Stop the Kubernetes node (k3d/kind docker container) hosting an OpenSearch pod for `duration`, then start it again
    and wait for the operator and cluster to recover."""

    action_name = "inject_node_failure"
    params = {"component", "duration", "delay", "min_health", "indices"}

    def execute(self, params):
        if params.get("delay"):
            time.sleep(parse_duration(params["delay"]))
        provider = self.config.kubernetes.provider
        ctx = k8s.current_context()
        if not (ctx.startswith("k3d-") or ctx.startswith("kind-")):
            return ActionResult(False, f"Node failure needs a k3d or kind cluster (context {ctx}, provider {provider})")
        victim = random.choice(self.pods(params.get("component")))
        node = victim["node"]
        duration = parse_duration(params.get("duration", "2m"))
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)
        self.logger.info(f"Stopping node {node} (hosts {victim['name']}) for {duration}s")
        sh(["docker", "stop", node])
        try:
            time.sleep(duration)
        finally:
            sh(["docker", "start", node])
        k8s.wait_for(f"node {node} Ready", lambda: "True" in k8s.kubectl("get", "node", node, "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"), 300, 5)
        self.wait_cluster_running(self.timeout(self.config.timeouts.recovery))
        k8s.wait_for("health green", lambda: self._green(), self.timeout(self.config.timeouts.recovery), 10)
        return self.finish_observed(obs, f"Node {node} stopped {duration}s and restarted; cluster recovered", params.get("min_health", "yellow"), max_unready_pods=None)

    def _green(self):
        with self.os_client() as c:
            h = c.health()
        return h["status"] == "green" and not h["relocating_shards"]


__all__ = ["subprocess"]
