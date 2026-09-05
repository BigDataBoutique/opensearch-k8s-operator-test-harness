"""Scaling actions: replicas of a node pool, adding and removing pools. All go through a full
read-modify-write of the CR (a merge patch would replace the whole nodePools array)."""

import time
from typing import Optional

from oko_test_harness import k8s
from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult


class _ScaleBase(BaseAction):
    def apply_and_wait(self, cr, obs, message: str, expected_nodes: int, params, removed: int = 0, step_drop: Optional[int] = 1):
        k8s.replace_cr(cr)
        self.wait_cluster_running(self.timeout(self.config.timeouts.scaling))

        def settled():
            with self.os_client() as c:
                h = c.health()
                if h["number_of_nodes"] != expected_nodes:
                    raise Exception(f"{h['number_of_nodes']} nodes in cluster, expected {expected_nodes}")
                if h["status"] != "green" or h["relocating_shards"]:
                    raise Exception(f"health {h['status']}, relocating {h['relocating_shards']}")
                return h

        k8s.wait_for("cluster settled", settled, self.timeout(self.config.timeouts.scaling), 10)
        # the operator starts a rolling restart a few seconds after the Scaler finishes (#1450); wait for it too instead
        # of letting the next step race a restarting primary
        time.sleep(20)
        self.wait_cluster_running(self.timeout(self.config.timeouts.scaling))
        k8s.wait_for("cluster settled", settled, self.timeout(self.config.timeouts.scaling), 10)
        # scale-ups: new pods are unready while they start, so judge by cluster members lost instead of unready pods;
        # scale-downs may lose exactly `removed` members overall but only one between consecutive samples
        return self.finish_observed(obs, message, params.get("min_health", "yellow"), params.get("max_unready_pods"), max_nodes_down=params.get("max_nodes_down", max(1, removed)), max_step_drop=step_drop)


class ScaleClusterAction(_ScaleBase):
    """Change replicas of one node pool (up or down) and verify the operator adds/drains nodes safely."""

    action_name = "scale_cluster"
    params = {"component", "replicas", "min_health", "max_unready_pods", "max_nodes_down", "indices"}

    def execute(self, params):
        cr = self.cr()
        component = params.get("component", cr["spec"]["nodePools"][0]["component"])
        pool = next((p for p in cr["spec"]["nodePools"] if p["component"] == component), None)
        if not pool:
            return ActionResult(False, f"No node pool {component} in {[p['component'] for p in cr['spec']['nodePools']]}")
        old, new = pool["replicas"], int(params["replicas"])
        obs = self.observer(params.get("indices"))
        time.sleep(obs.interval)
        pool["replicas"] = new
        expected = sum(p["replicas"] for p in cr["spec"]["nodePools"])
        result = self.apply_and_wait(cr, obs, f"Scaled pool {component} {old} -> {new}", expected, params, removed=max(0, old - new))
        if result.success and new < old:
            leftover = [
                i["metadata"]["name"]
                for i in k8s.get_json("pvc", "-n", self.namespace).get("items", [])
                if i["metadata"]["name"].startswith(f"data-{self.cluster}-{component}-") and int(i["metadata"]["name"].rsplit("-", 1)[1]) >= new
            ]
            result.message += f"; PVCs of removed nodes {'retained: ' + str(leftover) if leftover else 'removed'}"
        return result


class AddNodePoolAction(_ScaleBase):
    action_name = "add_node_pool"
    params = {"node_pool", "min_health", "max_unready_pods", "max_nodes_down"}

    def execute(self, params):
        from oko_test_harness.actions.cluster import DeployClusterAction

        cr = self.cr()
        built = DeployClusterAction(self.config, self.variables)
        built.namespace, built.cluster, built.p = self.namespace, self.cluster, {}
        new_pool = built.build_cr({"node_pools": [params["node_pool"]], "version": cr["spec"]["general"]["version"]})["spec"]["nodePools"][0]
        if any(p["component"] == new_pool["component"] for p in cr["spec"]["nodePools"]):
            return ActionResult(False, f"Node pool {new_pool['component']} already exists")
        obs = self.observer()
        time.sleep(obs.interval)
        cr["spec"]["nodePools"].append(new_pool)
        expected = sum(p["replicas"] for p in cr["spec"]["nodePools"])
        return self.apply_and_wait(
            cr,
            obs,
            f"Added node pool {new_pool['component']} x{new_pool['replicas']} roles={new_pool['roles']}",
            expected,
            {**params, "max_unready_pods": params.get("max_unready_pods", new_pool["replicas"])},
        )


class RemoveNodePoolAction(_ScaleBase):
    action_name = "remove_node_pool"
    params = {"component", "min_health", "max_unready_pods", "max_nodes_down"}

    def execute(self, params):
        cr = self.cr()
        pools = cr["spec"]["nodePools"]
        pool = next((p for p in pools if p["component"] == params["component"]), None)
        if not pool:
            return ActionResult(False, f"No node pool {params['component']}")
        obs = self.observer()
        time.sleep(obs.interval)
        cr["spec"]["nodePools"] = [p for p in pools if p is not pool]
        expected = sum(p["replicas"] for p in cr["spec"]["nodePools"])
        # a removed pool's StatefulSet is deleted as a whole, so all its members leave at once (no one-at-a-time drain)
        return self.apply_and_wait(
            cr, obs, f"Removed node pool {pool['component']} ({pool['replicas']} nodes)", expected,
            {**params, "max_unready_pods": params.get("max_unready_pods", pool["replicas"])}, removed=pool["replicas"], step_drop=None,
        )
