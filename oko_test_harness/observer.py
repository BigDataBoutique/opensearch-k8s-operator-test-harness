"""Background observer: samples cluster health, node availability and document counts while an
operation (upgrade, scale, chaos) runs, so we can assert what happened *during* it, not just after."""

import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Set

from loguru import logger

from oko_test_harness import k8s
from oko_test_harness.opensearch import HEALTH_ORDER, OpenSearchClient


class ClusterObserver(threading.Thread):
    def __init__(self, client_factory, namespace: str, cluster: str, indices: Optional[List[str]] = None, interval: int = 5):
        super().__init__(daemon=True)
        self.client_factory = client_factory
        self.namespace, self.cluster, self.indices, self.interval = namespace, cluster, indices, interval
        self._stop_event = threading.Event()
        self.samples: List[Dict[str, Any]] = []
        self.errors: List[str] = []
        self.baseline_counts: Dict[str, int] = {}
        self.min_counts: Dict[str, int] = {}
        self.worst_health = "green"
        self.max_unready_pods = 0
        self.max_pods_seen = 0
        self.min_nodes: Optional[int] = None
        self.baseline_nodes: Optional[int] = None
        # Node names a scale-down is meant to remove: their departure is planned and never counts. Every other
        # departure is unplanned, and the two node invariants bound those (see record_nodes / violations).
        self.expected_removals: Set[str] = set()
        self.baseline_names: Optional[Set[str]] = None
        self._last_names: Optional[Set[str]] = None
        self.max_step_drop = 0  # most unplanned members that left between two consecutive samples
        self.max_unplanned_down = 0  # most unplanned members missing at once, relative to the first sample
        self.unplanned_departures: Set[str] = set()
        self.unreachable_seconds = 0.0
        self.max_unreachable_streak = 0.0

    def run(self) -> None:
        client: Optional[OpenSearchClient] = None
        streak_start: Optional[float] = None
        while not self._stop_event.is_set():
            t0 = time.time()
            sample: Dict[str, Any] = {"t": t0}
            try:
                if client is None:
                    client = self.client_factory().connect(attempts=1)
                    if not self.baseline_counts:
                        self.baseline_counts = client.index_counts(self.indices)
                        self.min_counts = dict(self.baseline_counts)
                h = client.health()
                sample.update(status=h["status"], nodes=h["number_of_nodes"], unassigned=h["unassigned_shards"], relocating=h["relocating_shards"])
                if HEALTH_ORDER[h["status"]] < HEALTH_ORDER[self.worst_health]:
                    self.worst_health = h["status"]
                left = self.record_nodes(client.node_names())
                if left:
                    sample["left"] = sorted(left)
                counts = client.index_counts(list(self.baseline_counts) or None)
                sample["counts"] = counts
                for idx, c in counts.items():
                    self.min_counts[idx] = min(self.min_counts.get(idx, c), c)
                if streak_start is not None:
                    self.max_unreachable_streak = max(self.max_unreachable_streak, time.time() - streak_start)
                    streak_start = None
            except Exception as e:  # noqa: BLE001
                sample["error"] = str(e)[:200]
                if client:
                    client.disconnect()
                    client = None
                if streak_start is None:
                    streak_start = time.time()
                self.unreachable_seconds += self.interval
            try:
                pods = [p for p in k8s.get_pods(self.namespace, k8s.selector(self.cluster)) if p["labels"].get(k8s.NODEPOOL_LABEL)]
                unready = [p["name"] for p in pods if not p["ready"]]
                sample["unready_pods"] = unready
                self.max_unready_pods = max(self.max_unready_pods, len(unready))
                self.max_pods_seen = max(self.max_pods_seen, len(pods))
            except Exception as e:  # noqa: BLE001
                sample["pods_error"] = str(e)[:200]
            self.samples.append(sample)
            logger.debug(f"observer: {sample}")
            self._stop_event.wait(max(0.0, self.interval - (time.time() - t0)))
        if streak_start is not None:
            self.max_unreachable_streak = max(self.max_unreachable_streak, time.time() - streak_start)
        if client:
            client.disconnect()

    def record_nodes(self, names: Iterable[str]) -> Set[str]:
        """Fold one sample's cluster membership into the node invariants; returns the unplanned departures since the
        previous sample. A member listed in `expected_removals` (the target of a scale-down) may leave without counting,
        so a drained node being removed while the operator rolls one other pod is within budget, while two nodes both
        leaving unexpectedly -- or the removed node plus two others -- is not."""
        current = set(names)
        n = len(current)
        if self.baseline_names is None:
            self.baseline_nodes, self.baseline_names = n, current
        self.min_nodes = n if self.min_nodes is None else min(self.min_nodes, n)
        left: Set[str] = set()
        if self._last_names is not None:
            left = (self._last_names - current) - self.expected_removals
            self.max_step_drop = max(self.max_step_drop, len(left))
            self.unplanned_departures |= left
        missing = (self.baseline_names - current) - self.expected_removals
        self.max_unplanned_down = max(self.max_unplanned_down, len(missing))
        self._last_names = current
        return left

    def stop(self) -> Dict[str, Any]:
        self._stop_event.set()
        self.join(timeout=max(self.interval * 3, 30))  # a sample may be inside a slow kubectl call
        return self.summary()

    def summary(self) -> Dict[str, Any]:
        return {
            "samples": len(self.samples),
            "worst_health": self.worst_health,
            "baseline_nodes": self.baseline_nodes,
            "min_nodes": self.min_nodes,
            "expected_removals": sorted(self.expected_removals),
            "unplanned_departures": sorted(self.unplanned_departures),
            "max_step_drop": self.max_step_drop,
            "max_unplanned_down": self.max_unplanned_down,
            "max_unready_pods": self.max_unready_pods,
            "max_unreachable_streak_s": round(self.max_unreachable_streak),
            "baseline_counts": self.baseline_counts,
            "min_counts": self.min_counts,
        }

    def violations(
        self, min_health: str = "yellow", max_unready_pods: Optional[int] = 1, allow_doc_loss: bool = False, max_nodes_down: Optional[int] = None, max_step_drop: Optional[int] = None
    ) -> List[str]:
        """max_unready_pods counts pods (new pods during a scale-up are unready too); max_nodes_down counts
        cluster members missing at once relative to the first sample and max_step_drop members leaving between two
        consecutive samples -- both over *unplanned* departures only: the nodes a scale-down is meant to remove
        (`expected_removals`) never count, so the budget cannot be met by widening it to cover the planned removal."""
        v = []
        gone = f"unplanned departures: {sorted(self.unplanned_departures)}" + (f", planned: {sorted(self.expected_removals)}" if self.expected_removals else "")
        if max_step_drop is not None and self.max_step_drop > max_step_drop:
            v.append(f"{self.max_step_drop} cluster members left unexpectedly between two samples (allowed: {max_step_drop}; nodes must be removed one at a time; {gone})")
        if max_nodes_down is not None and self.max_unplanned_down > max_nodes_down:
            v.append(f"{self.max_unplanned_down} cluster members were down at once beyond the planned removals (allowed: {max_nodes_down}; cluster went from {self.baseline_nodes} to {self.min_nodes} nodes; {gone})")
        if HEALTH_ORDER.get(self.worst_health, 0) < HEALTH_ORDER[min_health]:
            v.append(f"cluster health dropped to {self.worst_health} (allowed: {min_health})")
        if max_unready_pods is not None and self.max_unready_pods > max_unready_pods:
            v.append(f"{self.max_unready_pods} pods were unready at once (allowed: {max_unready_pods})")
        if not allow_doc_loss:
            for idx, base in self.baseline_counts.items():
                if self.min_counts.get(idx, base) < base:
                    v.append(f"index {idx} dropped from {base} to {self.min_counts[idx]} documents")
        return v
