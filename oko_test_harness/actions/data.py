"""Data actions: index, query, verify."""

import json
import random
import re
import time
from datetime import datetime
from typing import Any, Dict, List

from oko_test_harness.actions.base import BaseAction
from oko_test_harness.models.playbook import ActionResult


def generate_docs(count: int, template: str = None, start: int = 0) -> List[Dict[str, Any]]:
    docs = []
    for i in range(start, start + count):
        if template:
            content = template.replace("{id}", str(i)).replace("{timestamp}", datetime.now().isoformat())
            content = re.sub(r"\{random:([^}]+)\}", lambda m: random.choice([c.strip() for c in m.group(1).split(",")]), content)
            doc = json.loads(content)
            doc["id"] = i
        else:
            doc = {"id": i, "timestamp": datetime.now().isoformat(), "message": f"Test document {i}", "level": random.choice(["INFO", "WARN", "ERROR"]), "value": i}
        docs.append(doc)
    return docs


DEFAULT_MAPPINGS = {
    "dynamic": True,
    "properties": {"id": {"type": "long"}, "timestamp": {"type": "date"}, "message": {"type": "text"}, "level": {"type": "keyword"}, "version": {"type": "keyword"}, "value": {"type": "long"}},
}


class IndexDocumentsAction(BaseAction):
    """Create an index (unless it exists) and bulk-index `count` documents with deterministic ids.
    With `duration` set, keeps indexing `count`-sized batches at `rate` docs/s for that long (background load)."""

    action_name = "index_documents"
    params = {"index", "count", "shards", "replicas", "bulk_size", "document_template", "duration", "start_id", "recreate", "min_success_ratio", "rate"}

    def execute(self, params):
        index = params.get("index", "test-data")
        count = int(params.get("count", 1000))
        bulk_size = int(params.get("bulk_size", 200))
        duration = self.timeout(params["duration"]) if params.get("duration") else 0
        template = params.get("document_template")
        start = int(params.get("start_id", 0))
        min_ratio = float(params.get("min_success_ratio", 1.0))
        rate = float(params.get("rate", 100))  # docs/second while `duration` is set; realistic background load, not an IO stress test
        with self.os_client() as c:
            if params.get("recreate", False):
                c.delete_index(index)
            if index not in c.user_indices():
                c.create_index(index, int(params.get("shards", 1)), int(params.get("replicas", 1)), DEFAULT_MAPPINGS)
            ok = failed = 0
            deadline = time.time() + duration
            next_id = start
            attempts = 0
            while True:
                docs = generate_docs(count, template, next_id)
                next_id += count
                try:
                    o, f = c.bulk(index, docs, bulk_size)
                except Exception as e:  # noqa: BLE001 - under chaos the forward can drop; reconnect and retry the same batch
                    attempts += 1
                    self.logger.warning(f"bulk failed ({e}); reconnecting (attempt {attempts})")
                    c.disconnect()
                    time.sleep(5)
                    next_id -= count
                    try:
                        c.connect()
                    except Exception:  # noqa: BLE001
                        pass
                    if attempts > 60:
                        return ActionResult(False, f"Indexing gave up after {attempts} reconnects: {e}")
                    continue
                ok, failed = ok + o, failed + f
                if time.time() >= deadline:
                    break
                if duration:
                    time.sleep(count / rate)
            try:
                c.refresh(index)
                total = c.count(index)
            except Exception:  # noqa: BLE001 - forward may have died under chaos; one reconnect
                c.disconnect()
                c.connect()
                c.refresh(index)
                total = c.count(index)
        ratio = ok / max(ok + failed, 1)
        msg = f"Indexed {ok} docs into {index} ({failed} failed, {total} total now)"
        return ActionResult(
            ratio >= min_ratio, msg if ratio >= min_ratio else msg + f"; success ratio {ratio:.3f} < {min_ratio}", {"indexed": ok, "failed": failed, "index": index, "next_id": next_id}
        )


class QueryDocumentsAction(BaseAction):
    action_name = "query_documents"
    params = {"index", "query", "expected_count", "expected_min_count"}

    def execute(self, params):
        index = params.get("index", "test-data")
        query = params.get("query", {"match_all": {}})
        query = json.loads(query) if isinstance(query, str) else query
        with self.os_client() as c:
            c.refresh(index)
            hits = c.search(index, query)["hits"]["total"]["value"]
        expected, minimum = params.get("expected_count"), params.get("expected_min_count")
        if expected is not None and hits != int(expected):
            return ActionResult(False, f"Query on {index} returned {hits} hits, expected {expected}")
        if minimum is not None and hits < int(minimum):
            return ActionResult(False, f"Query on {index} returned {hits} hits, expected at least {minimum}")
        return ActionResult(True, f"Query on {index} returned {hits} hits")


class ValidateDataIntegrityAction(BaseAction):
    """Verify document counts and fetch a random sample of documents by id to prove they are readable."""

    action_name = "validate_data_integrity"
    params = {"index", "expected_documents", "sample_size", "sample_queries", "max_id"}

    def execute(self, params):
        index = params.get("index", "test-data")
        with self.os_client() as c:
            c.refresh(index)
            total = c.count(index)
            expected = params.get("expected_documents")
            if expected is not None and total != int(expected):
                return ActionResult(False, f"{index}: {total} documents, expected {expected}")
            max_id = int(params.get("max_id", total))
            sample = random.sample(range(max_id), min(int(params.get("sample_size", 50)), max_id)) if max_id else []
            missing = [i for i in sample if c.get_doc(index, str(i)) is None]
            if missing:
                return ActionResult(False, f"{index}: {len(missing)}/{len(sample)} sampled documents missing by id: {missing[:10]}")
            for q in params.get("sample_queries", []):
                query = json.loads(q["query"]) if isinstance(q["query"], str) else q["query"]
                hits = c.search(index, query)["hits"]["total"]["value"]
                if "expected_hits" in q and hits != int(q["expected_hits"]):
                    return ActionResult(False, f"{index}: query {query} returned {hits}, expected {q['expected_hits']}")
                if "min_hits" in q and hits < int(q["min_hits"]):
                    return ActionResult(False, f"{index}: query {query} returned {hits}, expected at least {q['min_hits']}")
            health = c.health()
        return ActionResult(True, f"{index}: {total} documents, {len(sample)} sampled ids readable, cluster {health['status']}", {"count": total})
