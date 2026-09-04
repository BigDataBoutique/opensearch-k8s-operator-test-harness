"""OpenSearch HTTP client reached through `kubectl port-forward`."""

import json
import socket
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

HEALTH_ORDER = {"red": 0, "yellow": 1, "green": 2}


def health_at_least(current: str, target: str) -> bool:
    return HEALTH_ORDER.get(current, -1) >= HEALTH_ORDER.get(target, 0)


class OpenSearchClient:
    """Port-forwards to the cluster service; use as a context manager."""

    def __init__(self, namespace: str, service: str, username: str, password: str, use_ssl: bool = True, port: int = 9200):
        self.namespace, self.service, self.k8s_port = namespace, service, port
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False
        self.scheme = "https" if use_ssl else "http"
        self.proc: Optional[subprocess.Popen] = None
        self.base_url = ""

    # --- connection -------------------------------------------------------------
    def connect(self, attempts: int = 3) -> "OpenSearchClient":
        for attempt in range(attempts):
            with socket.socket() as s:
                s.bind(("", 0))
                port = s.getsockname()[1]
            self.proc = subprocess.Popen(
                ["kubectl", "port-forward", f"service/{self.service}", f"{port}:{self.k8s_port}", "-n", self.namespace],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.base_url = f"{self.scheme}://127.0.0.1:{port}"
            for _ in range(20):  # up to ~10s for the forward to come up
                time.sleep(0.5)
                if self.proc.poll() is not None:
                    break
                try:
                    self.session.get(f"{self.base_url}/", timeout=5).raise_for_status()
                    return self
                except requests.RequestException:
                    continue
            self.disconnect()
            logger.debug(f"port-forward to {self.namespace}/{self.service} not ready (attempt {attempt + 1})")
        raise ConnectionError(f"Could not connect to OpenSearch service {self.namespace}/{self.service}")

    def disconnect(self) -> None:
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *exc):
        self.disconnect()

    # --- low level ----------------------------------------------------------------
    def request(self, method: str, path: str, body: Any = None, ok: Tuple[int, ...] = (200, 201), **kw) -> requests.Response:
        headers = kw.pop("headers", {})
        data = None
        if body is not None:
            if isinstance(body, str):
                data = body
                headers.setdefault("Content-Type", "application/x-ndjson")
            else:
                data = json.dumps(body)
                headers.setdefault("Content-Type", "application/json")
        r = self.session.request(method, f"{self.base_url}{path}", data=data, headers=headers, timeout=kw.pop("timeout", 60))
        if r.status_code not in ok:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
        return r

    def get(self, path: str, **kw) -> Any:
        return self.request("GET", path, **kw).json()

    # --- cluster ------------------------------------------------------------------
    def info(self) -> Dict[str, Any]:
        return self.get("/")

    def version(self) -> str:
        return self.info()["version"]["number"]

    def health(self) -> Dict[str, Any]:
        return self.get("/_cluster/health")

    def nodes(self) -> Dict[str, Dict[str, Any]]:
        return self.get("/_nodes/_all/os,roles,plugins").get("nodes", {})

    def plugins(self) -> Dict[str, List[str]]:
        """node name -> installed plugin names."""
        return {n["name"]: [p["name"] for p in n.get("plugins", [])] for n in self.nodes().values()}

    def cluster_settings(self, flat: bool = True) -> Dict[str, Any]:
        return self.get("/_cluster/settings?include_defaults=false" + ("&flat_settings=true" if flat else ""))

    def setting(self, key: str) -> Optional[str]:
        s = self.cluster_settings()
        return s.get("transient", {}).get(key, s.get("persistent", {}).get(key))

    def put_cluster_settings(self, transient: Dict[str, Any]) -> None:
        self.request("PUT", "/_cluster/settings", {"transient": transient})

    # --- indices ------------------------------------------------------------------
    def create_index(self, name: str, shards: int, replicas: int, mappings: Optional[Dict] = None) -> None:
        body = {"settings": {"number_of_shards": shards, "number_of_replicas": replicas}}
        if mappings:
            body["mappings"] = mappings
        self.request("PUT", f"/{name}", body)

    def delete_index(self, name: str) -> None:
        self.request("DELETE", f"/{name}", ok=(200, 404))

    def refresh(self, index: str = "_all") -> None:
        self.request("POST", f"/{index}/_refresh")

    def count(self, index: str, query: Optional[Dict] = None) -> int:
        return self.request("POST", f"/{index}/_count", {"query": query} if query else None).json()["count"]

    def search(self, index: str, query: Dict, size: int = 0) -> Dict[str, Any]:
        return self.request("POST", f"/{index}/_search", {"query": query, "size": size, "track_total_hits": True}).json()

    def get_doc(self, index: str, doc_id: str) -> Optional[Dict[str, Any]]:
        r = self.request("GET", f"/{index}/_doc/{doc_id}", ok=(200, 404))
        return r.json() if r.status_code == 200 else None

    def user_indices(self) -> List[str]:
        return [i["index"] for i in self.get("/_cat/indices?format=json") if not i["index"].startswith(".")]

    def index_counts(self, indices: Optional[List[str]] = None) -> Dict[str, int]:
        return {i: self.count(i) for i in (indices or self.user_indices())}

    def bulk(self, index: str, docs: List[Dict[str, Any]], chunk: int = 200) -> Tuple[int, int]:
        """Index docs with `_id` = doc['id'] (idempotent). Returns (ok, failed)."""
        ok = failed = 0
        for i in range(0, len(docs), chunk):
            lines = []
            for d in docs[i : i + chunk]:
                lines.append(json.dumps({"index": {"_index": index, "_id": str(d["id"])}}))
                lines.append(json.dumps(d))
            resp = self.request("POST", "/_bulk", "\n".join(lines) + "\n", timeout=120).json()
            for item in resp.get("items", []):
                if item["index"].get("status") in (200, 201):
                    ok += 1
                else:
                    failed += 1
                    logger.warning(f"bulk item failed: {item['index'].get('error')}")
        return ok, failed
