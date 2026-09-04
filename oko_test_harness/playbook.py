"""Playbook loading: config.yaml defaults + playbook config + env var substitution + random namespace/cluster names."""

import os
import random
import re
import string
from typing import Any, Dict, Optional

import yaml
from loguru import logger

from oko_test_harness.models.playbook import Playbook

_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def substitute_env(text: str, extra: Optional[Dict[str, str]] = None) -> str:
    env = {**os.environ, **{k: str(v) for k, v in (extra or {}).items()}}
    return _ENV.sub(lambda m: env.get(m.group(1), m.group(2) or ""), text)


def deep_merge(dst: Dict, src: Dict) -> Dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            deep_merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def load_global_config(path: Optional[str] = None) -> Dict[str, Any]:
    path = path or os.environ.get("OKO_CONFIG") or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return yaml.safe_load(substitute_env(f.read())) or {}


def load_playbook(path: str, variables: Optional[Dict[str, Any]] = None, global_config: Optional[Dict[str, Any]] = None) -> Playbook:
    with open(path) as f:
        data = yaml.safe_load(substitute_env(f.read(), variables)) or {}
    config = deep_merge(deep_merge({}, global_config if global_config is not None else load_global_config()), data.get("config") or {})
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
    os_cfg = config.setdefault("opensearch", {})
    os_cfg.setdefault("namespace", f"oko-{suffix}")
    os_cfg.setdefault("cluster_name", f"os-{suffix}")
    data["config"] = config
    data.setdefault("metadata", {}).setdefault("name", os.path.splitext(os.path.basename(path))[0])
    playbook = Playbook.from_dict(data)
    if not playbook.phases:
        raise ValueError("playbook has no phases")
    logger.debug(f"Loaded playbook {playbook.metadata.name}: namespace={os_cfg['namespace']} cluster={os_cfg['cluster_name']}")
    return playbook
