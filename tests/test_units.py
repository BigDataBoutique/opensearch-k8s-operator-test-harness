"""Pure-function checks that need no cluster: manifest generation, document templates, duration parsing, playbook loading."""

import json

import pytest

from oko_test_harness.actions.base import parse_duration
from oko_test_harness.actions.cluster import DeployClusterAction
from oko_test_harness.actions.data import generate_docs
from oko_test_harness.executor import PlaybookExecutor
from oko_test_harness.models.playbook import Config
from oko_test_harness.observer import ClusterObserver
from oko_test_harness.playbook import load_playbook, substitute_env


def test_parse_duration():
    assert parse_duration("30s") == 30 and parse_duration("5m") == 300 and parse_duration("1h") == 3600 and parse_duration(42) == 42
    with pytest.raises(ValueError):
        parse_duration("5 minutes")


def test_substitute_env(monkeypatch):
    monkeypatch.setenv("X", "1")
    assert substitute_env("${X} ${Y:-two} ${Z}") == "1 two "


def test_build_cr_roles_and_plugins():
    a = DeployClusterAction(Config.from_dict({"opensearch": {"namespace": "ns", "cluster_name": "c1"}}))
    a.namespace, a.cluster, a.p = "ns", "c1", {}
    cr = a.build_cr(
        {
            "version": "3.1.0",
            "plugins": ["analysis-icu"],
            "cluster_settings": {"cluster.max_shards_per_node": 2000},
            "node_pools": [{"component": "coord", "replicas": 1, "roles": []}, {"component": "nodes", "replicas": 3}],
        }
    )
    assert cr["apiVersion"] == "opensearch.org/v1"
    assert cr["spec"]["nodePools"][0]["roles"] == []  # empty list must survive (coordinator repro)
    assert cr["spec"]["nodePools"][1]["roles"] == ["cluster_manager", "data", "ingest"]
    assert cr["spec"]["general"]["pluginsList"] == ["analysis-icu"]
    assert cr["spec"]["general"]["additionalConfig"] == {"cluster.max_shards_per_node": "2000"}
    json.dumps(cr)  # must be serialisable


def test_generate_docs_template_ids():
    docs = generate_docs(3, '{"id": {id}, "level": "{random:A,B}", "msg": "doc {id}"}', start=10)
    assert [d["id"] for d in docs] == [10, 11, 12] and docs[0]["level"] in ("A", "B")


def test_observer_violations():
    obs = ClusterObserver(lambda: None, "ns", "c")
    obs.baseline_counts, obs.min_counts, obs.worst_health, obs.max_unready_pods = {"i": 100}, {"i": 90}, "red", 2
    v = obs.violations(min_health="yellow", max_unready_pods=1)
    assert len(v) == 3 and "dropped from 100 to 90" in v[2]
    assert obs.violations(min_health="red", max_unready_pods=None, allow_doc_loss=True) == []


def test_all_playbooks_validate():
    actions = PlaybookExecutor().actions
    from pathlib import Path

    for path in sorted(Path(__file__).parent.parent.glob("playbooks/*.yaml")):
        pb = load_playbook(str(path), global_config={})
        for phase in pb.phases:
            for step in phase.steps:
                assert step.action in actions, f"{path}: unknown action {step.action}"
                unknown = set(step.params) - actions[step.action].params - actions[step.action].common_params
                assert not unknown, f"{path} {phase.name}/{step.action}: unknown params {unknown}"


def test_unknown_config_key_rejected():
    with pytest.raises(ValueError):
        Config.from_dict({"opensearch": {"nmaespace": "x"}})


def test_observer_thread_stops_cleanly():
    obs = ClusterObserver(lambda: (_ for _ in ()).throw(ConnectionError("no cluster")), "ns", "c", interval=1)
    obs.start()
    summary = obs.stop()  # must not raise (Thread internals use _stop)
    assert summary["worst_health"] == "green" and not obs.is_alive()


def test_feature_matchers():
    from oko_test_harness.actions.features import lookup, matches, mismatches

    body = {"index_templates": [{"name": "a", "index_template": {"index_patterns": ["x-*"], "priority": 100}}], "status": {"phase": "RUNNING"}, "n": 3}
    assert lookup(body, "index_templates.0.index_template.priority") == 100 and lookup(body, "$") is body
    assert matches(body["index_templates"], [{"index_template": {"index_patterns": ["x-*"]}}])  # list: subset element match
    assert not matches(body["index_templates"], [{"name": "b"}])
    assert mismatches(body, {"status.phase": "RUNNING", "n": "3"}) == []  # scalars compare as strings too
    assert mismatches(body, {"status.missing": 1, "n": 4}) == ["status.missing missing (expected 1)", "n=3 expected 4"]
