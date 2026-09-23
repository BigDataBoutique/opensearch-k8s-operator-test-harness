"""runner/oko_runner.py plan classification: which playbooks may share the operator and which must run alone."""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "runner"))

import oko_runner  # noqa: E402


def _pb(tmp_path, name, body):
    path = tmp_path / f"{name}.yaml"
    path.write_text(body)
    return oko_runner.Playbook(str(path))


PLAIN = """metadata:
  description: "shares the operator"
phases:
  - name: setup
    steps:
      - action: setup_cluster
      - action: install_operator
      - action: deploy_cluster
        params: {version: "3.8.0"}
"""


@pytest.mark.parametrize("use_yaml", [True, False], ids=["yaml", "regex-fallback"])
def test_operator_steps_put_a_playbook_in_the_solo_group(tmp_path, monkeypatch, use_yaml):
    if not use_yaml:
        monkeypatch.setattr(oko_runner, "yaml", None)
    plain = _pb(tmp_path, "30-plain", PLAIN)
    local_install = _pb(tmp_path, "33-local-install", PLAIN.replace("      - action: install_operator\n", "      - action: install_operator\n        params: {version: local, cert_manager: true}\n"))
    scaler = _pb(tmp_path, "76-scaler", PLAIN + "      - action: scale_operator\n        params: {replicas: 0}\n")
    killer = _pb(tmp_path, "40-killer", PLAIN + "      - action: kill_operator\n        background: true\n        params: {delay: 30s}\n")
    upgrader = _pb(tmp_path, "63-upgrader", PLAIN + "      - action: upgrade_operator\n        params: {version: local, legacy_api: false}\n")
    released = _pb(tmp_path, "57-released", PLAIN.replace("      - action: install_operator\n", '      - action: install_operator\n        params: {version: "${OPERATOR_PREV:-2.8.0}", values: {"kubeRbacProxy.enable": "false"}}\n'))
    flagged = _pb(tmp_path, "34-flagged", PLAIN.replace('  description: "shares the operator"\n', '  description: "opted in"\n  run_alone: true\n'))
    build = _pb(tmp_path, "10-build", PLAIN)

    build_first, pool, solo = oko_runner.classify([plain, local_install, scaler, killer, upgrader, released, flagged, build])
    assert [p.name for p in build_first] == ["10-build"]
    assert [p.name for p in pool] == ["30-plain", "33-local-install"]
    # "63-upgrader" calls upgrade_operator, so it's auto-detected as a migration playbook (content, not its
    # number, which deliberately doesn't match any old filename-regex range) and sorts after the others.
    assert [p.name for p in solo] == ["34-flagged", "40-killer", "57-released", "76-scaler", "63-upgrader"]
    assert scaler.solo_reasons == ["scale_operator"] and killer.solo_reasons == ["kill_operator"]
    assert upgrader.solo_reasons == ["migration", "upgrade_operator"] and upgrader.migration
    assert released.solo_reasons == ["install_operator(version=${OPERATOR_PREV:-2.8.0}, values)"] and not released.migration
    assert flagged.solo_reasons == ["metadata.run_alone"]
    assert plain.solo_reasons == [] and local_install.solo_reasons == []


MIGRATION_BODY = PLAIN.replace(
    "      - action: install_operator\n",
    '      - action: install_operator\n        params: {version: "${OPERATOR_PREV:-2.8.0}"}\n',
) + "      - action: upgrade_operator\n        params: {version: local}\n"


def test_migration_playbooks_run_after_the_other_solo_ones_in_tested_order(tmp_path):
    # "51-mig"/"62-mig"/"53-mig" are shaped like the real migration playbooks (non-local install_operator +
    # upgrade_operator, see playbooks/5*-migration-*.yaml) -- migration is detected from those steps, not
    # from the "-mig" name, which exists only to make the assertions below readable.
    names = ["53-mig", "76-scaler", "51-mig", "62-mig", "40-chaos", "20-plain"]
    pbs = [_pb(tmp_path, n, MIGRATION_BODY if n.endswith("-mig") else PLAIN + ("      - action: kill_operator\n" if n in ("76-scaler", "40-chaos") else "")) for n in names]
    build_first, pool, solo = oko_runner.classify(pbs)
    # "20-plain" is the only pool-eligible playbook here, so it becomes build_first rather than sitting in pool
    assert [p.name for p in build_first] == ["20-plain"] and pool == []
    assert [p.name for p in solo] == ["40-chaos", "76-scaler", "51-mig", "62-mig", "53-mig"]
    assert all(p.migration for p in solo[2:]) and not any(p.migration for p in solo[:2])
    assert solo[2].solo_reasons[0] == "migration"


def test_real_playbooks_plan():
    """Every playbook that touches the operator is solo; the migration ones stay solo; the concurrent scale/upgrade playbook stays in the pool."""
    pbs = [oko_runner.Playbook(os.path.join(oko_runner.PLAYBOOKS_DIR, f)) for f in sorted(os.listdir(oko_runner.PLAYBOOKS_DIR)) if f.endswith(".yaml")]
    build_first, pool, solo = oko_runner.classify(pbs)
    solo_names = {p.name for p in solo}
    for touching in ("40-chaos", "41-upgrade-under-chaos", "71-node-attributes-delete-cleanup", "76-deletion-semantics", "50-operator-upgrade", "53-migration-2.8-multi-pool-crds"):
        assert touching in solo_names, touching
    assert "31-scale-and-upgrade-together" in {p.name for p in pool}
    for p in pool:
        assert not oko_runner.operator_steps(p.path), f"{p.name} touches the operator but is in the parallel pool"
    # build_first is just whichever pool-eligible playbook sorts first -- no naming convention required,
    # so this checks that invariant directly rather than hardcoding today's answer ("10-basic-3x").
    assert build_first and all(build_first[0].name < p.name for p in pool)
