# OKO Test Harness

Release-gate tests for the [OpenSearch Kubernetes Operator](https://github.com/opensearch-project/opensearch-k8s-operator).
Playbooks drive a *locally built* operator through real scenarios on a local Kubernetes cluster and assert what the
**operator** must guarantee: correct topology and node configuration, rolling upgrades (minor and major) with no data
loss, safe scaling up/down, recovery from pod, operator and node failures, and operator upgrades that leave existing
clusters untouched.

The harness tests the operator, not OpenSearch: every operation runs with an observer that samples cluster health,
pod readiness and per-index document counts every few seconds, so a step fails if the cluster went red, if more pods
were down at once than a rolling operation allows, or if any index ever lost documents. After every operation the
operator's own invariants are asserted too: the CR is `RUNNING`, no component status is left dangling, and
`cluster.routing.allocation.enable` / `exclude._name` have been restored by the operator (the harness never "fixes"
them itself).

## Prerequisites

- Docker, `kubectl`, `helm`, `k3d` (or `kind`), Go + `make` (to build the operator), Python 3.9+ with Poetry
- `sysctl vm.max_map_count >= 262144` on the host (k3d/kind nodes share the host kernel)
- The operator source checked out next to this repo (`../opensearch-k8s-operator`), or set `OKO_OPERATOR_PATH`
- cert-manager is installed automatically (the operator's validating webhooks need it)

```bash
poetry install
poetry run oko-test validate            # parse every playbook, check actions and params (no cluster needed)
poetry run pytest                        # unit tests for manifest generation, templates, observer rules
poetry run oko-test run                  # run every playbook in playbooks/ in order
poetry run oko-test run playbooks/21-upgrade-major-2x-to-3x.yaml
poetry run oko-test -v run playbooks/40-chaos.yaml   # debug logging, includes every kubectl call and observer sample
poetry run oko-test cleanup              # delete every namespace the harness created (label oko-test=true)
poetry run oko-test cleanup --all-clusters
```

Version knobs are environment variables with defaults (see `config.yaml` and the playbooks):
`OS_VERSION_2X`, `OS_VERSION_2X_OLD`, `OS_VERSION_3X_OLD`, `OS_VERSION_3X`, `OPERATOR_PREV`, `OKO_PROVIDER`
(`k3d` | `kind` | `existing`), `OKO_K8S_CLUSTER`, `OKO_OPERATOR_PATH`.

On failure the harness always dumps operator logs, the CR, pods, PVCs, events and OpenSearch pod logs into
`./logs/<playbook>-<timestamp>/` and leaves the namespace in place (`cleanup_on_failure: false`).

## Playbooks

| Playbook | What it proves about the operator |
|---|---|
| `10-basic-3x` | 3.x cluster with plugins, `additionalConfig`, Dashboards; pod resources/JVM/PVCs/labels match the CR |
| `11-basic-2x` | 2.19.x with dedicated cluster managers and data nodes |
| `12-coordinator-nodes` | `roles: []` pool yields coordinator-only nodes (issue #1023) |
| `20-upgrade-minor-2x` | Minor rolling upgrade under live indexing, one pod down at a time, no doc loss |
| `21-upgrade-major-2x-to-3x` | Major upgrade of a multi-pool cluster with existing + live data; new data on 3.x |
| `22-upgrade-minor-3x` | Minor 3.x upgrade with plugins reinstalled on the new version |
| `23-upgrade-abort` | Unpullable target version gets stuck; reverting the spec recovers; a real upgrade works afterwards |
| `30-scaling` | Scale data nodes up and down (shard relocation, exclusions cleared), add and remove a node pool |
| `40-chaos` | Pod delete, SIGKILL, cluster-manager loss, two pods at once, operator restart, k8s node outage |
| `41-upgrade-under-chaos` | Operator killed and a pod deleted in the middle of a rolling upgrade; upgrade still completes |
| `50-operator-upgrade` | Cluster made by the previous released operator (`opensearch.opster.io`); upgrading to the local build leaves it untouched, migrates it to `opensearch.org`, and it can still be upgraded |
| `60-cluster-state-survives` | Index template, ISM policy + managed index, persistent setting and internal user created via REST survive a rolling restart (`additionalConfig` change) and an upgrade; the user still authenticates |
| `64-snapshot-repository` | `general.snapshotRepositories` registers an fs repo (1 node, emptyDir + `path.repo`) that can snapshot and restore; settings changes are pushed; removing the entry keeps the repo (documented) |
| `65-tls-custom-certs` | User-provided cert-manager certificates (transport, HTTP, admin client cert) with `generate: false`; the operator mounts them, generates nothing of its own, serves the custom HTTP cert |
| `66-volumes-keystore-config` | `additionalVolumes` (configMap/secret), `keystore` with `keyMappings`, per-pool `additionalConfig` are visible inside the pods; a changed volume with `restartPods` rolls the pool |
| `67-nodepool-scheduling-pdb` | `nodeSelector`, tolerations, labels/annotations, `topologySpreadConstraints`, `affinity`, `pdb` reach pods and PDBs; invalid pdb (both fields) yields a Warning event and is skipped |
| `68-custom-image-pinned` | Pinned `general.image`: version-only change denied by the webhook; image + version change rolls pods with the documented "custom image is pinned" Warning and no Upgrader run |
| `69-version-validation` | 2.x -> 4.0.0 and downgrades refused by the reconciler (Warning/Upgrade event), cluster and `status.version` untouched, operator keeps reconciling, valid upgrade works after revert |
| `70-webhook-validation` | Cluster webhook denials with clear messages: duplicate/empty pool names, TLS without generate/secret, custom HTTP cert without adminSecret, storage class change, legacy `opensearch.opster.io/v1` creation |
| `72-drain-data-nodes` | `drainDataNodes: true`: rolling upgrade of dedicated data nodes stays green with `replicas: 1`; scale-down drains the removed node |
| `73-emptydir-persistence` | `persistence.emptyDir` pool has no PVCs; a deleted pod comes back empty and data is recovered from replicas |
| `74-emptydir-total-loss` | All emptyDir pods force-deleted: `EmptyDirRecovery` event after the grace period, cluster recreated and RUNNING/green |
| `75-disk-resize` | `diskSize` change on a non-expandable storage class: PVC resize attempted, `Failed to Resize` Warning, pods keep running, data intact, revert settles (expansion itself needs `allowVolumeExpansion`) |
| `76-deletion-semantics` | PVCs retained on delete and reused by a same-name cluster (data back); delete mid-upgrade completes; namespace deletion not blocked by finalizers |
| `77-dashboards` | Dashboards `additionalConfig` reaches `opensearch_dashboards.yml`; `dashboards.version` change is a rolling Deployment update with 0 pods unavailable |
| `78-monitoring` | `monitoring.enable` installs `prometheus-exporter` on every node, `/_prometheus/metrics` serves; without prometheus-operator CRDs the ServiceMonitor is skipped with a Warning |
| `79-security-disabled` | `security.tls.http.enabled: false` disables the security plugin: `plugins.security.disabled` in opensearch.yml, no securityconfig job, plain HTTP without credentials |

## Playbook format

```yaml
metadata: {description: "..."}
config:                      # optional, overrides config.yaml
  opensearch: {api_group: opensearch.org}
phases:
  - name: upgrade
    steps:
      - action: index_documents
        background: true     # runs in a thread; joined and asserted at the end of the phase
        params: {index: data-live, count: 200, duration: 8m, min_success_ratio: 0.95}
      - action: upgrade_cluster
        params: {target_version: "3.3.2", max_unready_pods: 1, min_health: yellow}
```

Unknown actions or params are rejected at validation time. `oko-test list-actions` prints every action with its
parameters. Every action accepts `namespace`, `cluster_name` and `timeout`; the namespace and cluster name are
randomised per run unless set in `config`.

### Actions

- **Lifecycle**: `setup_cluster`, `install_operator` (Helm; `version: local` builds the image from source, tags it
  by git sha, imports it into k3d/kind and installs the local chart), `deploy_cluster` (`version`, `node_pools`,
  `plugins`, `cluster_settings`, `dashboards`, `storage_class`, `extra_spec` for anything else in the CR spec),
  `delete_cluster`, `cleanup_cluster`, `set_api_group`
- **Data**: `index_documents` (deterministic `_id`s, optional `duration` for sustained load), `query_documents`,
  `validate_data_integrity` (count + random sample of documents fetched by id)
- **Validation**: `wait_for_cluster_ready`, `validate_cluster_health`, `validate_cluster_version`,
  `validate_cluster_configuration` (roles, plugins, settings, coordinator nodes, CR topology),
  `validate_node_configuration`, `validate_dashboards`, `validate_operator_status` (no restarts, no panics)
- **Change**: `upgrade_cluster`, `upgrade_operator`, `scale_cluster`, `add_node_pool`, `remove_node_pool`
- **Chaos**: `inject_pod_failure` (`delete` | `force_delete` | `kill`, `target: master`), `kill_operator`,
  `inject_node_failure` (stops the k3d/kind node container); all take `delay` so they can strike mid-operation when run
  with `background: true`
- **Diagnostics**: `collect_logs`, `debug_pause`, `update_cluster_settings` (explicit, never used to mask operator bugs)
- **Feature checks** (`actions/features.py`): `apply_resource` (any raw manifest, e.g. cert-manager Certificates, with
  `wait_condition`), `check_opensearch_api` (any REST call, optionally as another `user`; asserts `status`, `expect`
  dotted paths, `contains`/`absent`; polls until it matches), `check_k8s_resource` (`kind` + `name`/`component`/`selector`,
  `expect` dotted paths, `exists: false`, `min_count`), `check_pod_exec` (run a command in the pods, assert output),
  `expect_event` (Kubernetes event by `reason`/`type`/`contains`), `patch_cluster` (merge `patch`, `node_pool`
  read-modify-write, `remove` paths, `wait_phase`, `wait_running`), `expect_rejected` (a `patch`/`node_pool`/
  `cluster_params`/`manifest` must be denied by the webhook with `message`; the CR must be unchanged),
  `delete_namespace`, `delete_cluster_check_pvcs`, `wait_dashboards_version` (rolling update with `max_unavailable`).
  Manifests, names and paths may use `__CLUSTER__` / `__NAMESPACE__` placeholders (the names are random per run).

## Running the suite

See [RUNNING.md](RUNNING.md) for the operational guide: environment checklist, how to launch playbooks and lanes in the background, which monitors to arm, and the failure modes seen so far with their causes.

## Layout

```
oko_test_harness/
  k8s.py          kubectl wrapper, labels, CR read-modify-write, wait helper
  opensearch.py   HTTP client over kubectl port-forward
  observer.py     background sampler + invariant rules
  playbook.py     config.yaml + playbook merge, ${VAR:-default} substitution
  executor.py     phases, background steps, failure diagnostics, teardown
  actions/        one module per area; each action declares its accepted params
playbooks/        the release-gate suite, numbered in execution order
tests/            unit tests (no cluster required)
```
