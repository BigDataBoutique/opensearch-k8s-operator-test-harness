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
| `12-coordinator-nodes` | `roles: []` pool yields coordinator-only nodes (issue #1023); a rolling upgrade with a shard-less pool completes under load |
| `20-upgrade-minor-2x` | Minor rolling upgrade under live indexing, one pod down at a time, no doc loss |
| `21-upgrade-major-2x-to-3x` | Major upgrade of a multi-pool cluster with existing + live data; new data on 3.x |
| `22-upgrade-minor-3x` | Minor 3.x upgrade with plugins reinstalled on the new version |
| `23-upgrade-abort` | Unpullable target version gets stuck; reverting the spec recovers; a real upgrade works afterwards |
| `30-scaling` | Scale data nodes up and down (shard relocation, exclusions cleared), add and remove a node pool |
| `31-scale-and-upgrade-together` | A version bump and a data-pool scale-up in the same reconcile window: Upgrader and Scaler serialised, one pod down at a time, both changes applied; then a scale-down concurrent with a config-change restart: drained node removed first, then the restart, at most one unplanned member down at a time |
| `40-chaos` | Pod delete, SIGKILL, cluster-manager loss, disk loss (pod + PVC), two pods at once, operator restart, k8s node outage (runs alone: it restarts the shared operator) |
| `41-upgrade-under-chaos` | Operator killed and a pod deleted in the middle of a rolling upgrade; upgrade still completes |
| `50-operator-upgrade` | Cluster made by the previous released operator (`opensearch.opster.io`); upgrading to the local build leaves it untouched, migrates it to `opensearch.org` (legacy `dashboards.replicas: 0`, the #1540 case), and it can still be upgraded |
| `51-operator-upgrade-migrated` | Same flow with `dashboards.replicas: 1`, so the migration and the post-migration OpenSearch upgrade are exercised |
| `52-migration-2.3-chart-shape` | Operator **2.3.2** (built from its git tag) with a chart-shaped cluster (`master` role name, Dashboards, drainDataNodes): migration keeps REST-created users/templates/policies/settings, transfers cert-secret ownership, backfills PVC/pod labels, Dashboards stay up, 2.x -> 3.x upgrade of OpenSearch + Dashboards afterwards, deleting the new CR cascades to the legacy CR and secrets |
| `53-migration-2.8-multi-pool-crds` | Operator **2.8.0**, managers + data pool (pdb, labels, nodeSelector, per-pool config), smartScaler, plugins, additionalVolumes, keystore: the cluster migrates with every pod setting intact; scale down/up (PVC reuse) and rolling restart work; `legacyAPI.enabled=false` after the legacy CR is gone restarts nothing |
| `54-migration-edge-states` | Operator **2.8.0**: an emptyDir cluster migrates under live indexing without tripping emptyDir recovery; a cluster caught mid-OpenSearch-upgrade (Upgrader `Upgrading`, one node already on the new version) must be adopted and the upgrade finished by the new operator |
| `55-migration-custom-tls` | Operator **2.8.0** with user-provided cert-manager certificates (`generate: false`): migration generates no secrets of its own, nodes keep serving the custom certs, rolling restart and major upgrade work, deleting the legacy CR leaves the cluster untouched |
| `56-operator-restart-mid-operation` | Operator scaled to 0 mid-drain (node excluded, drain finishes without it) and mid-rolling-restart (data pod down, StatefulSet brings it back): the returned operator resumes from the CR status, removes the drained node / finishes the restart one pod at a time, allocation restored; run alone (owns the operator) |
| `60-cluster-state-survives` | Index template, ISM policy + managed index, persistent setting and internal user created via REST survive a rolling restart (`additionalConfig` change) and an upgrade; the user still authenticates |
| `63-smartscaler-off-vs-drain` | `confMgmt.smartScaler: false` removes a node undrained with the documented Scaler Warning, replicated indices survive, PVC retained; flipped back to true on the live cluster the next scale-down drains an index with `replicas: 0` off the departing node with no document lost |
| `64-snapshot-repository` | `general.snapshotRepositories` registers an fs repo (1 node, emptyDir + `path.repo`) that can snapshot and restore; settings changes are pushed; removing the entry keeps the repo (documented) |
| `65-tls-custom-certs` | User-provided cert-manager certificates (transport, HTTP, admin client cert) with `generate: false`; the operator mounts them, generates nothing of its own, serves the custom HTTP cert |
| `66-volumes-keystore-config` | `additionalVolumes` (configMap/secret), `keystore` with `keyMappings`, per-pool `additionalConfig`, `env`, `initContainers`, `sidecarContainers`, `hostAliases` are visible inside the pods; a changed volume with `restartPods` rolls the pool |
| `67-nodepool-scheduling-pdb` | `nodeSelector`, tolerations, labels/annotations, `topologySpreadConstraints`, `affinity`, `pdb` reach pods and PDBs; invalid pdb (both fields) yields a Warning event and is skipped |
| `68-custom-image-pinned` | Pinned `general.image`: version-only change denied by the webhook; image + version change rolls pods with the documented "custom image is pinned" Warning and no Upgrader run |
| `69-version-validation` | 2.x -> 4.0.0 and downgrades refused by the reconciler (Warning/Upgrade event), cluster and `status.version` untouched, operator keeps reconciling, valid upgrade works after revert |
| `70-webhook-validation` | Cluster webhook denials with clear messages: duplicate/empty pool names, TLS without generate/secret, custom HTTP cert without adminSecret, storage class change, legacy `opensearch.opster.io/v1` creation |
| `71-node-attributes-delete-cleanup` | `general.nodeAttributes` resolves `node.attr.*` from a node label; the per-cluster node-attributes ClusterRoleBinding (cluster-scoped, no owner reference) is removed on delete, also when it is already gone or the CR is deleted while the operator is down |
| `72-drain-data-nodes` | `drainDataNodes: true`: rolling upgrade of dedicated data nodes stays green with `replicas: 1`; scale-down drains the removed node |
| `73-emptydir-persistence` | `persistence.emptyDir` pool has no PVCs; a deleted pod comes back empty and data is recovered from replicas |
| `74-emptydir-total-loss` | All emptyDir pods force-deleted: `EmptyDirRecovery` event after the grace period, cluster recreated and RUNNING/green |
| `75-disk-resize` | `diskSize` change on a non-expandable storage class: PVC resize attempted, `Failed to Resize` Warning, pods keep running, data intact, revert settles (expansion itself needs `allowVolumeExpansion`) |
| `76-deletion-semantics` | PVCs retained on delete and reused by a same-name cluster (data back); delete mid-upgrade completes; a CR deleted while the operator is down is finished when it returns; namespace deletion not blocked by finalizers |
| `77-dashboards` | Dashboards `additionalConfig` reaches `opensearch_dashboards.yml`; `dashboards.version` change is a rolling Deployment update with 0 pods unavailable |
| `78-monitoring` | `monitoring.enable` installs `prometheus-exporter` on every node, `/_prometheus/metrics` serves; without prometheus-operator CRDs the ServiceMonitor is skipped with a Warning |
| `79-security-disabled` | `security.tls.http.enabled: false` disables the security plugin: `plugins.security.disabled` in opensearch.yml, no securityconfig job, plain HTTP without credentials |

### Regression playbooks

Derived from recent operator issues and fixes (open issues are marked; those playbooks are expected to fail until the issue is fixed, which is the point).

| Playbook | Issue | What it proves about the operator |
|---|---|---|
| `80-issue-1448-manager-scale-down` | #1448 (open) | Dedicated manager pool scaled 5 -> 3 and 3 -> 1 under live indexing: API never unreachable > 90s, never red, no member lost beyond the removed ones, voting configuration = surviving managers with no exclusion left, a manager rolling restart still completes. Expected to fail at 3 -> 1 (quorum lost) |
| `81-issue-1448-bootstrap-manager-loss` | #1448 (open) | Elected manager deleted the instant the operator removes the bootstrap pod of a fresh 3-manager cluster: re-election, RUNNING/green, clean voting configuration; same after a later manager loss |
| `82-issue-1449-single-manager-restart` | #1449 (open) | A config change on a one-node cluster still restarts the pod and lands in `opensearch.yml`. Expected to fail (quorum guard requeues forever) |
| `83-issue-1453-crashloop-recovery` | #1453 / PR #1473 | Bad `additionalConfig` crash-loops one pod: Warning event, no second pod taken down; after the fix the operator deletes the stuck pod itself and finishes the restart one pod at a time |
| `84-issue-1329-parallel-pods` | PR #1329 | StatefulSets are `Parallel`/`OnDelete`: a new pool's 3 pods exist within 60s, while a jvm rolling restart and the pool removal still go one pod at a time |
| `85-issue-1476-pool-rename` | PR #1476 | Data pool renamed (remove + add in one apply) while the managers roll: the old pool drains one node at a time to completion, never 2 nodes missing, no doc loss, old STS gone, `exclude._name` cleared |
| `86-issue-1447-drain-fail-closed` | #1447 | Scale-down of a node holding an index pinned to it: node kept with its shards, Scaler Warning + `DrainStalled` after 15m, scale-down completes once unpinned (~25 min) |
| `87-issue-1455-emptydir-readiness-blip` | #1455 | All emptyDir JVMs frozen 3 min (probes fail cluster-wide): no `EmptyDirRecovery` teardown, same pod UIDs, green again with all documents |
| `88-issue-1456-securityconfig-job-retry` | #1456 | Unloadable `roles.yml`: job fails, Warning `Securityconfig update job failed, retrying`, `Securityconfig: Failed` status, cluster keeps serving; repaired secret -> job succeeds |
| `89-issue-1451-tls-rotation` | #1451 | Generated 30-day certs; switching to 365-day + `rotateDaysBeforeExpiry: 60` renews them and the nodes *serve* the new HTTP certificate (>= 300 days) with the cluster RUNNING and no doc loss |
| `90-issue-1450-restart-after-scaler` | #1450 #1471 #1369 | Rolling restart of every pool after a scale-down left a `Scaler` status first: one pod at a time, `allocation.enable` back to `all`, `RollingRestart: Finished`; a manager-only change rolls only the managers |
| `91-issue-1364-limitrange-bootstrap` | #1364 (open) | A namespace `LimitRange` (admission-injected init-container resources) must not send the bootstrap pod into a recreate loop; the cluster forms. May fail while unfixed |
| `92-issue-1078-bootstrap-plugin-settings` | #1078 | A plugin-registered setting (`s3.client.default.*` from `repository-s3`) in `general.additionalConfig` does not crash the bootstrap pod with `unknown setting`; the cluster forms and every node has the plugin and the setting |
| `93-issue-1570-system-index-permission-upgrade` | #1570 (open; PR #1569 / #1579) | `plugins.security.system_indices.permission.enabled: true` with the operator on basic auth: a 3.0.x -> 3.8.x upgrade must not stall after the first pod (the health gate's `_cat/shards/.opendistro_security` gets 403 before allocation is set back to `all`); `allocation.enable` is not left at `primaries`, no doc loss. Fails on the upgrade timeout while unfixed |
| `94-issue-962-pvc-retention-policy` | #962 / PR #1437 | `general.persistentVolumeClaimRetentionPolicy`: unset keeps the current default (retain on scale-down); `whenScaled/whenDeleted: Delete` is propagated onto the StatefulSet, actually deletes the excess-ordinal PVC on the next scale-down, and every PVC is gone after cluster deletion |
| `95-issue-1521-dashboards-user-unmapped` | #1521 / PR #1556 | A custom `roles_mapping.yml` that stops mapping the Dashboards user (neither `users` nor `backend_roles`) emits a `DashboardsUserUnmapped` Warning event instead of failing silently; stays silent by default and once the mapping is restored |
| `96-issue-1530-smartscaler-default` | #1530 / PR #1536 | smartScaler defaults to true even when `spec.confMgmt` is omitted entirely; a scale-down under live indexing with no confMgmt block still excludes + drains the departing data node (no doc loss) |
| `97-issue-1590-scale-down-during-restart` | #1590 (open) | Config-change rolling restart in flight on 3 managers + 3 data, replicas lowered 3 -> 2 while a restarted data pod is still absent: the Scaler must wait for the pool to be whole (never two data members absent), final 3 + 2, no doc loss. Expected to fail while unfixed (Scaler has no readiness gate: 6 -> 4 members) |
| `98-issue-1424-permanently-yellow-restart` | #1424 / #1585 (open) | Permanently yellow cluster (index with more replicas than nodes): config-change restart and version upgrade proceed one pod at a time, never red, `allocation.enable` restored; with `drainDataNodes: true` the restart stalls (documented) but must not leave the candidate in `exclude._name` (expected to fail while #1585 is open) |
| `99-issue-1537-startup-probe-kill-loop` | #1537 | A pool startup probe that never passes: the restarted pod runs OpenSearch but is killed every ~40s (Running, never CrashLoopBackOff for long); the operator flags it `RepeatedlyFailing`, takes no second pod down, and after the probe is repaired deletes the stuck pod itself and finishes the restart |

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
  by git sha, imports it into k3d/kind and installs the local chart; `build_ref: v2.3.2` builds an old operator from a git tag
  and runs it under the published chart of that `version`, for operators whose images no longer exist), `deploy_cluster` (`version`, `node_pools`,
  `plugins`, `cluster_settings`, `dashboards`, `storage_class`, `extra_spec` for anything else in the CR spec),
  `delete_cluster`, `cleanup_cluster`, `set_api_group` (waits up to `timeout` for the CR under the new group)
- **Data**: `index_documents` (deterministic `_id`s, optional `duration` for sustained load), `query_documents`,
  `validate_data_integrity` (count + random sample of documents fetched by id)
- **Validation**: `wait_for_cluster_ready`, `validate_cluster_health`, `validate_cluster_version`,
  `validate_cluster_configuration` (roles, plugins, settings, coordinator nodes, CR topology),
  `validate_node_configuration`, `validate_dashboards`, `validate_operator_status` (no restarts, no panics)
- **Change**: `upgrade_cluster`, `upgrade_operator` (`expect_restart: true|false|any`, `wait_running: false` when the cluster is
  deliberately not RUNNING, `legacy_api`), `scale_cluster`, `add_node_pool`, `remove_node_pool`
- **Chaos**: `inject_pod_failure` (`delete` | `force_delete` | `delete_with_pvc` | `kill`, `target: master`), `kill_operator`, `scale_operator`,
  `inject_node_failure` (stops the k3d/kind node container); all take `delay` so they can strike mid-operation when run
  with `background: true`
- **Diagnostics**: `collect_logs`, `debug_pause`, `update_cluster_settings` (explicit, never used to mask operator bugs)
- **Feature checks** (`actions/features.py`): `apply_resource` (any raw manifest, e.g. cert-manager Certificates, with
  `wait_condition`), `check_opensearch_api` (any REST call, optionally as another `user`; asserts `status`, `expect`
  dotted paths, `contains`/`absent`; polls until it matches), `check_k8s_resource` (`kind` + `name`/`component`/`selector`,
  `expect` dotted paths, `exists: false`, `min_count`), `check_pod_exec` (run a command in the pods, assert output),
  `expect_event` (Kubernetes event by `reason`/`type`/`contains`; `absent: true` proves it never appears within `timeout`), `patch_cluster` (merge `patch`, `node_pool`
  read-modify-write, `remove` paths, `wait_phase`, `wait_running`), `expect_rejected` (a `patch`/`node_pool`/
  `cluster_params`/`manifest` must be denied by the webhook with `message`; the CR must be unchanged),
  `delete_resource` (any object, e.g. a legacy CR; `expect_pods_untouched`), `delete_namespace`, `delete_cluster_check_pvcs`, `wait_dashboards_version` (rolling update with `max_unavailable`).
  Manifests, names and paths may use `__CLUSTER__` / `__NAMESPACE__` placeholders (the names are random per run).
- **Regression checks** (`actions/regressions.py`): `scale_cluster_managers` (manager pool scale under the observer, fails fast
  when the API is unreachable > `max_unreachable`, then checks the voting configuration), `check_voting_config`,
  `delete_pod_after_bootstrap` (deletes the elected manager the moment the bootstrap pod is removed), `rolling_restart`
  (`patch`/`node_pool` change; every pod of `expect_pools` replaced, others untouched, one down at a time, invariants restored),
  `wait_crashloop_recovery`, `edit_node_pools` (`add`/`remove`/`update` pools in one apply, `wait: false` to just apply),
  `pause_pods` (SIGSTOP the JVMs for `duration`; no emptyDir teardown, same pod UIDs), `patch_secret`, `check_served_cert`
  (`min_days`/`max_days` of the HTTP certificate the pods actually serve), `create_namespace`.

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
