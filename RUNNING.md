# Running the harness (guide for Claude sessions and humans)

## 0. Environment checklist (2 minutes)

```bash
which k3d helm kubectl docker go make          # k3d + helm live in ~/.local/bin
sysctl vm.max_map_count                        # must be >= 262144 (host kernel is shared with k3d nodes)
df -h /                                        # keep < 80% used: k3d kubelets GC images above 85% and evict pods below 5% free (setup_cluster relaxes this for new clusters)
k3d cluster list; kubectl config current-context   # expect cluster `oko`, context `k3d-oko`
kubectl get pods -n cert-manager -n opensearch-operator-system   # cert-manager + operator Running
poetry install && poetry run pytest -q && poetry run oko-test validate
```

If the k3d cluster does not exist, `setup_cluster` creates it (1 server + 3 agents) and `install_operator`
installs cert-manager and builds/installs the operator from `../opensearch-k8s-operator`. First operator build
takes ~1.5 min; later runs reuse the image (tag = git sha + diff hash) and skip helm when the running image matches.

## 1. How to run

```bash
# one playbook, verbose, log to file, in the background, exit code appended to the .out file
pb=10-basic-3x
nohup bash -c "poetry run oko-test -v --log-file logs/run-$pb.log run playbooks/$pb.yaml > logs/run-$pb.out 2>&1; echo exit=\$? >> logs/run-$pb.out" >/dev/null 2>&1 &

# several playbooks sequentially in a "lane" (writes logs/lane-<name>.txt with rc + duration per playbook)
nohup scripts/run-lane.sh A playbooks/12-coordinator-nodes.yaml playbooks/20-upgrade-minor-2x.yaml >/dev/null 2>&1 &
```

Concurrency: **at most 2 playbooks at a time** on a laptop-class box (three clusters booting at once pushed IO pressure above 50% and made containerd miss stop deadlines); the operator-upgrade / migration playbooks (`50-*` to `55-*`)
must run **alone** (they replace the shared operator with a released 2.x chart and upgrade it back to the local build). Every playbook uses its own random namespace and cluster
name, and `install_operator` is idempotent, so lanes can share the k3d cluster and the operator.

Typical durations when images are cached and IO is quiet: basic playbooks 2-3 min, upgrades 10-20 min,
scaling ~15 min, chaos ~15 min. A step that runs longer than its timeout is almost always a real hang; the
per-step backstop (`timeouts.step`, 40 m) fails the playbook instead of waiting forever.

## 2. Monitors to arm (background, then keep working)

Use the Monitor tool (or a background `until` loop) rather than polling. These three cover everything:

1. **Per-playbook result** (one event when it ends):
   ```bash
   sleep 20; until grep -q "exit=" logs/run-<pb>.out; do sleep 15; done; grep -E "SUCCESS|FAILED|exit=|invariants" logs/run-<pb>.out | cut -c1-400
   ```
2. **Lane progress** (one event per finished playbook):
   ```bash
   n=0; while ! grep -q LANE_DONE logs/lane-A.txt; do c=$(wc -l < logs/lane-A.txt); [ "$c" -gt "$n" ] && tail -n +$((n+1)) logs/lane-A.txt; n=$c; sleep 20; done
   ```
3. **Stuck pods watch** (every 3 min; empty = healthy):
   ```bash
   while true; do kubectl get pods -A --no-headers | awk '$4=="Pending"||$4=="Terminating"||$4~/BackOff|Error|Unknown/{print $1"/"$2" "$4" "$6}' | tr '\n' ';'; echo; sleep 180; done
   ```
   Pods `Pending` for < 1 min right after `deploy_cluster` are normal. Anything Pending > 3 min, or
   `helper-pod-create-pvc-*` in kube-system stuck `Terminating`, needs attention (section 4).

**Killing runs safely.** `pkill -f <pattern>` kills your own shell if the shell's command line contains a match
(this happened twice: patterns like `oko-test` also matched `kubectl get ns -l oko-test=true` in the same command).
Run the kill in its *own* Bash call with nothing else in it:
```bash
pgrep -fa "scripts/run-lane.sh" | grep -v pgrep | awk '{print $1}' | xargs -r kill   # always match the script path: a bare "chain" once matched the desktop's pipewire filter-chain
pgrep -fa "scripts/chain" | grep -v pgrep | awk '{print $1}' | xargs -r kill
pgrep -fa "bin/python" | grep -E "playbooks/[0-9]" | awk '{print $1}' | xargs -r kill
```
The harness process is `.venv/bin/python -c ...` under poetry; the playbook path is in its arguments.

## 3. Reading results

- `logs/run-<pb>.out`: INFO log + summary. `grep -E "SUCCESS|FAILED"` gives the step timeline.
- `logs/run-<pb>.log`: DEBUG log, every kubectl call and every observer sample (`observer: {...}`).
- On failure the executor writes `logs/<pb>-<ts>/` with the CR, pods, PVCs, events, operator log and every
  OpenSearch pod log, and leaves the namespace in place (`cleanup_on_failure: false`).
- `poetry run oko-test cleanup` deletes every namespace labelled `oko-test=true` (all harness leftovers).

An operation step (upgrade/scale/chaos) reports `worst health`, `max unready pods` and document-count minimums
observed *during* the operation; a violation there is the signal that matters for the operator.

## 4. Things that went wrong and what they meant

| Symptom | Cause | Action |
|---|---|---|
| Cluster stays **yellow**, replicas `UNASSIGNED` with reason `REPLICA_ADDED`, `_cluster/allocation/explain` says "above the low watermark 85%" | k3d volumes report the *host* disk usage; the host was 91% full | Harness sets watermarks 97/98/99% via `opensearch.default_cluster_settings` in `config.yaml` (goes into `additionalConfig`). Keep host disk < 85% if you can. |
| PVCs `Pending` for minutes; kube-system `helper-pod-create-pvc-*` stuck `Terminating`; provisioner log "create process timeout after 120 seconds" | k3s local-path provisioner helper pod cannot be stopped by containerd under IO pressure (concurrent image imports / several clusters booting) | `k8s.unstick_local_path_helpers()` force-deletes helpers stuck > 2 min from inside wait loops. Manually: `kubectl delete pod -n kube-system <helper> --force --grace-period=0`. Avoid importing images while playbooks run. |
| `wait_for_cluster_ready` failed with `RollingRestart InProgress` right after creation | Operator rolls pods once after bootstrap (initial cluster-manager setting removed), on multi-pool clusters this takes a few minutes | Expected. `wait_cluster_running` now waits for in-flight `RollingRestart`/`Scaler`/`Upgrader` statuses to clear. |
| Dashboards pod never ready, log `no permissions ... User [name=kibanaserver]` | Harness ships a custom securityconfig; Dashboards default user is `kibanaserver`, which was not defined | Harness now adds the `kibanaserver` internal user, `kibana_server` role mapping, and `dashboards.opensearchCredentialsSecret`. (Whether the operator should validate this is tracked separately.) |
| `helm upgrade --install failed: release name is invalid: /path/to/chart` | Argument order bug | Fixed; keep `helm upgrade --install <release> <chart> ...`. |
| `k3d image import failed: Failed to run tools container` | Two playbooks importing at once | `install_operator` skips build/import when the operator already runs the wanted image and retries imports. |
| IO pressure (`cat /proc/pressure/io` inside a node > 30% `full`) | Image imports + several clusters starting + plugin downloads; an unthrottled background indexer once wrote 2.3M docs in 8 min | Run ≤ 2 playbooks concurrently; pre-pull images *before* starting runs; background `index_documents` is throttled by `rate` (default 100 docs/s). |
| `'Event' object is not callable` at the end of an observed step | Observer thread shadowed `threading.Thread._stop` | Fixed (`_stop_event`); covered by a unit test. |
| Operator pod `ImagePullBackOff` after `kill_operator`; `crictl images` on every node shows no `opensearch-operator:dev-*` | kubelet image GC (host > 85% used) deleted the locally imported image; the replacement pod cannot pull it from anywhere | `install_operator` re-imports on its fast path and `kill_operator` re-imports before killing. Manual fix: `k3d image import opensearch-operator:<tag> -c oko`. New clusters get relaxed GC/eviction kubelet args from `setup_cluster`. |
| Pod stuck `Terminating` for many minutes, event `FailedKillPod ... DeadlineExceeded` | containerd in the k3d node cannot stop the container under IO pressure | `kubectl delete pod --force --grace-period=0`; the StatefulSet recreates it from its PVC. |
| Cluster yellow for 15+ min after two pods were force-killed at once; `_cat/recovery` shows peer recoveries at stage `init`, 0%, no log errors | OpenSearch peer recovery hung on the *source* node (the manager whose java was SIGKILLed in place) | Restart the source node, not the target (target restart re-hangs). Cluster went green in ~1 min with all data. Not an operator issue (FINDINGS N9); `40-chaos` runs this as a non-blocking last phase. |
| `check_opensearch_api` reports "condition not met" for an expected 4xx | `wait_for` predicate returned a `requests.Response`, which is falsy for non-2xx | Fixed: predicates must return a truthy container, never a bare Response. |
| `apply_resource` fails with `namespaces "oko-xxxxx" not found` | Resource applied before `deploy_cluster` created the namespace | Fixed: `apply_resource` ensures the namespace. |
| Operator pod `CreateContainerError` / `ContainerCreating` for minutes on `k3d-oko-server-0`; `cat /proc/pressure/io` in that node > 50% | k3s schedules workloads on the server node too; under IO saturation its containerd misses deadlines, and a node-outage test there takes the API server down | `kubectl cordon k3d-oko-server-0` and recreate the pod. New clusters get `--node-taint=CriticalAddonsOnly=true:NoExecute@server:*` from `setup_cluster`; `inject_node_failure` never picks control-plane nodes. |
| Host disk shrinking during a long suite (43 -> 32 GB free in ~2 h) | Docker build cache from operator rebuilds (+6 GB) and image churn in the k3d node stores caused by kubelet image GC above 85% | Keep the operator source stable during a suite (each HEAD change rebuilds and re-imports), prune `docker builder` cache between suites, keep the host below 85%. |
| A fresh 1-manager cluster never forms; CR says RUNNING; node log `an election requires a node with id [...]` = the bootstrap pod | Operator issue #1448 (bootstrap removed without voting-config exclusion) | Use 3 managers in playbooks that are not about quorum (FINDINGS N10). |
| `50-operator-upgrade`: released chart crash-loops (`flag provided but not defined: -enable-webhooks`), or pod stuck 1/2 on `gcr.io/kubebuilder/kube-rbac-proxy`, or the "released" operator still runs the dev image | Published charts 2.8.1-2.8.4 are broken/pre-release (FINDINGS-round3 N22); helm reuses previous user values when none are passed | Use chart 2.8.0 with `kubeRbacProxy.enable=false` (playbook default); `install_operator` passes `--reset-values`, checks the installed chart version on its fast path and reinstalls released charts from scratch. |
| After a released-chart install every harness pod lookup reports 0 pods / "0/3 ready" although the cluster is green | Operator <= 2.8 labels pods `opster.io/...` and names its Deployment `-controller-manager` | `k8s.get_pods` falls back to the legacy labels and mirrors them; `operator_pods()` / `operator_deployment()` handle both chart generations. |
| `helm uninstall` (or the harness replacing the operator) hangs for 5 min and the namespace stays `Terminating` | The chart templates its CRDs without a keep policy; a leftover CR with the operator's finalizer deadlocks the CRD deletion once the operator is gone (N23/N25) | Never replace the operator while OpenSearchCluster objects exist (`install_operator` refuses); to unstick: `kubectl patch opensearchclusters.<group> <name> -n <ns> --type=merge -p '{"metadata":{"finalizers":[]}}'`. |
| First bulk request after `wait_for_cluster_ready` hangs / times out on a fresh cluster | The bootstrap pod was still a cluster member (often the elected manager) and left seconds later | `wait_for_cluster_ready` now also requires `number_of_nodes` == sum of pool replicas. |
| Every scale-down is followed by a rolling restart of the whole pool, and the next step hits a 503 on the primary being killed | Operator bug: `last-applied` annotation on the pod template encodes `spec.replicas` (FINDINGS-round3 N29) | Scaling actions wait 20 s and re-wait for RUNNING after the Scaler finishes. **Fixed as of a3aa8ab (2026-09-12): `30`, `86` and `90` now pass** — a plain scale-down keeps one member down at a time. What still fails is a scale-down concurrent with a config-change rolling restart (`31` phase 4: Scaler removes `data-2` while RollingRestart takes `data-0`, 6 -> 4 members); see FINDINGS-round6 N29 re-check. |
| Dashboards never ready under an operator <= 2.8 (`Startup probe failed: 503`, log `[ResponseError]`) while the same spec works on 3.x | The harness securityconfig defined no `kibanaserver` internal user; the 3.x operator tolerates that, older operators do not | `create_security_secrets` now defines `kibanaserver` (same password as admin) in `internal_users.yml`. |
| A migrated cluster has lost every REST/Dashboards-created user, role or tenant after the operator upgrade | Operator finding N31 (FINDINGS-round5): adoption re-runs the securityconfig job with the full initial config | Expected while unfixed; `52` asserts it with `continue_on_error`. Not a harness problem. |
| `validate_cluster_configuration` right after `wait_for_cluster_ready` on a **2.x** operator reports one node fewer than the CR (a pod is restarting) | 2.x operators roll every pod once after bootstrap without exposing an in-flight component status, so `wait_cluster_running` returns before the roll | `validate_cluster_configuration` polls its expectations for up to 3 min instead of judging one sample. |
| `validate_operator_status` reports one operator restart; `kubectl logs --previous` ends with `failed to renew lease ... context deadline exceeded` / `leader election lost` | The API server answered a lease renewal slower than controller-runtime's 5 s deadline while two playbooks rolled pods at once (IO pressure); the manager exits by design and is restarted | Environment, not an operator finding (an operator with a longer `RenewDeadline` would ride it out). Rerun the playbook, or its remaining phases as an ad-hoc playbook with a fixed `namespace`/`cluster_name` on the leftover cluster. |
| Unit test `test_observer_thread_stops_cleanly` fails only inside the full pytest run | Observer sample was inside a slow `kubectl` call when `stop()` joined with a 3 s timeout | `stop()` joins for up to 30 s. |
| Dashboards replicas never become ready (`Another OpenSearch Dashboards instance appears to be migrating the index`) | Operator rolls the Deployment 1 s after creation (N27), the resulting pods race the `.kibana_1` migration | Operator finding; **`replicas: 1` is not a workaround** (round-4 `10-basic-3x` hit the same double-ReplicaSet livelock with a single-replica Dashboards spec) — just rerun the playbook. |
| Suite runs go yellow-to-broken after several hours: k3d nodes flap `NotReady`, `cert-manager`/the operator pod itself start `CrashLoopBackOff` on liveness/readiness timeouts, and playbooks fail with "no endpoints available for service" on a webhook | `/proc/pressure/io` saturates (`some` 80-95%+) because failed playbooks' namespaces (`cleanup_on_failure: false`) accumulate for hours with their OpenSearch clusters still running/indexing; nothing purges them mid-suite | Not an operator bug. Run `poetry run oko-test cleanup` periodically during a long suite (or split it into shorter batches) once failure evidence for each namespace has been collected; `50`/`51` will also correctly refuse to run while any `OpenSearchCluster` objects exist (N23), which is itself a symptom of this pile-up, not a new finding. |

## 4b. Migration playbooks (50-55)

- `52` needs operator **2.3.2**, whose image was deleted from every registry (the `opsterio` ECR repo is gone, Docker Hub starts at 2.5.0):
  `install_operator {version: 2.3.2, build_ref: v2.3.2}` builds `opensearch-operator:v2.3.2` from the git tag in `../opensearch-k8s-operator`
  (plain `docker build`, ~3 min once) and runs it under the published 2.3.2 chart. Two more registry casualties are worked around in the
  playbook: chart 2.3.2's `gcr.io/kubebuilder/kube-rbac-proxy:v0.12.0` (same tag exists on `quay.io/brancz`) and the 2.3.2 default
  init helper `public.ecr.aws/opsterio/busybox` (`initHelper.image: busybox:1.36`).
- **2.8.1 cannot be tested**: no `opensearch-operator:2.8.1` image exists and the 2.8.1/2.8.2 charts always pass `--enable-webhooks`
  to the 2.8.0 binary, which rejects the flag (FINDINGS-round3 N22). No user can be running it, so `OPERATOR_PREV` stays 2.8.0.
- Every legacy cluster the harness writes uses `dashboards.replicas: 1` (N25) except `50`, which documents the `replicas: 0` blocker.
- `53` ends by upgrading the operator with `legacyAPI.enabled=false` and then back to `true`; if it fails in between, the shared
  operator is left without the legacy CRDs/webhooks (`70` then fails its `legacy_api_group` phase): rerun `53` or
  `install_operator {version: local, legacy_api: true}` from any playbook. `53` is therefore last in lane S3.
- `54` runs a second cluster in the fixed namespace `oko-mig-b`; the playbook removes it before its last phase, `oko-test cleanup` removes it otherwise.

## 5. Version knobs

Defaults come from Docker Hub as of 2026-09-04: `OS_VERSION_2X=2.19.6`, `OS_VERSION_2X_OLD=2.18.0`,
`OS_VERSION_3X_OLD=3.0.0`, `OS_VERSION_3X=3.8.0`, `OPERATOR_PREV=2.8.0` (last published chart that works as a 2.x operator: 2.8.1 and 2.8.2 pass webhook flags the 2.8.0 binary rejects, 2.8.3 and 2.8.4 ship 3.0.0-alpha; legacy
`opensearch.opster.io` API). Dashboards images must exist for the same tag as OpenSearch (3.3.2 has none, 3.8.0 does).
Check with:

```bash
curl -s 'https://hub.docker.com/v2/repositories/opensearchproject/opensearch/tags?page_size=100' | python3 -c "import json,sys; print(sorted(x['name'] for x in json.load(sys.stdin)['results']))"
helm search repo opensearch-operator --versions | head
```

## 6. Suite order for a release gate

`nohup scripts/run-suite.sh >/dev/null 2>&1 &` runs everything below in one go (10 alone to build the operator image, two lanes, then 50 alone); results land in `logs/lane-S0..S3.txt`. Arm one monitor on those files (section 2). Manually:

1. `10-basic-3x`, `11-basic-2x`, `12-coordinator-nodes` (fast smoke, run first)
2. Lane A: `20-upgrade-minor-2x`, `22-upgrade-minor-3x`, `30-scaling`, `31-scale-and-upgrade-together`
3. Lane B: `21-upgrade-major-2x-to-3x`, `23-upgrade-abort`, `40-chaos`, `41-upgrade-under-chaos`
4. `51`, `50`, `52`, `54`, `55`, `53` alone at the end (each installs a released 2.x operator, then upgrades to the local build; see 4b)

Judge a failure by category: harness bug (fix and re-run), environment (section 4), or operator behaviour
(collect `logs/<pb>-<ts>/`, the observer summary, and the CR status; that is the finding to report).
