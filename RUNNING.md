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

**Automated (recommended):** `poetry run python runner/oko_runner.py` discovers every playbook, runs the
image-build one first, fans the rest out in parallel (default 2 at a time), then runs the operator-affecting
and migration playbooks alone at the end (see section 4b) — see `runner/oko_runner.py`'s docstring for env vars
(`CONCURRENCY`, `DRY_RUN=1` to preview the plan, etc). It arms its own stuck/anomaly detection (section 2)
and only calls out to Claude Code when something needs a human-grade look, and writes a trace log designed
to be handed to a fresh Claude Code session afterward for a final sanity check. Prefer this for a full-suite run.

**Manual, for one playbook or a hand-picked subset:**
```bash
# one playbook, verbose, log to file, in the background, exit code appended to the .out file
pb=10-basic-3x
nohup bash -c "poetry run oko-test -v --log-file logs/run-$pb.log run playbooks/$pb.yaml > logs/run-$pb.out 2>&1; echo exit=\$? >> logs/run-$pb.out" >/dev/null 2>&1 &

# several playbooks sequentially in a "lane" (writes logs/lane-<name>.txt with rc + duration per playbook)
nohup scripts/run-lane.sh A playbooks/12-coordinator-nodes.yaml playbooks/20-upgrade-minor-2x.yaml >/dev/null 2>&1 &

# the full static-lane suite (superseded by runner/oko_runner.py, kept for reference/manual reruns):
nohup scripts/run-suite.sh >/dev/null 2>&1 &
```

Concurrency: **at most 2 playbooks at a time** on a laptop-class box (three clusters booting at once pushed IO pressure above 50% and made containerd miss stop deadlines); the migration playbooks
must run **alone**, one at a time (they replace the shared operator with a released 2.x chart and upgrade it
back to the local build — `install_operator` refuses to run while any `OpenSearchCluster` exists). The same
rule applies to any playbook with a step that restarts or reconfigures the shared operator Deployment
(`scale_operator`, `kill_operator`, `upgrade_operator`, or an `install_operator` beyond the plain `version: local`):
the runner derives that from the steps (or `metadata.run_alone: true`) and schedules it solo, and `scale_operator`/
`kill_operator` refuse to run while another namespace holds an `OpenSearchCluster`. Every
playbook uses its own random namespace and cluster name, and `install_operator` is idempotent, so
all other playbooks can freely share the k3d cluster and the operator with each other.

Typical durations when images are cached and IO is quiet: basic playbooks 2-3 min, upgrades 10-20 min,
scaling ~15 min, chaos ~15 min. A step that runs longer than its timeout is almost always a real hang; the
per-step backstop (`timeouts.step`, 40 m) fails the playbook instead of waiting forever.

## 2. Monitors to arm (background, then keep working)

`runner/oko_runner.py` does this automatically. Doing it by hand (or watching a manual `run-suite.sh`/
`run-lane.sh` invocation) needs three loops — use the Monitor tool or a background `until` loop, not polling:

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
   a provisioner helper pod stuck `Terminating`, needs attention (section 4).

**Killing runs safely.** `pkill -f <pattern>` kills your own shell if the shell's command line contains a match
(this happened twice: patterns like `oko-test` also matched `kubectl get ns -l oko-test=true` in the same command).
Run the kill in its *own* Bash call with nothing else in it:
```bash
pgrep -fa "scripts/run-lane.sh" | grep -v pgrep | awk '{print $1}' | xargs -r kill   # always match the script path: a bare pattern can match an unrelated process by accident
pgrep -fa "runner/oko_runner.py" | grep -v pgrep | awk '{print $1}' | xargs -r kill
pgrep -fa "bin/python" | grep -E "playbooks/[0-9]" | awk '{print $1}' | xargs -r kill
```
The harness process is `.venv/bin/python -c ...` under poetry; the playbook path is in its arguments.

## 3. Reading results

- `logs/run-<pb>.out`: INFO log + summary. `grep -E "SUCCESS|FAILED"` gives the step timeline.
- `logs/run-<pb>.log`: DEBUG log, every kubectl call and every observer sample (`observer: {...}`).
- On failure the executor writes `logs/<pb>-<ts>/` with the CR, pods, PVCs, events, operator log and every
  OpenSearch pod log, and leaves the namespace in place (`cleanup_on_failure: false`).
- `poetry run oko-test cleanup` deletes every namespace labelled `oko-test=true` (all harness leftovers).
  **Never** run this while other playbooks are still active — it deletes their live clusters too, not just
  finished/failed ones. Safe points: before a run starts, between the parallel phase and the migration
  phase (`run-suite.sh` and `runner/oko_runner.py` both do this automatically), and after everything is done.
- An operation step (upgrade/scale/chaos) reports `worst health`, `max unready pods` and document-count
  minimums observed *during* the operation; a violation there is the signal that matters for the operator.

## 4. Environment quirks (not operator findings)

These recur under host resource pressure and explain failures that are *not* about the operator:

| Symptom | Cause | Action |
|---|---|---|
| Cluster stays yellow, `_cluster/allocation/explain` says "above the low watermark 85%" | k3d volumes report the *host* disk usage | Harness relaxes watermarks to 97/98/99% already (`config.yaml`); keep host disk < 85%. |
| PVCs `Pending` for minutes; a storage-provisioner helper pod stuck `Terminating`; pod stuck `Terminating` anywhere with `FailedKillPod ... DeadlineExceeded` | containerd can't stop containers fast enough under IO pressure (concurrent image imports / several clusters booting) | The harness auto-force-deletes stuck helper pods after a couple of minutes. Manual: `kubectl delete pod ... --force --grace-period=0` (StatefulSet pods recreate from PVC). Run ≤ 2 playbooks concurrently; avoid importing images mid-run. |
| `k3d image import failed`, `ImagePullBackOff` after replacing the operator, or the operator pod stuck `CreateContainerError`/`ContainerCreating` on the k3d server node | Image GC (host > 85% full) evicted the locally-imported image; or the k3s server node (which also runs workloads) is IO-saturated | The harness already re-imports on its fast paths; manual fallback `k3d image import <tag> -c oko`. The server node is already tainted to avoid ordinary workloads and excluded from node-failure injection; cordon it manually if it's still saturated. |
| Host disk shrinking over a long suite (tens of GB in a couple hours) | Docker build cache from repeated operator rebuilds + image churn from GC | Keep operator source stable during a suite (each HEAD change rebuilds/re-imports); `docker builder prune` between suites. |
| **Suite goes yellow-to-broken after several hours**: k3d nodes flap `NotReady`, cert-manager/operator pods `CrashLoopBackOff` on liveness timeouts, or playbooks fail with "no endpoints available for service" on a webhook | `/proc/pressure/io` saturates because **failed playbooks' namespaces pile up for hours** (`cleanup_on_failure: false`) with their clusters still running/indexing | Run `poetry run oko-test cleanup` periodically on a long suite (once failure evidence is collected — see section 3), or split into shorter batches. A genuine operator limitation can look identical to this pattern — always solo-rerun a suspect failure under a quiet host before writing it off as environmental. |
| Cluster yellow 15+ min after two pods force-killed at once; peer recovery stuck at `init`, 0%, no errors | Recovery hung on the *source* node (the one that was killed), not the target | Restart the source node; not an operator issue (the chaos playbook runs this as a non-blocking last phase). |
| `validate_operator_status` reports an operator restart; its previous log ends with `leader election lost` / lease-renewal deadline exceeded | API server answered a lease renewal slower than controller-runtime's default deadline while playbooks rolled pods concurrently (IO pressure) | Environment, not a finding. Rerun the playbook (or its remaining phases as an ad-hoc playbook with a fixed namespace/cluster on the leftover cluster). |

Known, still-unresolved operator limitations (quorum handling on single-manager clusters, certain
concurrent scale/restart interleavings, timing of some failure-detection paths, behavior differences when
adopting a legacy-labeled cluster) are intentionally **not** re-documented here — that list changes as the
operator changes. Check the latest `FINDINGS-*.md` in this repo and the operator's own issue tracker before
concluding a given failure is new.

## 4b. Migration playbooks

- One migration playbook needs an old operator version whose image is no longer published anywhere; the
  harness builds it once from the matching git tag and works around a couple of dependent images that have
  also disappeared from their registries (a proxy sidecar and an init helper) with pinned alternates.
- A couple of intermediate released chart versions cannot be tested at all: they pass a flag to the
  operator binary that an older binary rejects, so no real user could be running them either.
- Every legacy cluster the harness writes uses a non-zero Dashboards replica count except one playbook,
  which deliberately uses zero to document that specific gap.
- One migration playbook toggles a legacy-API compatibility flag off and back on at the end; if it fails
  mid-way the shared operator is left in the "off" state and a later webhook-validation playbook's
  legacy-API check will then fail too — rerun the toggling playbook, or flip the flag back manually via
  `install_operator`. Run it last in the migration order for this reason.
- One migration playbook runs a second cluster in a fixed namespace; it's removed by the playbook itself,
  or by `oko-test cleanup` otherwise.
- One migration playbook is a superset of another (same phases plus extra child-resource checks); run the
  superset immediately before the plain one since they share the legacy-API-toggle ordering constraint above.

`runner/oko_runner.py` and `scripts/run-suite.sh` both encode the same tested-safe solo order; check either
for the exact current sequence rather than assuming it here.

## 5. Version knobs

Defaults come from Docker Hub as of 2026-09-04: `OS_VERSION_2X=2.19.6`, `OS_VERSION_2X_OLD=2.18.0`,
`OS_VERSION_3X_OLD=3.0.0`, `OS_VERSION_3X=3.8.0`, `OPERATOR_PREV=2.8.0` (last published chart that works as a 2.x operator: two intermediate patch releases pass a webhook flag the 2.8.0 binary rejects, the two after that
ship a 3.x alpha; legacy `opensearch.opster.io` API). Dashboards images must exist for the same tag as
OpenSearch (some OpenSearch tags have no matching Dashboards image). Check with:

```bash
curl -s 'https://hub.docker.com/v2/repositories/opensearchproject/opensearch/tags?page_size=100' | python3 -c "import json,sys; print(sorted(x['name'] for x in json.load(sys.stdin)['results']))"
helm search repo opensearch-operator --versions | head
```

## 6. Suite order for a release gate

Prefer `poetry run python runner/oko_runner.py` (section 1) — it discovers playbooks dynamically instead of
needing manual lane rebalancing every time one is added (a recurring source of drift in `run-suite.sh`).

Manually, or via `nohup scripts/run-suite.sh >/dev/null 2>&1 &` (results in `logs/lane-S0..S3.txt`):

1. The image-build playbook alone first, then a couple of other fast smoke playbooks
2. Two parallel lanes covering everything else except the migration playbooks
3. The migration playbooks alone at the end, in their tested-safe order (section 4b)

Judge a failure by category: harness bug (fix and re-run), environment (section 4), or operator behaviour
(collect `logs/<pb>-<ts>/`, the observer summary, and the CR status — that's the finding to report, checked
against the latest `FINDINGS-*.md` and open upstream issues first).
