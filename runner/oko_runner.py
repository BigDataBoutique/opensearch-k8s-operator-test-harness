#!/usr/bin/env python3
"""Run every playbook in ../playbooks/, in parallel where it's safe to. Stdlib only (PyYAML is used
to read playbook steps when importable, with a regex fallback).

Discovers playbooks dynamically (no hand-maintained lane lists to rebalance every time one is
added or removed — a recurring source of drift in scripts/run-suite.sh). Three phases:

  1. build-first : the `10-*` playbook alone, if present, so only one `make docker-build` /
                    `k3d image import` race happens (RUNNING.md section 4).
  2. pool        : everything else except the migration playbooks, up to CONCURRENCY at a time
                    (default 2 — see RUNNING.md section 1 for why that ceiling exists).
  3. solo        : one at a time. First every playbook whose steps touch the shared operator
                    Deployment (`scale_operator`, `kill_operator`, `upgrade_operator`, or an
                    `install_operator` that is not the plain idempotent `version: local` install --
                    derived from the playbook's steps, so a new playbook cannot regress it; a
                    playbook can also opt in with `metadata.run_alone: true`), then the migration/
                    operator-upgrade playbooks (`50-55`, `62`) in the tested-safe order from
                    RUNNING.md section 4b. An operator restart under a parallel lane restarts the
                    reconcile of every other lane's cluster and loses its operator log (2026-09-18:
                    playbook 76's scale_operator restarted the operator under playbook 31's rolling
                    restart), and the migration playbooks replace the operator outright and refuse
                    to start while any OpenSearchCluster exists.

A background supervisor thread watches every running/finished playbook and only calls out to
Claude Code (`claude -p`, read-only tools) in three situations:
  - STUCK:    no new line in a playbook's debug log for STUCK_SECS (default 10m) while running.
  - VALIDATE: a "non-trivial" playbook (>= VALIDATE_MIN_STEPS steps) exited 0 — sanity-check the
              step transcript actually looks healthy, not just rc=0.
  - ANOMALY:  a playbook's debug log contains a pattern not already part of the harness's own
              expected FAILED-reporting noise (panics, tracebacks, OOMKills, ...), win or lose.
Each diagnosis is a single non-interactive `claude -p` call with read-only tools (kubectl get/
describe/logs, Read/Grep/Glob — no Edit/Write, no kubectl delete/apply/patch/exec, no git/gh
mutations) and a timeout, so a hung diagnosis can't itself become a stuck resource.

Two log files come out of a run:
  - logs/runner.log       a live, one-line-per-event stream (start/finish/diagnosis-verdict-first-
                           line) -- what to `tail -f` or arm a Monitor on while it's running.
  - logs/runner-trace.md  a single self-contained Markdown trace of the whole run (plan, full
                           timeline, every diagnosis in full, and a closing summary with a "needs
                           attention" section) -- written incrementally so it survives a crash, and
                           meant to be handed to a fresh Claude Code session afterward with a prompt
                           like "read logs/runner-trace.md and check whether anything needs a human
                           that the runner might have missed."

Usage:
  poetry run python runner/oko_runner.py            # interactive TUI if stdout is a tty, else headless
  poetry run python runner/oko_runner.py --headless # force plain stdout even in a terminal
  poetry run python runner/oko_runner.py --tui       # force the curses dashboard
  DRY_RUN=1 poetry run python runner/oko_runner.py   # print the phase plan and exit, run nothing
  PLAYBOOKS=31,68,95 poetry run python runner/oko_runner.py   # restrict to these (numeric prefixes or full names)

Env vars (all optional):
  CONCURRENCY          max playbooks running at once in the pool phase (default 2)
  STUCK_SECS           seconds of debug-log silence before a running playbook is flagged (default 600)
  DIAG_COOLDOWN        seconds before the same stuck playbook can be re-diagnosed (default 900)
  VALIDATE_MIN_STEPS   step count above which a clean pass still gets an LLM sanity check (default 8)
  MODEL                model for diagnostic calls (default claude-sonnet-5 — a stuck/anomaly
                        diagnosis is reading live cluster state and unfamiliar log output and
                        making a real judgment call, not a mechanical text-to-label task)
  CLAUDE_FLAGS          override the default --allowedTools/--disallowedTools (shell word list,
                        shlex-split — mirrors the convention in ../../pulse/ai-sdlc/*.sh)
  AUTO_CLEANUP_FAILED=1 delete a failed playbook's namespace right after its diagnosis finishes
                        (default off — leaves it for human inspection, matching the harness's own
                        cleanup_on_failure: false default; turn this on for long unattended runs
                        where namespace pile-up/IO pressure is the bigger risk, see RUNNING.md 4)
  PLAYBOOKS             comma-separated filter (playbook name or numeric prefix)
  DRY_RUN=1             print the plan, run nothing

Last-known-safe design notes (see RUNNING.md for the underlying incidents):
  - Never call `oko-test cleanup` while the pool phase has active playbooks — it deletes every
    oko-test=true namespace, including live clusters mid-run. Only called at phase boundaries
    (start, and once between pool and solo) where nothing else is running.
  - A step that stops logging is not necessarily hung (an operation can legitimately run 15-20
    min) — STUCK_SECS should stay comfortably below the harness's own 40m per-step backstop, and
    the diagnostic call is read-only and advisory: it never kills or restarts anything on its own.
"""
import collections
import glob
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time

try:
    import yaml
except ImportError:  # the runner stays usable outside the poetry env; the regex fallback below covers the plan
    yaml = None

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLAYBOOKS_DIR = os.path.join(ROOT, "playbooks")
LOGS_DIR = os.path.join(ROOT, "logs")
RUNNER_LOG = os.path.join(LOGS_DIR, "runner.log")
TRACE_LOG = os.path.join(LOGS_DIR, "runner-trace.md")

CONCURRENCY = int(os.environ.get("CONCURRENCY", 2))
STUCK_SECS = int(os.environ.get("STUCK_SECS", 600))
DIAG_COOLDOWN = int(os.environ.get("DIAG_COOLDOWN", 900))
VALIDATE_MIN_STEPS = int(os.environ.get("VALIDATE_MIN_STEPS", 8))
MODEL = os.environ.get("MODEL", "claude-sonnet-5")
AUTO_CLEANUP_FAILED = os.environ.get("AUTO_CLEANUP_FAILED") == "1"
DRY_RUN = os.environ.get("DRY_RUN") == "1"
ONLY = [p.strip() for p in os.environ.get("PLAYBOOKS", "").split(",") if p.strip()]
POLL_SECS = 2  # how often the log-tail thread re-reads a running playbook's debug log
SUPERVISE_SECS = 30  # how often the supervisor scans for stuck/finished playbooks

# Migration / operator-upgrade playbooks: they replace the shared operator and refuse to run
# while any OpenSearchCluster exists (RUNNING.md section 4b) -- must never overlap with anything.
MIGRATION_PATTERNS = [re.compile(r"^5[0-5]-"), re.compile(r"^62-")]
SOLO_ORDER = ["51", "50", "52", "54", "55", "62", "53"]  # tested-safe order; 53 last (toggles legacyAPI)
BUILD_FIRST_PREFIX = "10-"  # if present, run alone first (builds/imports the operator image)
# Actions that restart, replace or reconfigure the shared operator Deployment. A playbook containing any of
# them runs solo: under a parallel lane the restart interrupts every other lane's reconcile and loses the
# operator log of the window they are asserting on. `install_operator` is special-cased in operator_steps().
OPERATOR_ACTIONS = {"scale_operator", "kill_operator", "upgrade_operator"}
INSTALL_OPERATOR_REPLACING_PARAMS = {"values", "values_file", "legacy_api", "build_ref"}


def _install_operator_reason(params):
    """The plain `install_operator` (no params, or `version: local` alone) that starts every playbook is idempotent:
    it returns early when the local chart+image are already running. Anything else runs helm and rolls the operator."""
    params = params or {}
    version = str(params.get("version", "local"))
    extra = sorted(INSTALL_OPERATOR_REPLACING_PARAMS & set(params))
    if version == "local" and not extra:
        return None
    return "install_operator(" + ", ".join(([f"version={version}"] if version != "local" else []) + extra) + ")"


_ACTION_LINE = re.compile(r"^(\s*)-\s*action:\s*['\"]?([A-Za-z_]\w*)")
_INSTALL_PARAM = re.compile(r"\b(values_file|values|legacy_api|build_ref)\s*:")
_INSTALL_VERSION = re.compile(r"\bversion\s*:\s*['\"]?((?:\$\{[^}]*\}|[^'\",}\s])+)")  # `${VAR:-default}` keeps its closing brace


def _steps_from_yaml(text):
    data = yaml.safe_load(text) or {}
    steps = [dict(step) for phase in data.get("phases") or [] for step in phase.get("steps") or []]
    flag = bool((data.get("metadata") or {}).get("run_alone"))
    return steps, flag


def _steps_from_text(text):
    """Fallback without PyYAML: `- action: x` lines, with the lines up to the next step scanned for install_operator params."""
    lines = text.splitlines()
    steps = []
    for i, line in enumerate(lines):
        m = _ACTION_LINE.match(line)
        if not m:
            continue
        block = []
        for nxt in lines[i + 1:]:
            if _ACTION_LINE.match(nxt) or re.match(r"^\s*-\s*name:", nxt) or re.match(r"^\S", nxt):
                break
            block.append(nxt.split("#", 1)[0])
        params = {}
        if m.group(2) == "install_operator":
            body = "\n".join(block)
            params = {k: True for k in _INSTALL_PARAM.findall(body)}
            v = _INSTALL_VERSION.search(body)
            if v:
                params["version"] = v.group(1)
        steps.append({"action": m.group(2), "params": params})
    flag = bool(re.search(r"^\s+run_alone\s*:\s*true\s*$", text, re.M | re.I))
    return steps, flag


def operator_steps(path):
    """Reasons this playbook must run alone: the operator-affecting steps it contains (as short labels) and/or
    the explicit `metadata.run_alone` flag. Empty list = safe to run in the parallel pool."""
    with open(path) as f:
        text = f.read()
    steps, flag = _steps_from_yaml(text) if yaml else _steps_from_text(text)
    reasons = []
    for step in steps:
        action = step.get("action")
        if action == "install_operator":
            reason = _install_operator_reason(step.get("params"))
        elif action in OPERATOR_ACTIONS:
            reason = action
        else:
            reason = None
        if reason and reason not in reasons:
            reasons.append(reason)
    if flag:
        reasons.append("metadata.run_alone")
    return reasons

ANOMALY_PATTERNS = [re.compile(p, re.I) for p in [
    r"\bpanic\b", r"dpanic", r"OOMKilled", r"leader election lost",
    r"unexpected error", r"object has been modified",
]]
# Every action failure is reported via oko_test_harness/actions/base.py as "<action> raised" immediately
# followed by a normal Python traceback -- that's expected, standard FAILED-reporting, not an anomaly, and
# it fires on nearly every failed playbook. A single such failure can also be a *chained* exception, which
# Python renders as multiple "Traceback (most recent call last):" blocks separated by a blank line and one
# of the two standard chain-linking sentences. Only a traceback that follows none of these (a crash that
# escaped the harness's own try/except, e.g. the historical observer-thread "Event object is not callable"
# bug) is worth flagging.
EXPECTED_TRACEBACK_PRECURSOR = re.compile(r"raised\s*$|the following exception:\s*$|exception occurred:\s*$")


def _is_expected_traceback(lines, i):
    """Walk back past blank lines from a 'Traceback (most recent call last):' at index i and check
    whether the nearest non-blank line is one of the standard wrapper/chain-linking phrases."""
    for back in range(1, 5):
        j = i - back
        if j < 0:
            return False
        if lines[j].strip():
            return bool(EXPECTED_TRACEBACK_PRECURSOR.search(lines[j]))
    return False

DEFAULT_ALLOWED = "Bash(kubectl:*),Bash(cat:*),Bash(tail:*),Bash(grep:*),Bash(ls:*),Read,Grep,Glob"
DEFAULT_DISALLOWED = ("Edit,Write,MultiEdit,NotebookEdit,Bash(kubectl delete:*),Bash(kubectl apply:*),"
                       "Bash(kubectl patch:*),Bash(kubectl exec:*),Bash(helm:*),Bash(git push:*),Bash(gh:*)")
if os.environ.get("CLAUDE_FLAGS"):
    CLAUDE_FLAGS = shlex.split(os.environ["CLAUDE_FLAGS"])
else:
    CLAUDE_FLAGS = ["--allowedTools", DEFAULT_ALLOWED, "--disallowedTools", DEFAULT_DISALLOWED]

HEADLESS = "--headless" in sys.argv or not sys.stdout.isatty()
if "--tui" in sys.argv:
    HEADLESS = False

lock = threading.Lock()
combined = collections.deque(maxlen=5000)
paused = threading.Event()


def fmt_delta(secs):
    secs = max(0, int(secs))
    return f"{secs // 3600}h{secs % 3600 // 60:02d}m" if secs >= 3600 else f"{secs // 60}m{secs % 60:02d}s"


def log_event(msg, trace=True):
    stamp = time.strftime("%H:%M:%S")
    with lock:
        combined.append((stamp, msg))
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(RUNNER_LOG, "a") as f:
        f.write(f"{stamp} {msg}\n")
    if trace:
        with open(TRACE_LOG, "a") as f:
            f.write(f"- `{stamp}` {msg}\n")
    if HEADLESS:
        print(f"{stamp} {msg}", flush=True)


def trace_write(text):
    with open(TRACE_LOG, "a") as f:
        f.write(text)


def run_capture(cmd, timeout=60):
    try:
        r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
        return (r.stdout or "") + (r.stderr or "")
    except Exception as e:  # noqa: BLE001 - diagnostic helper, never raise into the caller
        return f"<{' '.join(cmd)} failed: {e}>"


class Playbook:
    def __init__(self, path):
        self.path = path
        self.name = os.path.splitext(os.path.basename(path))[0]
        self.out_path = os.path.join(LOGS_DIR, f"run-{self.name}.out")
        self.log_path = os.path.join(LOGS_DIR, f"run-{self.name}.log")
        self.proc = None
        self.started = self.finished = None
        self.rc = None
        self.last_line = ""
        self.last_progress = None
        self.lines = collections.deque(maxlen=500)
        self.status = "pending"  # pending running success failed
        self.diagnosis = None  # (reason, verdict) of the most recent diagnostic call
        self.last_diagnosed = 0
        self.finalized = False
        self.namespace = None
        self.migration = any(pat.match(self.name) for pat in MIGRATION_PATTERNS)
        self.solo_reasons = []  # why classify() put it in the solo group (empty = parallel pool)

    @property
    def running(self):
        return self.status == "running"

    def start(self):
        os.makedirs(LOGS_DIR, exist_ok=True)
        for p in (self.out_path, self.log_path):
            if os.path.exists(p):
                os.remove(p)
        cmd = ["poetry", "run", "oko-test", "-v", "--log-file", self.log_path, "run", self.path]
        self.started = self.last_progress = time.time()
        self.status = "running"
        log_event(f"[{self.name}] starting")
        outf = open(self.out_path, "w")
        self.proc = subprocess.Popen(cmd, cwd=ROOT, stdout=outf, stderr=subprocess.STDOUT,
                                      text=True, start_new_session=True)
        self._outf = outf
        threading.Thread(target=self._watch, daemon=True).start()

    def _watch(self):
        pos = 0
        while True:
            try:
                with open(self.log_path) as f:
                    f.seek(pos)
                    new = f.read()
                    pos = f.tell()
            except FileNotFoundError:
                new = ""
            if new:
                with lock:
                    self.last_progress = time.time()
                    for ln in new.splitlines():
                        self.lines.append(ln)
                    self.last_line = new.splitlines()[-1]
                if self.namespace is None:
                    m = re.search(r"namespace=(\S+)", new)
                    if m:
                        self.namespace = m.group(1)
            if self.proc.poll() is not None:
                break
            time.sleep(POLL_SECS)
        self.rc = self.proc.returncode
        self._outf.close()
        with open(self.out_path, "a") as f:
            f.write(f"exit={self.rc}\n")
        self.finished = time.time()
        self.status = "success" if self.rc == 0 else "failed"
        log_event(f"[{self.name}] {self.status} rc={self.rc} in {fmt_delta(self.finished - self.started)}")
        self.proc = None

    def kill(self):
        if self.proc:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


def discover_playbooks():
    paths = sorted(glob.glob(os.path.join(PLAYBOOKS_DIR, "*.yaml")))
    pbs = [Playbook(p) for p in paths]
    if ONLY:
        def wanted(pb):
            prefix = pb.name.split("-")[0]
            return pb.name in ONLY or prefix in ONLY
        pbs = [p for p in pbs if wanted(p)]
    return pbs


def classify(pbs):
    """(build_first, pool, solo). A playbook is solo when it is a migration playbook (name pattern), when its steps
    touch the shared operator (operator_steps()), or when it sets metadata.run_alone. Non-migration solo playbooks
    come first (by name), then the migration ones in SOLO_ORDER, so a failed migration playbook's leftover chart
    never bleeds into the others."""
    build_first = [p for p in pbs if p.name.startswith(BUILD_FIRST_PREFIX)]
    rest = [p for p in pbs if p not in build_first]
    for p in rest:
        p.solo_reasons = (["migration"] if p.migration else []) + operator_steps(p.path)
    solo = [p for p in rest if p.solo_reasons]
    pool = [p for p in rest if p not in solo]

    def solo_key(p):
        prefix = p.name.split("-")[0]
        if not p.migration:
            return (0, 0, p.name)
        return (1, SOLO_ORDER.index(prefix) if prefix in SOLO_ORDER else len(SOLO_ORDER), p.name)

    solo.sort(key=solo_key)
    return build_first, pool, solo


def solo_label(p):
    return f"{p.name} ({', '.join(p.solo_reasons)})"


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------

def run_pool(items, concurrency):
    """Run `items` with up to `concurrency` active at once; blocks until all are done."""
    idx = 0
    active = []
    while idx < len(items) or active:
        active = [p for p in active if p.running]
        while not paused.is_set() and len(active) < concurrency and idx < len(items):
            pb = items[idx]
            idx += 1
            pb.start()
            active.append(pb)
        time.sleep(3)


def run_solo(items):
    for pb in items:
        while paused.is_set():
            time.sleep(3)
        pb.start()
        while pb.running:
            time.sleep(3)


# ---------------------------------------------------------------------------
# Diagnostics (the only place this tool calls out to an LLM)
# ---------------------------------------------------------------------------

def diagnose(pb, reason, prompt):
    log_event(f"[{pb.name}] diagnosing ({reason})...", trace=False)
    try:
        cmd = ["claude", "-p", prompt, "--model", MODEL, "--max-turns", "6", *CLAUDE_FLAGS]
        r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=180)
        verdict = (r.stdout or r.stderr or "(empty response)").strip()
    except Exception as e:  # noqa: BLE001 - a failed diagnosis must not crash the runner
        verdict = f"<diagnosis failed: {e}>"
    pb.diagnosis = (reason, verdict[:4000])
    first_line = verdict.splitlines()[0][:160] if verdict else "(empty)"
    log_event(f"[{pb.name}] {reason} verdict: {first_line}", trace=False)
    trace_write(f"\n#### [{pb.name}] {reason} — `{time.strftime('%H:%M:%S')}`\n\n{verdict}\n")


def build_stuck_prompt(pb):
    tail = "\n".join(list(pb.lines)[-80:])
    ns_state = run_capture(["kubectl", "get", "pods,jobs,events", "-n", pb.namespace,
                             "--sort-by=.lastTimestamp"]) if pb.namespace else "(namespace not identified yet)"
    return f"""Playbook `{pb.name}` in the OKO OpenSearch-operator test harness has logged nothing new for
over {STUCK_SECS // 60} minutes and may be stuck. Repo: {ROOT} (see RUNNING.md section 4 for
known environmental hang patterns, e.g. IO-pressure node flaps, and section 5 for known operator
behavior quirks that can look like a hang).

Last ~80 debug-log lines:
{tail}

Current cluster state (namespace {pb.namespace or 'unknown'}):
{ns_state}

In 3-5 sentences: is this a real hang, a known pattern from RUNNING.md (name it), or a slow-but-
normal operation? End with one line: WAIT, INVESTIGATE, or KILL_AND_RETRY, plus your reasoning."""


def build_validate_prompt(pb, anomalies=None):
    out = read_tail(pb.out_path, 15000)
    anomaly_note = (f"\nNote: its debug log also matched these anomaly patterns, worth weighing in: "
                     f"{anomalies}\n") if anomalies else ""
    return f"""Playbook `{pb.name}` in the OKO OpenSearch-operator test harness just exited 0 (reported
success). It is non-trivial (>= {VALIDATE_MIN_STEPS} steps), so give its transcript a sanity
check before we trust the green result. Repo: {ROOT}.
{anomaly_note}
Full run transcript (logs/run-{pb.name}.out):
{out}

In 2-4 sentences: does this look like a genuinely healthy pass, or is there anything suspicious
(a silently-skipped check, a suspiciously-low document count, a step that took far longer than
its neighbors, a warning buried in an otherwise-green step)? End with one line: PASS or CONCERN,
plus why."""


def build_anomaly_prompt(pb, matches):
    tail = "\n".join(list(pb.lines)[-120:])
    return f"""Playbook `{pb.name}` in the OKO OpenSearch-operator test harness ({'succeeded' if pb.status == 'success' else 'failed'})
but its debug log contains lines matching an anomaly pattern not part of the harness's normal
FAILED-reporting output: {matches}. Repo: {ROOT} (check RUNNING.md and any FINDINGS-round*.md for
whether this is already a known/tracked pattern).

Last ~120 debug-log lines:
{tail}

In 2-4 sentences: is this worth a human looking at (a real operator issue or a new harness bug),
or is it already-known/benign noise? End with one line: BENIGN or WORTH_A_LOOK, plus why."""


def read_tail(path, max_chars):
    try:
        with open(path) as f:
            data = f.read()
        return data[-max_chars:]
    except OSError:
        return "(log file not found)"


def count_steps(pb):
    text = read_tail(pb.out_path, 50000)
    return len(re.findall(r"^\s*[✓✗]\s", text, re.M))


def scan_anomalies(pb):
    text = read_tail(pb.log_path, 200000)
    hits = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if "Traceback (most recent call last)" in line and not _is_expected_traceback(lines, i):
            hits.append("unwrapped Traceback (did not follow a normal '<action> raised' report or exception chain)")
    for pat in ANOMALY_PATTERNS:
        m = pat.search(text)
        if m:
            hits.append(m.group(0))
    return hits


def finalize(pb):
    anomalies = scan_anomalies(pb)
    if pb.status == "success" and count_steps(pb) >= VALIDATE_MIN_STEPS:
        diagnose(pb, "validate", build_validate_prompt(pb, anomalies))
    elif anomalies:
        diagnose(pb, "anomaly", build_anomaly_prompt(pb, anomalies))
    if pb.status == "failed" and AUTO_CLEANUP_FAILED and pb.namespace:
        log_event(f"[{pb.name}] AUTO_CLEANUP_FAILED: deleting namespace {pb.namespace}")
        run_capture(["kubectl", "delete", "ns", pb.namespace, "--wait=false"])


diag_threads = []  # live Thread objects for in-flight diagnose()/finalize() calls, protected by `lock`


def _spawn(target, args):
    t = threading.Thread(target=target, args=args, daemon=True)
    with lock:
        diag_threads.append(t)
    t.start()


def supervisor(all_pbs, stop_event):
    while not stop_event.is_set():
        now = time.time()
        for pb in all_pbs:
            if pb.status == "running":
                if now - pb.last_progress > STUCK_SECS and now - pb.last_diagnosed > DIAG_COOLDOWN:
                    pb.last_diagnosed = now
                    _spawn(diagnose, (pb, "stuck", build_stuck_prompt(pb)))
            elif pb.status in ("success", "failed") and not pb.finalized:
                pb.finalized = True
                _spawn(finalize, (pb,))
        time.sleep(SUPERVISE_SECS)


def wait_for_diagnoses(timeout_each=200):
    """Block until every in-flight diagnose()/finalize() thread has finished (each `diagnose()` call
    already times out on its own at 180s, so this can't hang indefinitely). Called after the last
    phase so a diagnosis that started near the end of the run isn't silently dropped when the
    process exits (daemon threads are killed outright on interpreter shutdown, mid-write)."""
    while True:
        with lock:
            live = [t for t in diag_threads if t.is_alive()]
        if not live:
            return
        log_event(f"waiting for {len(live)} in-flight diagnosis(es) to finish before summarizing...",
                   trace=False)
        for t in live:
            t.join(timeout=timeout_each)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def orchestrate(build_first, pool, solo, stop_event):
    all_pbs = build_first + pool + solo
    start_trace(build_first, pool, solo)
    threading.Thread(target=supervisor, args=(all_pbs, stop_event), daemon=True).start()

    log_event("cleanup: clearing any leftover namespaces before starting")
    run_capture(["poetry", "run", "oko-test", "cleanup"], timeout=120)

    if build_first:
        log_event(f"phase build-first: {[p.name for p in build_first]}")
        run_pool(build_first, 1)

    if pool:
        log_event(f"phase pool: {len(pool)} playbooks, concurrency={CONCURRENCY}")
        run_pool(pool, CONCURRENCY)

    solo_ops = [p for p in solo if not p.migration]
    migration = [p for p in solo if p.migration]
    if solo_ops:
        # nothing else may run while these restart/scale the shared operator; leftovers from the pool are still
        # harmless to them, but clearing now keeps IO pressure down (RUNNING.md section 4)
        log_event("cleanup: clearing namespaces before the solo (operator-affecting) phase")
        run_capture(["poetry", "run", "oko-test", "cleanup"], timeout=120)
        log_event(f"phase solo: {[solo_label(p) for p in solo_ops]}")
        run_solo(solo_ops)
    if migration:
        log_event("cleanup: clearing namespaces before the migration phase")
        run_capture(["poetry", "run", "oko-test", "cleanup"], timeout=120)
        log_event(f"phase migration: {[p.name for p in migration]}")
        run_solo(migration)

    # give the supervisor one more sweep to catch the last finished playbook before stopping it, then
    # a short buffer for that in-progress sweep to finish *starting* any last diagnosis thread, then
    # actually wait for every diagnosis to finish (not just guess a sleep long enough) before summarizing
    time.sleep(SUPERVISE_SECS + 5)
    stop_event.set()
    time.sleep(3)
    wait_for_diagnoses()
    print_summary(all_pbs)


CONCERN_KEYWORDS = ("CONCERN", "WORTH_A_LOOK", "KILL_AND_RETRY", "INVESTIGATE")


def start_trace(build_first, pool, solo):
    with open(TRACE_LOG, "w") as f:
        f.write(f"""# oko_runner trace

- started: {time.strftime('%F %T')}
- model: {MODEL}
- concurrency: {CONCURRENCY}
- build-first: {[p.name for p in build_first]}
- pool ({len(pool)}): {[p.name for p in pool]}
- solo ({len(solo)}): {[solo_label(p) for p in solo]}

## Timeline
""")


def print_summary(all_pbs):
    ok = [p for p in all_pbs if p.status == "success"]
    bad = [p for p in all_pbs if p.status == "failed"]
    log_event(f"DONE: {len(ok)} passed, {len(bad)} failed, {len(all_pbs)} total")

    needs_attention = []
    for p in bad:
        note = f" -- {p.diagnosis[0]}: {p.diagnosis[1].splitlines()[0][:160]}" if p.diagnosis else " -- no diagnosis triggered"
        log_event(f"  FAILED {p.name} rc={p.rc}{note}")
        needs_attention.append(f"- **{p.name}** (rc={p.rc}){note} — see `logs/run-{p.name}.out`")
    for p in ok:
        if p.diagnosis and any(k in p.diagnosis[1].upper() for k in CONCERN_KEYWORDS):
            needs_attention.append(f"- **{p.name}** (passed, but {p.diagnosis[0]} flagged it) — "
                                    f"{p.diagnosis[1].splitlines()[-1][:200]}")

    trace_write(f"""
## Final summary — {time.strftime('%F %T')}

{len(all_pbs)} total, {len(ok)} passed, {len(bad)} failed.

## Needs attention

{chr(10).join(needs_attention) if needs_attention else "Nothing flagged: every failure category above already fully explains itself, and no successful-but-flagged verdicts came back."}
""")
    log_event(f"trace written to {TRACE_LOG}")


def print_plan(build_first, pool, solo):
    print(f"Build-first ({len(build_first)}): {[p.name for p in build_first]}")
    print(f"Parallel pool, concurrency={CONCURRENCY} ({len(pool)}): {[p.name for p in pool]}")
    print(f"Solo/sequential ({len(solo)}):")
    for p in solo:
        print(f"  {solo_label(p)}")


# ---------------------------------------------------------------------------
# curses TUI (optional; headless mode is the default and is what a background/agent run uses)
# ---------------------------------------------------------------------------

def tui_main(scr, all_pbs, stop_event):
    import curses
    curses.curs_set(0)
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN, -1)
    curses.init_pair(2, curses.COLOR_RED, -1)
    curses.init_pair(3, curses.COLOR_YELLOW, -1)
    scr.timeout(500)
    sel = 0
    scroll = 0
    quit_armed = False

    while True:
        h, w = scr.getmaxyx()
        scr.erase()

        def put(y, x, s, attr=0):
            if 0 <= y < h and x < w:
                scr.addnstr(y, x, s, w - x - 1, attr)

        counts = collections.Counter(p.status for p in all_pbs)
        put(0, 1, f"oko_runner  {counts['success']} ok  {counts['failed']} failed  "
                  f"{counts['running']} running  {counts['pending']} pending  "
                  f"{'PAUSED' if paused.is_set() else ''}", curses.A_BOLD)
        put(0, w - 10, time.strftime("%H:%M:%S"))

        list_h = min(len(all_pbs), max(3, h // 2))
        for i, pb in enumerate(all_pbs[:list_h]):
            attr = 0
            if pb.status == "running":
                attr = curses.color_pair(1)
            elif pb.status == "failed":
                attr = curses.color_pair(2)
            elif pb.diagnosis:
                attr = curses.color_pair(3)
            dur = fmt_delta(time.time() - pb.started) if pb.started and not pb.finished else \
                  (fmt_delta(pb.finished - pb.started) if pb.started else "-")
            diag = f" [{pb.diagnosis[0]}]" if pb.diagnosis else ""
            marker = ">" if i == sel else " "
            put(1 + i, 1, f"{marker}{pb.name:<45}{pb.status:<9}{dur:<8}{pb.last_line[:60]}{diag}",
                attr | (curses.A_REVERSE if i == sel else 0))

        sep_y = 1 + list_h
        put(sep_y, 0, "── activity " + "─" * w)
        with lock:
            src = list(combined)
        body_h = h - sep_y - 2
        scroll = min(scroll, max(0, len(src) - body_h))
        end = len(src) - scroll
        for j, (stamp, line) in enumerate(src[max(0, end - body_h):end]):
            put(sep_y + 1 + j, 0, f"{stamp} {line}")

        keys = "↑↓ select  d diagnosis  k kill sel  K kill all  p pause  q quit"
        if quit_armed:
            keys = "playbooks still running: press q again to kill+quit, any other key to stay"
        put(h - 1, 1, keys, curses.A_REVERSE)
        scr.refresh()

        k = scr.getch()
        if k == -1:
            if stop_event.is_set() and not any(p.running for p in all_pbs):
                return
            continue
        if k == ord("q"):
            if not any(p.running for p in all_pbs) or quit_armed:
                for p in all_pbs:
                    p.kill()
                return
            quit_armed = True
            continue
        quit_armed = False
        if k == curses.KEY_UP:
            sel = max(0, sel - 1)
        elif k == curses.KEY_DOWN:
            sel = min(list_h - 1, sel + 1)
        elif k == ord("p"):
            paused.clear() if paused.is_set() else paused.set()
        elif k == ord("k") and all_pbs:
            all_pbs[sel].kill()
        elif k == ord("K"):
            for p in all_pbs:
                p.kill()
        elif k == ord("d") and all_pbs and all_pbs[sel].diagnosis:
            pass  # full text is in logs/runner-diagnoses.log; the status line already shows reason+first line


def main():
    pbs = discover_playbooks()
    if not pbs:
        print(f"No playbooks found under {PLAYBOOKS_DIR}" + (f" matching {ONLY}" if ONLY else ""))
        sys.exit(1)
    build_first, pool, solo = classify(pbs)
    if DRY_RUN:
        print_plan(build_first, pool, solo)
        return
    all_pbs = build_first + pool + solo
    stop_event = threading.Event()

    def handle_sigint(signum, frame):
        log_event("SIGINT: killing all running playbooks")
        for p in all_pbs:
            p.kill()
        sys.exit(130)

    signal.signal(signal.SIGINT, handle_sigint)

    if HEADLESS:
        orchestrate(build_first, pool, solo, stop_event)
    else:
        import curses
        threading.Thread(target=orchestrate, args=(build_first, pool, solo, stop_event), daemon=True).start()
        curses.wrapper(tui_main, all_pbs, stop_event)
        print_summary(all_pbs)


if __name__ == "__main__":
    main()
