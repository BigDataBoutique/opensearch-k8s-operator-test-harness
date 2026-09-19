# CLAUDE.md

Release-gate test harness for the OpenSearch Kubernetes Operator (see README.md for what it tests).
This file is intentionally short — see `RUNNING.md` for the full operational guide.

## Running playbooks

- `poetry run python runner/oko_runner.py` is the preferred way to run the suite (or a subset via
  `PLAYBOOKS=31,68,95`). It discovers playbooks dynamically, runs them in parallel where safe and
  the migration playbooks alone at the end, and only calls out to Claude Code (`claude -p`, via
  read-only tools) when a playbook looks stuck, when a non-trivial playbook's success needs a
  sanity check, or when its log contains something outside normal FAILED-reporting noise.
- Every run produces `logs/runner-trace.md`: a single Markdown trace of the whole run (plan,
  timeline, every diagnosis in full, and a closing "needs attention" section). **After a run
  finishes, read that file (or hand it to a fresh session) to check nothing was missed** before
  declaring the suite green.
- `RUNNING.md` covers manual/single-playbook invocation, monitors to arm, and environment quirks
  that are not operator findings — read it before troubleshooting a failure by hand.

## Ground rules

- The harness exists to catch operator bugs — never adjust an assertion just to make a run pass
  without first confirming the underlying behavior is actually correct.
- Findings go in a new `FINDINGS-<round>.md` at the repo root; check the most recent one and any
  open upstream issue before writing up something that might already be known.
- Auxiliary/deprecated CRDs (ISM policies, custom users/roles) are out of scope — don't add
  coverage or file findings against them.
