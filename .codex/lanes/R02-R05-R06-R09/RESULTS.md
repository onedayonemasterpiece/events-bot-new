# Lane R02-R05-R06-R09 Results

## Status
committed

## Requirement IDs
- R02
- R05
- R06
- R09

## Branch
`feat/unusual-monitor-ops`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/unusual-monitor-ops`

## Base SHA
`7598de224e64659c31325c9f5bc1c39f03c4e6ff`

## Head SHA
Implementation commit: `42c1e07ed1b5f4fab49d4f06556e154129590eaa`

## Files changed
- `.github/scripts/unusual-events-health-issue.py`
- `.github/workflows/unusual-events-production-health.yml`
- `docs/features/unusual-events/unusual-events-health-policy-v1.json`
- `docs/features/unusual-events/unusual-events-health-resolver-v1.schema.json`
- `docs/features/unusual-events/unusual-events-production-health-v1.schema.json`
- `scripts/unusual_events_health.py`
- `tests/test_unusual_events_health.py`

## Delivered
- Strict `unusual-events-health-v1` normalizer/evaluator for the real collection
  BGE cache receipt, Unusual manifest/cache, and StaticSiteBuilder receipt.
- Exact file-SHA, snapshot, fingerprint, build/run, BGE contract, coverage,
  zero-provider-call, feed eligibility, expiry, and dedup checks.
- Bounded package-like evidence with `source`, `bge`, `contracts`,
  `publication`, `quality`, and `feed`, including R08 aliases and no persisted
  base/candidate URLs.
- Provisional configurable target 20/minimum 12, WATCH success, INCIDENT exit 2,
  and a two-distinct-run HEALTHY+READY closure gate.
- Persistent single-issue plan/apply helper.
- Pinned daily/manual/PR workflow with concurrency, timeouts, minimal permissions,
  Fly SSH same-pipeline request/resolver environment contracts, rollout gate,
  honest unsupported-cold blocking, artifacts, summary, and issue lifecycle.

## Commands run
```text
python3 -m py_compile scripts/unusual_events_health.py .github/scripts/unusual-events-health-issue.py
python3 -m json.tool docs/features/unusual-events/unusual-events-health-policy-v1.json
python3 -m json.tool docs/features/unusual-events/unusual-events-health-resolver-v1.schema.json
python3 -m json.tool docs/features/unusual-events/unusual-events-production-health-v1.schema.json
/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_unusual_events_health.py
/home/dev/.codex/venvs/events-bot-new/bin/python (Draft202012Validator.check_schema + sample validation)
/home/dev/.codex/venvs/events-bot-new/bin/python (yaml.safe_load workflow validation)
git diff --check
git diff --cached --check
```

## Tests / verification
- Focused unit/CLI/issue tests: **13 passed**.
- Healthy and BLOCKED output both validated against the Draft 2020-12 schema.
- Workflow parsed as YAML; every third-party action reference is pinned to a
  full commit SHA.
- Python compile, JSON parse, and whitespace checks passed.

## Risks
- The rollout gate defaults closed. Production must provide
  `UNUSUAL_EVENTS_HEALTH_REQUEST_COMMAND` and
  `UNUSUAL_EVENTS_HEALTH_RESOLVER_COMMAND`; until the integrator exposes these
  commands on Fly, scheduled runs correctly classify BLOCKED rather than
  inventing an HTTP endpoint or launching Kaggle directly.
- Sunday cold requests remain BLOCKED unless the same StaticSiteBuilder pipeline
  explicitly advertises `UNUSUAL_EVENTS_HEALTH_SAME_PIPELINE_COLD_SUPPORTED=true`.
- R08 browser CLI is owned by its separate lane. This workflow exposes the
  agreed `UNUSUAL_EVENTS_BASE_URL`, `HEALTH_FILE`, `SCREENSHOT_DIR`, and
  `BROWSER_RECEIPT` env names; the integrator must add the merged R08 command to
  the workflow execution sequence.
- Canonical README/CHANGELOG edits were forbidden in this lane and remain the
  integrator's responsibility.

## Merge notes
- Cherry-pick implementation commit `42c1e07ed1b5f4fab49d4f06556e154129590eaa`.
- Exact builder-boundary CLI:
  `python3 scripts/unusual_events_health.py evaluate --bge-receipt <receipt> --unusual-manifest <manifest> --unusual-cache <cache> --builder-receipt <builder-result-or-success-receipt> --output <health.json> --markdown-output <health.md> [--previous-health <prior.json>]`.
- Same-pipeline resolver CLI:
  `python3 scripts/unusual_events_health.py evaluate-bundle --input <resolver-v1.json> --output <health.json> --markdown-output <health.md> [--previous-health <prior.json>]`.
