# Lane C — transport refresh results

## Scope

- Base: `d169004376c309dc487fa6b48a7aae4a8ed7dea3`
- Branch: `agent/static-release/transport-refresh`
- PR #37 / `origin/integration/event-transport-schedule` used only as evidence for exact-date selection and separate transport ICS. No old UI was copied.

## Result matrix

| Requirement | Status | Evidence |
|---|---|---|
| C1 independent KPPK/bus Kaggle CPU jobs | Done | two kernel directories and two wrapper commands over a shared runner |
| C2 common versioned schema | Done | `transport_refresh/schema.py`; provider/route/stops/date/tz/times/binding/source/validity |
| C3 server validation, last-good, partial/stale rejection | Done | `transport_refresh/store.py`, timeout/empty/stale/recovery tests |
| C4 immutable combined manifest/current pointer | Done | provider and combined snapshot trees, atomic fsync+rename |
| C5 one changed-only coalesced rebuild | Done | semantic content hash + `static_site_build:prod`; unchanged test enqueues zero |
| C6 eligibility/time/venue/ICS/failure checks | Done | `tests/test_transport_refresh.py` |
| Live official-source controlled canary | Blocked | intentionally requires operator-approved real provider adapters/credentials and successful status-ledger evidence |
| Production nightly activation | Blocked | explicitly prohibited until canary; schedule documented only |
| Final event-detail UI integration | Superseded | excluded from this lane; no PR #37 UI copied |

## Configuration / migrations

- New optional runtime paths: `TRANSPORT_MANIFEST_ROOT`, `TRANSPORT_PUBLISH_DB`.
- Provider source is an explicit HTTPS `--source-url`; controlled canary may also mount `--source-payload`.
- Status-aware run needs existing status DB/callback flags, as documented for other Kaggle jobs.
- No database schema migration and no production scheduler/config activation.
- Integrator must add the new canonical page to `docs/routes.yml` and the lane entry to `CHANGELOG.md`.

## Tests

- `TMPDIR=/dev/shm PYTHONDONTWRITEBYTECODE=1 /home/dev/.codex/venvs/events-bot-new/bin/pytest -q -p no:cacheprovider tests/test_transport_refresh.py` — `9 passed`.
- Python AST/compile validation for package, kernels and runner/publisher scripts — passed.
- both independent wrapper `--help` commands — passed.
- `git diff --check` — passed.

## Rollback

Restore the previous reviewed `combined/current.json` target, keep immutable artifacts, and enqueue one `static_site_build:prod`. Disable future provider invocations; no DB migration needs reversal.
