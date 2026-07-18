# Lane build_idempotency results

## Scope

- Lane: `build_idempotency`
- Requirements: R04, R05
- Base SHA: `2fda48d8ba1fb8cda13878a2e9fb726c984eb0f3`
- Implementation SHA: `a20ee0d7a9f443e28362a109a3b76ce8d81903f4`
- Final lane head: the commit containing this report (`git rev-parse HEAD`)

## Delivered evidence

### R04 — zoned build clock and calendar refresh

- One explicit `Europe/Kaliningrad` build clock resolves effective date and zoned datetime.
- UTC boundary coverage proves `2026-07-18 21:59Z -> 2026-07-18` and `22:00Z -> 2026-07-19` locally.
- Date-only overrides normalize to local midnight; conflicting date/datetime inputs fail closed.
- Removed pinned `2026-06-28` defaults from the runner, Kaggle kernel handoff, and exporter default.
- Clock identity propagates through Fly command, runner config, kernel validation/result, publisher validation, outbox evidence, and durable receipt.
- APScheduler enqueues a local-midnight calendar rollover, and startup performs the same catch-up independently from Smart Update.
- Existing candidates are never mutated; no-op snapshots are deleted before handoff and prior published candidates remain create-only.

### R05 — durable no-op and single flight

- Canonical SHA-256 fingerprint covers date-relevant public event rows and related public tables, effective local date, repo SHA, export/projection/policy versions, related-cache digest, public build configuration, and secret-publication enablement.
- It excludes outbox/Kaggle/metrics churn, generated build/run/candidate ids, queue/generated timestamps, and fully elapsed event churn.
- Additive SQLite tables `static_site_build_state` and `static_site_build_history` persist singleton state, active lease, last success, append-only outcomes, and receipt evidence.
- `BEGIN IMMEDIATE` serializes durable claim/no-op decisions. The outbox enqueue path also uses `BEGIN IMMEDIATE`; concurrent producers retain one pending row, while a running build receives exactly one merged pending follow-up.
- Due-row ownership uses a conditional update and refuses a second running static build across processes.
- The server-side claim occurs before `asyncio.create_subprocess_exec`/Kaggle push. Identical default automatic/manual requests become `noop` with `kaggle_push_count=0`; operator-only `force_rebuild` bypasses fingerprint equality but not the active lease.
- A changed request blocked by an active lease is returned to pending without incrementing attempts or losing payload, then runs after the lease clears.
- Before stale reset, both enqueue and due-job recovery consult the durable Kaggle ledger; a fresh heartbeat/unterminated remote run prevents reset/relaunch.
- Fingerprint and clock are validated and emitted by the runner/kernel result and included in Fly receipt evidence.

## Commands and tests

- `python3 -m py_compile static_site_release.py main.py db.py models.py scheduling.py scripts/run_static_site_builder_kaggle.py kaggle/StaticSiteBuilder/static_site_builder.py site/scripts/export-production-preview-data.py` — passed.
- `uv run --with-requirements requirements.txt pytest -q tests/test_static_site_build_debounce.py tests/test_static_site_release.py tests/test_static_site_build_handoff.py tests/test_static_site_public_gate.py tests/test_static_site_bus_boarding.py tests/test_static_site_content_projection.py tests/test_static_site_preview_duration.py` — **43 passed**.
- Earlier focused run of the four directly assigned suites after the final busy/no-op behaviors — **32 passed**.
- `uv run --with-requirements requirements.txt pytest -q tests/test_scheduling.py tests/test_scheduler_limits.py` — 36 passed, 1 unrelated environment-sensitive failure: `test_scheduler_offsets_and_limits` asserts every registered job has a 30-second grace, while existing enabled non-static jobs use 60–1800 seconds. The new `static_site_calendar_rollover` itself uses 30 seconds.
- `git diff --check` — passed before implementation commit.

## Risks / follow-up

- No live Kaggle push or production deploy was performed in this lane; zero-push behavior is verified by a subprocess-forbidden no-op regression.
- The fingerprint allowlist must be updated when the exporter gains a new public DB input. Version/config changes already provide an explicit invalidation rail.
- The existing production rail continues to hand an immutable full SQLite snapshot to the private Kaggle dataset. This lane does not expand that exposure; a future safe-projection dataset migration remains advisable.
- Canonical docs and `CHANGELOG.md` were intentionally not edited because they are owned by the integration lane.

## Changed files

- `db.py`
- `models.py`
- `main.py`
- `scheduling.py`
- `static_site_release.py`
- `scripts/run_static_site_builder_kaggle.py`
- `kaggle/StaticSiteBuilder/static_site_builder.py`
- `site/scripts/export-production-preview-data.py`
- `tests/test_static_site_build_debounce.py`
- `tests/test_static_site_build_handoff.py`
- `tests/test_static_site_release.py`
- `.codex/lanes/build_idempotency/RESULTS.md`
