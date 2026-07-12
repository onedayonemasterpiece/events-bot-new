# R04 — CI audit results

## Scope and refs

- Requirement: `R04` — explain and repair the hanging incident regression job without disabling unrelated coverage.
- Repository: `onedayonemasterpiece/events-bot-new`.
- Base: `origin/main` at `d09948130e26bea9f2294248f0b987940bc5b869`.
- Head branch: `agent/email-release-followup/ci-audit`.
- Implementation commit: `dedeb0d1334c97685244828597e04db55cb7df9f`.

## Root cause

The GitHub runner was not waiting on an external service. Commit `643444db` first added the full
`tests/test_vk_auto_queue_import.py` file to `.github/workflows/ci.yaml`. One pre-existing test,
`test_vk_auto_queue_rate_limit_marks_row_deferred_for_next_batch`, combined:

1. a mocked `asyncio.sleep` that returned immediately;
2. a real five-second `time.monotonic()` deadline; and
3. a warning on every retry.

That produced a CPU/log busy loop. A local verbose reproduction made about 24,400 retry attempts and
24,594 log lines in one test. After pytest printed its summary, persistent `Database._conn`
`aiosqlite` worker threads also kept the Python process alive indefinitely because the test module did
not close its `Database` instances.

There was a second independent failure hidden behind the hang: PR #25 correctly qualified the
Supabase function argument as `extensions.vector`, but `tests/test_event_identity.py` still asserted
the old unqualified `p_embedding vector` text.

## GitHub evidence

All affected jobs reached `Run incident regression tests` after checkout, dependency installation and
compile succeeded:

- run [29151904682](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/29151904682): six-hour automatic cancellation;
- run [29152207538](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/29152207538): six-hour automatic cancellation;
- run [29153574605](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/29153574605): six-hour automatic cancellation;
- static-site PR #24 run [29163020847](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/29163020847): six-hour automatic cancellation;
- email PR #25 run [29169771947](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/29169771947): manually cancelled after about 12 minutes;
- static release PR #26 run [29182395826](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/29182395826): still in the same pytest step during this audit.

Recent remote release/static branches and PR #26 were inspected. None contains a CI disable or a fix
for the busy loop/connection teardown. The change was therefore not fixed elsewhere and omitted from
`main`; it had not been implemented.

## Fix

- The retry test now advances a deterministic monotonic clock through its mocked sleep. It exercises
  multiple retries and the final defer path without waiting or spinning.
- An autouse async fixture closes every `Database` created by the VK test module through
  `Database.close()`, covering both the persistent raw connection and ORM engine.
- The vector migration assertion now requires the deployed `extensions.vector` signature.
- The CI job retains all four incident regression files, runs pytest with `-vv`, and has a 20-minute
  job timeout. No incident test was removed or skipped.
- The bounded-CI contract and resource-cleanup rule are recorded in
  `docs/operations/release-governance.md` and `CHANGELOG.md`.

## Validation

Exact workflow test selection, run locally under an outer 180-second kill bound:

```text
python -m pytest -vv \
  tests/test_event_vector_sync.py \
  tests/test_event_identity.py \
  tests/test_static_site_build_handoff.py \
  tests/test_vk_auto_queue_import.py
45 passed in 6.97s
process exit 0; total command elapsed 17.90s including uv environment startup
```

The isolated rate-limit regression passed in `0.29s`, process exited `0`, and YAML parsing confirmed
`jobs.python-ci.timeout-minutes == 20`. `git diff --check` passed.

## Risks and follow-up

- GitHub-hosted validation is still required on the PR for this branch. Local evidence proves the
  previous hang and bounded exit, but is not a substitute for a green Actions run.
- Existing already-running jobs use their head commit's old workflow and will not be repaired in
  place; cancel/re-run them only after their branch includes this fix or after the fix reaches `main`.
- The 20-minute timeout is a guardrail, not the primary fix. If this small suite approaches that bound
  later, investigate the named verbose test rather than increasing or disabling the gate blindly.
