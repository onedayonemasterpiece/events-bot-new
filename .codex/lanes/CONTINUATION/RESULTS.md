# Lane CONTINUATION Results

## Status
committed

## Requirement IDs
- R-CONT-01: atomic pending/retry/stale claim with `BEGIN IMMEDIATE` + CAS
- R-CONT-02: running lease, locked owner/time, run id, attempts, typed retry/backoff
- R-CONT-03: preserve owner/mode/page/offset/since/horizon/original cursor boundary
- R-CONT-04: raw-first whole-page persistence and post-page-only offset advancement
- R-CONT-05: bounded terminal conditions, stale recovery, concurrency, idempotence
- R-CONT-06: default-on bounded APScheduler worker
- R-CONT-07: producer cap correctness and separate incremental/backfill continuation
- R-CONT-08: remove duplicate `VKInbox.provider_retry_after`
- R-CONT-09: dynamic acceptance coverage A-G plus legacy init compatibility

## Branch
`agent/smart-update-llm-first-final/continuation`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/smart-update-final-continuation`

## Base SHA
`f72dce8164c5b77a22865032dbbedbc4fd0817d9`

## Head SHA
Implementation commit: `e1a4b070c`

## Files changed
- `db.py`
- `models.py`
- `vk_intake.py`
- `scheduling.py`
- `tests/test_vk_crawl_continuation.py`
- `tests/test_scheduling.py`
- `.codex/lanes/CONTINUATION/RESULTS.md` (this receipt)

## Commands run
- `python3 -m py_compile db.py models.py vk_intake.py scheduling.py tests/test_vk_crawl_continuation.py tests/test_scheduling.py`
- `/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q tests/test_vk_crawl_continuation.py tests/test_vk_raw_first_llm_contract.py tests/test_scheduling.py`
- `git diff --check`

## Tests / verification
- PASS: 66 focused tests (`tests/test_vk_crawl_continuation.py`, full raw-first VK contract, full scheduling suite).
- PASS: dynamic backlog beyond initial hard cap reaches all 25 raw packet IDs across sequential bounded workers.
- PASS: backfill continues beyond producer page cap and persists the complete horizon-crossing page.
- PASS: injected mid-page persistence failure leaves offset unchanged; replay is idempotent.
- PASS: expired running lease recovery increments attempt and completes.
- PASS: two concurrent workers claim/process one row only.
- PASS: completed repeat and stable continuation key produce no duplicate row/packet.
- PASS: default-on bounded scheduler job is registered dynamically.
- PASS: legacy continuation table upgrades and `Database.init()` succeeds twice.
- PASS: typed 429 retry respects capped backoff and clears lease ownership.
- PASS: full-page original-cursor overlap and exact replay termination paths.
- PASS: `py_compile` and diff whitespace check.

## Risks
- The worker deliberately does not advance `vk_crawl_cursor`; the immutable continuation row owns only its pagination offset.
- Existing pre-key rows are adopted by immutable boundary fields on the next producer schedule; new rows use a stable unique continuation key.
- Retry is intentionally unbounded in count but bounded in delay; transient source outages remain durable rather than becoming a false terminal.

## Merge notes
- Cherry-pick implementation commit `e1a4b070c`, then this receipt commit.
- No docs/CHANGELOG/source contract/main/vk_auto_queue files were changed, per lane ownership.
- No push performed.
