# CONTINUATION-DRIFT lane receipt

- Status: **committed**
- Branch: `agent/smart-update-final-code/continuation-drift`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/smart-update-continuation-drift`
- Base: `e46b4365e88a77c238e877af9fe1011f279a6ad1`
- Implementation commit: `7339abd224014d1440b8a377a180ea68159bf14a`

## Delivered C1-C8

- Removed `EXACT_PAGE_REPLAY` as a success/terminal condition. A repeated full page is now a typed `OFFSET_DRIFT`/`NO_PROGRESS` retry with capped exponential backoff, a durable next offset, and no `completed_at` or canonical cursor change.
- Added additive, init-repeatable `deepest_page_ts`/`deepest_page_post_id` storage and model parity. Each fully persisted page is compared with the durable oldest `(date, post_id)` key, so mutable offset alone is not treated as proof of progress.
- Deterministic one-page offset rebasing drains the frozen older tail after `<P`, `=P`, and `>2P` head insertion drift. Every fetched post reaches the raw source-packet path before offset/deepest-boundary state changes.
- Completion remains limited to empty/short pages, backfill horizon, or original incremental cursor overlap.
- An offset-ignoring provider performs one bounded attempt per invocation, remains retryable, and never enters a tight in-process loop or false `done` state.
- Restart, duplicate-packet idempotence, stale-lease recovery, and atomic claim/concurrency behavior remain covered. The continuation worker never updates `vk_crawl_cursor`.
- `Database.init()` idempotently reopens legacy `done/EXACT_PAGE_REPLAY` rows; `_schedule_vk_crawl_continuation()` defensively reopens the same poisoned shape without duplicating the stable continuation key.
- The historical mutable-offset UNIQUE constraint is handled safely: supported keyed rows remain singular, while an additive-schema legacy target-offset collision falls back to durable `OFFSET_DRIFT_COLLISION` retry rather than stale `running` or false completion. The colliding legacy row remains independently due.

## Dynamic acceptance coverage

- A: backlog beyond the primary hard cap drains all source-packet IDs with sequential bounded workers.
- B: backfill continues past its page cap through the horizon.
- C: a mid-page persistence failure does not advance offset/deepest state.
- D: stale running leases recover with owner/run CAS semantics.
- E: concurrent workers cannot process the same row.
- F: repeated completed work is idempotent and does not duplicate packets/rows.
- G: scheduler/producer behavior remains bounded and natural incremental completion does not enqueue spurious work (registration coverage remains on the base scheduler tests).
- H: exact replay, `<P`/`=P`/`>2P` insertion drift, offset-ignoring provider, restart, true cursor terminal, legacy init×2/schedule reopening, and legacy UNIQUE collision all have runtime tests.

## Validation

- `python -m pytest -q tests/test_vk_crawl_continuation.py` → `18 passed`
- `python -m pytest -q tests/test_vk_crawl_continuation.py tests/test_vk_raw_first_llm_contract.py tests/test_db.py` → `68 passed`
- `python3 -m py_compile vk_intake.py db.py models.py tests/test_vk_crawl_continuation.py` → pass
- `git diff --check` → pass
