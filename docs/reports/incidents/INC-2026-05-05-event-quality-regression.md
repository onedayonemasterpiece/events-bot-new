# INC-2026-05-05 Event Quality Regression

Status: monitoring
Severity: sev2
Service: Telegram Monitoring / VK auto-import / Smart Event Update event quality
Opened: 2026-05-05
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-04-30-tg-monitoring-event-quality-regressions`, `INC-2026-05-01-future-event-quality-audit`, `INC-2026-05-02-pre-daily-event-quality`, `INC-2026-04-26-daily-location-fragments`, `INC-2026-04-20-club-znakomstv-duplicate-event-cards`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/operations/release-governance.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-05-05 a fresh production audit showed that three event-quality classes previously fixed in late April / early May still reproduced in active event inventory: duplicate event cards, prose or unsupported defaults in location fields, and false `is_free` labels.

The incident is production-relevant because these rows can be surfaced by `/daily`, Telegraph event pages, month/day pages, VK daily, and video-announcement candidate pools.

## User / Business Impact

- Readers can see duplicate cards for one real event.
- Readers can see prose fragments or unrelated defaults instead of venue names.
- Paid, included-in-ticket, or giveaway-ticket events can be presented as free.
- Operators lose confidence that prompt-only fixes will hold without global pipeline invariants.

## Detection

- Detected by operator request during CherryFlash catch-up wait on 2026-05-05.
- Production evidence collected from Fly `/data/db.sqlite` snapshot and focused SQL audits.
- Runtime file mirror was checked during the same 2026-05-05 incident work: production has `ENABLE_RUNTIME_FILE_LOGGING=0`, so DB rows, `ops_run`, Fly health, and saved audit artifacts are primary evidence.

Audit artifact directory:

- `artifacts/codex/event-quality-audit-20260505/` in the incident worktree.

## Timeline

- 2026-05-05 09:37 UTC — production active/future event audit generated.
- 2026-05-05 10:08 UTC — focused location-field audit generated.
- 2026-05-05 UTC — high-confidence findings classified: duplicate clusters `4584/4585`, `3864/4188`, `4568/4570`, `3276/3829`, `2758/3655`, `3871/4309`; false-free controls `4448`, `4442`, `4587`; location/prose/default drift controls including `4581`, `4584`, `4585`, `4520`, `4523`, `4524`.
- 2026-05-05 UTC — code fix branch `incident/event-quality-gates-20260505` created from current `origin/main`.

## Root Cause

1. Previous fixes were real but local: prompt hardening and source-specific guards existed, while deterministic fallback code could still override LLM semantics later in the pipeline.
2. `is_free` was not a global invariant. Telegram candidate build and Smart Update create/update paths inferred free from `ticket_price_min=0`, so an unknown/zero/default price could override LLM uncertainty or a false flag.
3. Location recovery still allowed blind default drift. The Kaggle producer filled empty `location_name` with `default_location`, and server-side mismatch fallback could replace unsupported off-site/prose values with the default instead of failing closed.
4. Duplicate matching covered several repost shapes but missed high-confidence anchors where titles were rewritten: same specific ticket URL + date/place with missing time, and near-identical source text + same date/time/place with different title wording.
5. Already-imported active rows were not backfilled after earlier fixes, so production counts mixed old residue with new post-release regressions.

## Contributing Factors

- Telegram, VK, parser, and Smart Update paths all touch event quality fields.
- Some parser/source URLs are generic, so deterministic duplicate matching must stay specific and evidence-based.
- Scheduled imports can finish green while creating suspicious rows because the quality audit is not yet a post-import gate.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring extraction, candidate build, `default_location`, or import persistence;
- changing Smart Update matching, ticket/free handling, or non-event guards;
- changing VK/parser import paths that write `event.is_free`, `location_name`, ticket URL, or source text;
- preparing production deploys that touch public event inventory surfaces.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- `vk_intake.py`
- production SQLite `event`, `event_source`, `event_source_fact`, `ops_run`
- `/daily`, Telegraph event/month/day pages, VK daily, video-announcement event selection

### Mandatory checks before closure or deploy

- Targeted regression tests for:
  - zero ticket price does not imply free;
  - giveaway/included-in-ticket evidence does not imply free;
  - rental/booking availability is skipped as non-event;
  - unsupported off-site/prose location is not replaced by source default;
  - same specific ticket/date/place and near-identical same-slot copies merge.
- `py_compile` for touched modules.
- Fresh release-governance checks: branch from current `origin/main`, clean worktree, changelog/docs synced, deployed SHA reachable from `origin/main`.
- Post-deploy `/healthz` and Fly machine version evidence.
- Production audit/backfill plan: distinguish old active residue from newly blocked classes and repair high-confidence rows separately.

### Required evidence

- Test output and `git diff --check`.
- Deployed SHA, Fly release/version, and deploy path.
- `/healthz` response after deploy.
- Production DB/audit evidence for any data cleanup performed or deferred.

## Immediate Mitigation

- No data repair was performed before code deployment in this branch.
- The code fix is designed to stop recurrence first; old active bad rows require a separate high-confidence cleanup/backfill pass to reduce current counts.

## Corrective Actions

- Telegram candidate build no longer turns `ticket_price_min=0` into `is_free=true`.
- Smart Update create/update paths no longer infer `is_free` from zero price.
- VK intake no longer marks library events free from venue alone; it requires explicit free wording or LLM/source free evidence.
- Smart Update drops contradictory free claims when source evidence says ticket giveaway, included-in-entry-ticket, or positive price.
- Smart Update skips venue/space rental availability posts as `skipped_non_event:rental_booking`.
- Kaggle Telegram producer no longer writes `default_location` into empty `location_name`; it preserves it as source context.
- Telegram server fallback now prefers known/reference evidence over source default after dropping prose-like locations and fails closed instead of defaulting unsupported off-site/prose mismatches.
- Smart Update added same specific ticket/date/place and same-slot near-identical source-text duplicate guards.

## Follow-up Actions

- [ ] Run targeted production cleanup/backfill for high-confidence old active rows and rebuild affected public pages.
- [ ] Add a cheap post-import quality gate that marks `ops_run=partial` and notifies operators when new prose-location/free-contradiction/duplicate smell rows are created.
- [ ] Extend the LLM prompt split with explicit `location_refine`, `is_free_strict`, and `candidate_match_score` stages.

## Release And Closure Evidence

- deployed SHA: `9f3b1c99869de48273070d133b62f0f4f2789b02` (reachable from `origin/main`)
- deploy path: `flyctl deploy --remote-only --app events-bot-new-wngqia`
- deployed image: `events-bot-new-wngqia:deployment-01KQVWBAS2RV23VD6BX62SYA4N`
- Fly machine: `48e42d5b714228`, version `1036`, region `iad`, checks `1 total, 1 passing`, updated `2026-05-05T10:54:31Z`
- regression checks:
  - `artifacts/codex/event-quality-venv/bin/python -m pytest tests/test_tg_monitor_reprocess_incomplete_scan.py tests/test_tg_candidate_location_grounding.py tests/test_smart_event_update_duplicate_guards.py tests/test_smart_event_update_non_event_guards.py tests/test_vk_intake_keywords_dates.py` -> `76 passed`
  - `python3 -m py_compile smart_event_update.py source_parsing/telegram/handlers.py kaggle/TelegramMonitor/telegram_monitor.py vk_intake.py`
  - `git diff --check`
  - release-governance: branch `incident/event-quality-gates-20260505` created from `origin/main`, clean deploy worktree, `HEAD` pushed to `origin/main`; remote `hotfix/INC-2026-05-01-daily-location-drift` is behind `origin/main` and not ahead (`10 0`)
- post-deploy verification:
  - `/healthz`: `ok=true`, `ready=true`, `db=ok`, scheduler/tasks `ok`, `issues=[]`
  - Fly startup logs show `BOOT_OK`, aiohttp listening on `0.0.0.0:8080`, service health check passing, and `video_popular_review` startup catch-up skipped because today's Kaggle handoff already exists.
  - No production data backfill was run in this deploy; old active bad rows remain a separate cleanup step and should not be used to judge recurrence of the new gates until after cleanup or newly imported rows are audited.

## Prevention

This incident is the current regression contract for recurring event-quality defects. Prompt changes are not sufficient closure unless downstream deterministic code also preserves LLM-first semantics and fails closed on unsupported venue/free/duplicate evidence.
