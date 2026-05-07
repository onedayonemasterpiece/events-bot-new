# INC-2026-04-30 Telegram Monitoring Event Quality Regressions

Status: monitoring
Severity: sev2
Service: Telegram Monitoring extraction / Smart Update matching / public event inventory
Opened: 2026-04-30
Closed: —
Owners: Codex
Related incidents: `INC-2026-04-30-tg-monitoring-work-schedule-false-skips`, `INC-2026-04-26-daily-location-fragments`, `INC-2026-04-20-club-znakomstv-duplicate-event-cards`
Related docs: `docs/llm/request-guide.md`, `docs/llm/prompts.md`, `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-04-30 the public inventory contained several Telegram-imported quality regressions from the current-day monitoring batch:

- real events missing from the earlier work-schedule false-skip incident;
- work-hours notices imported as public event cards;
- duplicate cards for the same real event;
- many paid or unknown-price events marked as free.
- A later full production audit of active future `is_free=1` rows found the free-label issue was broader than the initially reported controls: 69 additional future public rows had no explicit free-attendance evidence or had paid/ticket signals without free evidence.

The fix for this incident must remain LLM-first: prompt/schema contracts own the semantic decisions (`is_free`, work-hours-vs-event, venue/title meaning, duplicate semantics). Deterministic code is allowed only as narrow output consistency/safety plumbing.

## User / Business Impact

- Users could see library holiday work-hour cards as cultural events.
- Users could see duplicate cards for the same wine tasting and the same `Музейная ночь` program.
- Users could see paid-ticket events under the free marker, including standup, museum-night, concert, zoo, and tasting cards.
- The bot's `/daily`, month/weekend pages, recommendations, and video/event selection inherited the bad labels and duplicates until remediation.

## Detection

- Detected by operator report in the Codex session on 2026-04-30 after the first work-schedule incident was closed.
- Production file log mirror was checked: `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, directory existed but had no active or rotated logs. Fallback evidence came from production SQLite and Fly status/health.
- Fly health at investigation start: app `events-bot-new-wngqia`, machine `48e42d5b714228`, version `1027`, checks `1 total, 1 passing`; `/healthz` returned `ok=true`, `ready=true`, `db=ok`.
- Follow-up mass audit snapshot on 2026-04-30 12:46 UTC found 120 active future public rows still marked `is_free=1`; 51 had explicit free evidence or explicit free+registration evidence, while 69 lacked explicit free evidence under the incident contract.

## Timeline

- 2026-04-29 01:29 UTC — event `4350` was imported from `https://t.me/kulturnaya_chaika/7615` as free: `Винные дегустации с сомелье Ольгой Скобовой`.
- 2026-04-29 21:40 UTC — scheduled `tg_monitoring` run `957` started.
- 2026-04-30 01:00 UTC — event `4396` was imported from `https://t.me/terkatalk/4784` as a duplicate/free card for the same 2026-05-01 19:00 wine tasting slot.
- 2026-04-30 01:00 UTC — events `4398`-`4401` were imported from `https://t.me/kaliningradlibrary/2219` as four public cards for a library work schedule.
- 2026-04-30 01:xx UTC — multiple paid/unknown-price events were imported with `is_free=1`.
- 2026-04-30 06:19 UTC — earlier incident `INC-2026-04-30-tg-monitoring-work-schedule-false-skips` was deployed; it fixed false skips of real museum/library events but did not remediate these already-imported event-quality rows.
- 2026-04-30 — this incident was opened to handle the remaining event-quality regressions and prompt-first remediation.
- 2026-04-30 12:46 UTC — operator requested a full production audit of all future free-labeled events; audit found 69 additional future rows where the free marker did not meet the explicit-evidence contract.
- 2026-04-30 12:55 UTC — production data remediation cleared `is_free` on those 69 rows and enqueued individual Telegraph/page rebuild jobs.

## Root Cause

1. The main Telegram Monitoring Gemma prompt had a work-hours rule, but `schedule_like` messages bypass the main prompt and enter the schedule-rescue prompt. That rescue prompt did not repeat the work-hours/non-event contract, so the library holiday schedule was converted into four event rows.
2. The extraction schema/prompt described `is_free` too weakly. It did not explicitly say that missing price is unknown rather than free, or that ticket links/status/sale wording should prevent free classification unless the source explicitly states free entry.
3. Duplicate matching depended on deterministic title/location similarity after extraction. When Gemma produced different titles or prose-fragment venues, same real events could survive as multiple cards. The dedup improvement must remain LLM-first or narrowly scoped to already-grounded anchors.

## Contributing Factors

- Runtime file logs were disabled, so investigation relied on production SQLite rows and Fly health instead of app-side run logs.
- Existing tests pinned several Gemma 4 prompt contracts but did not include the free/paid or schedule-rescue work-hours contract.
- Public rows from the 2026-04-30 batch needed data cleanup even after prompt fixes because the scheduled import had already written them.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring extraction prompts/schemas.
- Changing `schedule_like` / schedule-rescue prompts.
- Changing Smart Update duplicate matching, venue extraction, ticket/free handling, or public inventory rendering of free markers.
- Investigating production rows where `is_free=1` but source text has ticket sale/status/link and no explicit free-entry evidence.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `docs/llm/request-guide.md`
- `docs/llm/prompts.md`
- `tests/test_tg_monitor_gemma4_contract.py`
- `smart_event_update.py` duplicate and ticket/free merge surfaces
- production `event` and `event_source` rows for reported 2026-04-30 cards

### Mandatory checks before closure or deploy

- LLM-first policy/docs explicitly include event semantics beyond prose fields: `is_free`, ticket status, work-hours/non-event, venue/title meaning, duplicate/match decisions.
- Telegram Monitoring Gemma schema/prompt says:
  - `is_free=true` only with explicit free-attendance evidence;
  - missing price is unknown, not free;
  - ticket links/status/sale or paid venue admission are not free unless the source explicitly says free entry;
  - schedule-rescue prompt returns `[]` for institution work-hours/holiday-opening notices.
- Unit tests:
  - `pytest -q tests/test_tg_monitor_gemma4_contract.py`
- Prompt run/eval against reported shapes:
  - `@kaliningradlibrary/2219` work schedule returns `[]`;
  - paid/ticket-link examples do not return `is_free=true`;
  - explicit `Вход свободный` positive control can still return `is_free=true`.
- Production remediation evidence:
  - bogus work-hours rows are no longer active/public;
  - reported false-free rows are corrected or explicitly documented as still free with source evidence;
  - duplicate cards are merged/deactivated with source links preserved on the kept event.
- Release governance:
  - fix starts from `origin/main`;
  - deployed SHA is reachable from `origin/main` before closure;
  - Fly health remains green after deploy and remediation.

### Required evidence

- Test output and prompt-eval artifact.
- Deployed SHA and deploy path, if deployed.
- Production before/after row evidence for the reported events.
- Confirmation that no production-meaningful fix remains only in a side branch.

## Immediate Mitigation

- Investigation was isolated in linked worktree `hotfix/INC-2026-04-30-event-quality-regressions` from current `origin/main`.
- Production health and runtime-log evidence were collected before code changes.

## Corrective Actions

- Strengthened LLM-first policy docs so free/paid, non-event/work-hours, venue/title semantics, and duplicate/match decisions are explicitly LLM-owned.
- Strengthened Telegram Monitoring Gemma schema and prompts for the free/ticket contract and schedule-rescue work-hours exclusion.
- Added regression tests that pin those prompt contracts.
- Ran a production-key Gemma 4 prompt eval on the reported shapes:
  - library work-hours notice returned `[]`;
  - standup, museum-night, and wine-tasting paid/unknown-price controls did not return `is_free=true`;
  - explicit `Вход свободный` positive control returned `is_free=true`.
- Pending: deploy and clean up already-imported production rows.
- Deployed the prompt/doc/test fix from `origin/main` and remediated production rows:
  - work-hours cards `4398`-`4401` set `lifecycle_status=cancelled`, `silent=1`;
  - duplicate `4396` linked into kept event `4350`, and duplicate `4344` linked into kept event `4342`;
  - duplicate `event_source` rows were preserved on the kept events and removed from cancelled duplicate rows;
  - false-free controls `4347`, `4349`, `4350`, `4351`, `4379`, `4392`, `4397`, `4407` set `is_free=0`;
  - explicit free positive control `4367` remained `is_free=1`.
- Individual Telegraph event pages rebuilt through `joboutbox`.
- Follow-up production future-free audit/remediation:
  - raw audit artifact: `artifacts/codex/INC-2026-04-30-event-quality-regressions/future_free_audit_20260430T124605Z.raw`;
  - summary artifact: `artifacts/codex/INC-2026-04-30-event-quality-regressions/future_free_audit_20260430T124605Z.summary.json`;
  - 120 active future public rows were marked `is_free=1` before the follow-up cleanup;
  - 69 rows without explicit free-attendance evidence had `is_free` cleared to `0`;
  - after cleanup, 51 active future public rows remained `is_free=1`: 38 explicit-free rows and 13 explicit-free-with-registration/link rows;
  - before/after remediation artifact: `artifacts/codex/INC-2026-04-30-event-quality-regressions/inc_future_free_remediation_20260430.json`.
- Residual blocker: full May 2026 month-page rebuild hits pre-existing `CONTENT_TOO_BIG` even in force/split path. The production DB and active-list filters are corrected, but the stale month aggregate page needs a separate page-splitting fix before this incident can be marked `closed`.

## Follow-up Actions

- [x] Run prompt eval with production-equivalent `GOOGLE_API_KEY3` if available.
- [x] Deploy prompt/doc/test fix from a clean branch and make it reachable from `origin/main`.
- [x] Remediate production rows and preserve source evidence.
- [ ] Fix/repair May 2026 month-page force split path so the aggregate page rebuild no longer fails with `CONTENT_TOO_BIG`.
- [ ] Clean up merged hotfix branches/worktrees after closure.

## Release And Closure Evidence

- deployed SHA: `e0c39ea5c8fd21bd6e9584ff8b595afbd8b30aa8` (`origin/main`)
- deploy path: `flyctl deploy --app events-bot-new-wngqia --remote-only`
- deployed image: `events-bot-new-wngqia:deployment-01KQF57DQPCMDQPY8XAQN50Y89`
- regression checks:
  - `artifacts/codex/INC-2026-04-30-event-quality-regressions/.venv/bin/python -m pytest -q tests/test_tg_monitor_gemma4_contract.py` (`23 passed`)
  - Fly production `GOOGLE_API_KEY3` prompt eval artifact: `artifacts/codex/INC-2026-04-30-event-quality-regressions/prompt_eval_gemma4_free_workhours_20260430_passed.json` (`5/5` cases passed)
- post-deploy verification:
  - Fly status: machine `48e42d5b714228`, version `1028`, checks `1 total, 1 passing`.
  - `/healthz`: `ok=true`, `ready=true`, `db=ok`, scheduler/tasks `ok`, `issues=[]`.
  - Production remediation artifact: `artifacts/codex/INC-2026-04-30-event-quality-regressions/inc_event_quality_remediation_20260430.json`.
  - Confirmed production rows after remediation: work-hours and duplicate rows are cancelled/silent; false-free rows are no longer `is_free=1`; explicit `Вход свободный` control remains free.
  - Full future-free audit/remediation artifacts:
    - `artifacts/codex/INC-2026-04-30-event-quality-regressions/future_free_audit_20260430T124605Z.summary.json`;
    - `artifacts/codex/INC-2026-04-30-event-quality-regressions/inc_future_free_remediation_20260430.json`;
    - `artifacts/codex/INC-2026-04-30-event-quality-regressions/future_free_audit_after_20260430T124605Z.summary.json`.
  - Future-free post-check: before cleanup `120` future rows had `is_free=1`; after cleanup `51` remained, with `0` rows in the `paid signal/no free evidence` or `no free evidence` risk classes.
  - Post-cleanup `/healthz`: `ok=true`, `ready=true`, `db=ok`, scheduler/tasks `ok`, `issues=[]`; Fly status remained machine `48e42d5b714228`, version `1028`, checks `1 total, 1 passing`.
  - Month-page caveat: outbox `month_pages:2026-05` and manual force rebuild both hit `CONTENT_TOO_BIG`; stuck running month jobs were marked `error` after interruption / follow-up cleanup so they no longer block the worker. Individual Telegraph pages and week/weekend pages from the future-free cleanup were processed; aggregate month page splitting remains the open blocker.

## Prevention

- This incident is the mandatory regression contract for future Telegram Monitoring prompt changes involving free/paid classification, work-hours notices, and duplicate public cards.
