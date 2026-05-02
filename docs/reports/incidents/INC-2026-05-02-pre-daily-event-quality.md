# INC-2026-05-02 Pre-Daily Event Quality

Status: closed
Severity: sev2
Service: Telegram Monitoring / VK auto-import / Smart Event Update event quality
Opened: 2026-05-02
Closed: 2026-05-02
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-05-01-future-event-quality-audit`, `INC-2026-05-01-daily-location-drift`, `INC-2026-04-20-club-znakomstv-duplicate-event-cards`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/llm/prompts.md`, `docs/operations/runtime-logs.md`

## Summary

A fresh production audit before the 2026-05-02 daily announcement found active today/future event-quality regressions: duplicate public cards for the same real event, source/default venue drift into unrelated venues (`Бар Советов`, `Стендап клуб Локация`, `Понарт`), and placeholder/prose venue fields such as `location_address`.

This is a production incident because these rows were active and could appear in the imminent daily announcement, Telegraph event pages, month/day pages, VK daily output, and video-announcement pools.

## User / Business Impact

- Readers could see the same real event multiple times in one daily/future surface.
- Readers could be sent to a wrong venue/city/address.
- Operators had less than an hour before the daily slot to repair public data without rerunning daily.
- The same failure families had appeared in recent incidents, so closure required prompt/code prevention, not only data repair.

## Detection

- Detected by operator request on 2026-05-02 before the daily announcement window.
- Evidence came from live production SQLite `/data/db.sqlite`, manual source review of the candidate clusters, and follow-up production SQL verification.
- Runtime file mirror policy was checked via existing operations docs; production runtime logging mirror is disabled, so DB/source/Fly evidence is the primary evidence path.

## Timeline

- 2026-05-02 05:12 UTC — production future/today event audit started against `/data/db.sqlite`; 432 active future/today rows scanned, 31 fresh rows since 2026-05-01 reviewed.
- 2026-05-02 05:20 UTC — confirmed duplicate and wrong-venue clusters for May 2, May 3, and May 7 rows.
- 2026-05-02 05:24 UTC — production data repair applied with row-level backup tables `incident_pre_daily_quality_event_backup_20260502` and `incident_pre_daily_quality_side_backup_20260502`.
- 2026-05-02 05:24-05:25 UTC — affected Telegraph event pages rebuilt for 13 survivor events; daily announcement was not rerun.
- 2026-05-02 05:27 UTC — production verification: `PRAGMA quick_check=ok`, no temporary repair script remained on the machine, no daily joboutbox tasks were created, and all duplicate rows were inactive `merged`.
- 2026-05-02 UTC — prompt/code corrective work added event-local venue grounding, literal field-placeholder rejection, and canonical title guidance for Telegram/VK extraction.
- 2026-05-02 05:35 UTC — corrective SHA `dbde95636794de547fde6bea3f5d8fed4e6ea9c0` deployed to Fly machine version `1032`.

## Root Cause

1. LLM extraction did not explicitly require event-local venue grounding for multi-event, digest, and repost posts, so source/default venue hints could override the venue actually named in the event block.
2. The prompt did not explicitly ban literal schema field names like `location_address` as output values; a placeholder could pass downstream as a public address.
3. Title guidance did not cover in-character promo copy where a ticket URL/page or clear program title provides the canonical attendee-facing title.
4. Duplicate matching still needed production cleanup for older active rows created by pre-existing extraction drift.

## Contributing Factors

- Several source posts were reposts or schedule/digest posts where one source's default venue was not the event venue.
- Some sources use `doors/start` wording or promotional copy that changes titles across reposts.
- Recent data repairs had not yet fully converted the recurring quality lessons into prompt-level regression checks.

## Automation Contract

### Treat as regression guard when

- changing Telegram Monitoring extraction prompts/schema, schedule rescue, title review, or venue review;
- changing VK auto-import draft extraction prompts or post-LLM field cleanup;
- changing Smart Update duplicate matching for same-date/same-venue/source repost cases;
- running pre-daily production audits or repairing future active event rows.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `vk_intake.py`
- `docs/llm/prompts.md`
- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- production SQLite `event`, `event_source`, `event_source_fact`, `eventposter`, `vk_inbox`, `joboutbox`
- Telegraph event pages, `/daily`, VK daily, month/day pages, video-announcement candidate pools

### Mandatory checks before closure or deploy

- Re-run focused tests for Telegram prompt contract and VK placeholder cleanup.
- Verify production survivor rows are active, duplicate rows are `merged`, and `PRAGMA quick_check=ok`.
- Verify affected Telegraph event pages were rebuilt.
- Verify no daily announcement rerun or daily joboutbox task was created by the repair.
- If code is deployed, confirm deployed SHA is reachable from `origin/main`.

### Required evidence

- Production SQL before/after evidence for touched event IDs.
- Source confirmation for each repaired cluster.
- Test output for changed prompt/code guards.
- Release/deploy evidence if code changes are deployed.

## Immediate Mitigation

- Production data repair completed for active today/future rows only.
- Confirmed duplicate clusters were merged into survivor rows:
  - keep `4329`, merge `3911`;
  - keep `4436`, merge `4466`;
  - keep `3675`, merge `4289`, `4402`;
  - keep `4096`, merge `4184`;
  - keep `4462`, merge `2635`;
  - keep `3450`, merge `3445`, `4353`;
  - keep `4444`, merge `4487`, `4272`, `4359`;
  - keep `3676`, merge `4290`;
  - keep `4351`, merge `4480`;
  - keep `4035`, merge `4121`;
  - keep `4384`, merge `4262`, `4295`.
- Corrected confirmed fields for `4471` (`Библиотека им. Лунина`) and `4488` (`Полесск`).
- Rebuilt affected Telegraph event pages for all survivor rows.
- Daily announcement was intentionally not updated.

## Corrective Actions

- Done: Telegram Monitoring prompt/schema now bans field-name placeholders and requires event-local venue grounding for multi-event/digest/repost posts.
- Done: Telegram schedule rescue prompt now states that event-local venue/address beats shared source context/default.
- Done: VK auto-import draft prompt now carries the same event-local venue rule and canonical-title rule.
- Done: VK post-LLM cleanup drops literal field-name placeholders in location fields.
- Done: docs and changelog updated with the new regression contract.

## Follow-up Actions

- [ ] Add a reusable pre-daily production quality audit command that reports future duplicate/location candidates without ad hoc SQL.
- [ ] Add production-equivalent prompt eval cases for the confirmed `Бар Советов`, `Понарт`, `Стендап клуб Локация`, and `location_address` placeholder regressions.

## Release And Closure Evidence

- deployed SHA: `dbde95636794de547fde6bea3f5d8fed4e6ea9c0` (reachable from `origin/main`)
- deploy path: `flyctl deploy --remote-only --app events-bot-new-wngqia`
- regression checks:
  - production repair verification: `PRAGMA quick_check=ok`; 29 event rows and 11 side rows backed up; duplicate IDs listed above are `lifecycle_status='merged'`; 13 survivor rows are active with corrected venues.
  - Telegraph rebuild: survivor IDs `3450`, `3675`, `3676`, `4035`, `4096`, `4329`, `4351`, `4384`, `4436`, `4444`, `4462`, `4471`, `4488` returned `TELEGRAPH_OK`.
  - daily announcement not rerun; `joboutbox` query for daily tasks returned no rows.
  - temporary `/tmp/prod_repair_and_rebuild_20260502.py` removed from production machine.
  - local syntax check: `python3 -m py_compile vk_intake.py kaggle/TelegramMonitor/telegram_monitor.py`.
  - targeted regression tests: `24 passed` for `tests/test_tg_monitor_gemma4_contract.py` and `tests/test_vk_default_time.py::test_vk_llm_text_field_cleaner_drops_location_placeholders`.
  - Fly deploy evidence: app `events-bot-new-wngqia`, machine `48e42d5b714228`, version `1032`, image `deployment-01KQKJV05FTD9HQ6XGG5HCXEEG`, health check passing.
  - post-deploy `/healthz`: `ok=true`, `ready=true`, `db=ok`, scheduler/tasks `ok`, no issues.
  - post-deploy production DB verification: `quick_check=ok`, `merged_bad=0`, `survivor_bad=0`, `daily_joboutbox_count=0`.
- post-deploy verification: passed

## Prevention

Future extractors must treat venue grounding as event-local evidence, not source-global defaulting. Literal schema field names are syntax-only placeholders and must be empty strings, and canonical ticket/program titles must beat in-character promotional phrasing when they identify the same attendee-facing event.
