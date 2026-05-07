# INC-2026-05-01 Daily Location Drift In Announcement

Status: closed
Severity: sev1
Service: Telegram daily announcement / Telegram Monitoring import / Smart Update
Opened: 2026-05-01
Closed: 2026-05-01
Owners: Codex / event ingestion owner
Related incidents: `INC-2026-04-26-daily-location-fragments`, `INC-2026-04-29-bar-bastion-city-jazz-location`, `INC-2026-04-30-tg-monitoring-event-quality-regressions`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `docs/llm/request-guide.md`

## Summary

The 2026-05-01 daily announcement exposed repeated event-quality regressions: `location_name` contained prose/schedule fragments instead of venues, one theatre event was duplicated because an unsupported Telegram time became a hard anchor, VK source defaults again placed a standup event at `Калининград Сити Джаз Клуб`, and the Зеленоградск `Камамбер` venue was missing from the reference layer.

## User / Business Impact

- Subscribers saw doubtful duplicate cards and nonsensical public location lines in the daily announcement.
- Operators could not tell which of two `Женитьба` rows was correct from the public text.
- Venue trust regressed on a surface that had already had location-fragment incidents.

## Detection

- Detected by manual review of the 2026-05-01 daily announcement.
- Runtime file mirror was checked on Fly: `ENABLE_RUNTIME_FILE_LOGGING=0`, `/data/runtime_logs` exists, no active mirrored logs. Fallback evidence comes from production DB rows and Fly status.

## Timeline

- 2026-05-01 UTC: user reported seven daily-announcement defects.
- 2026-05-01 UTC: production DB evidence confirmed affected rows and source posts.
- 2026-05-01 UTC: linked hotfix worktree created from `origin/main` because the primary worktree was dirty/out-of-sync.
- 2026-05-01 UTC: fix implementation started with LLM-first venue review, time-default grounding, VK source default repair, and reference additions.
- 2026-05-01 07:17 UTC: deployed `c9266892` to Fly app `events-bot-new-wngqia`, machine version `1029`.
- 2026-05-01 07:27 UTC: repaired production rows with backup tables `incident_event_quality_backup_20260501` / `incident_event_source_backup_20260501`.
- 2026-05-01 07:29 UTC: verified daily preview for 2026-05-01 with `bad_hits=[]`.
- 2026-05-01 07:29 UTC: sent corrected daily catch-up to `@kenigevents` and `@keniggpt`, 12 Telegram posts each, `record=False`.

## Root Cause

1. Telegram Monitoring Gemma 4 could place nearby prose, schedule commentary, or section labels into `location_name`; the existing prompt contract was not strong enough after the main extraction pass.
2. Server-side candidate import treated some unsupported extracted times as hard anchors, so same-title/same-venue rows could survive as separate event cards.
3. Known VK sources with stale/wrong default location (`Калининград Сити Джаз Клуб`) were not repaired at DB init for the affected standup/bar sources.
4. The location reference did not include `Сырный магазин Камамбер`, `Бар ЛЕС`, and the Gusev railway station alias set.

## Contributing Factors

- Previous guardrails were too easy to interpret as a regex cleanup problem. For arbitrary text fragments, the canonical fix is LLM-first prompt/schema or a staged Gemma review pass.
- `location_name` smell checks need to be treated as their own regression class, not covered by generic JSON-shape validation.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring extraction prompts/schema, producer repair stages, or source default-location context.
- Changing `source_parsing/telegram/handlers.py` candidate grounding, `time_is_default`, or reference-location normalization.
- Changing Smart Update duplicate matching around weak/default times or venue anchors.
- Changing VK source default-location seeding/repair.
- Editing `docs/reference/locations.md` or `docs/reference/location-aliases.md` for Kaliningrad-region venues.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `source_parsing/telegram/handlers.py`
- `smart_event_update.py`
- `db.py`
- `docs/reference/locations.md`
- `docs/reference/location-aliases.md`
- `/daily` and Telegraph event pages
- Fly production DB and scheduled daily catch-up

### Mandatory checks before closure or deploy

- `tests/test_tg_monitor_gemma4_contract.py`
- `tests/test_tg_candidate_location_grounding.py`
- `tests/test_smart_event_update_duplicate_guards.py`
- `tests/test_smart_event_update_location_aliases.py`
- `tests/test_vk_default_time.py`
- `python -m py_compile kaggle/TelegramMonitor/telegram_monitor.py source_parsing/telegram/handlers.py db.py smart_event_update.py`
- Production health before/after deploy: Fly status plus `/healthz`.
- Post-deploy daily preview/catch-up must show no May 1 location fragments reported in this incident.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test output for the mandatory checks.
- Production DB repair evidence for affected rows.
- Daily catch-up/preview evidence for 2026-05-01.

## Immediate Mitigation

- Repaired affected production rows, cancelled duplicate public cards, requeued Telegraph/ICS/month/week rebuild jobs, and sent a same-day corrected daily catch-up.

## Corrective Actions

- Add a staged Gemma location-review pass that repairs suspicious `location_name/location_address/city` fields from original message/OCR/source/default-location context. The deterministic part only detects broad bad shapes and never chooses the semantic replacement.
- Mark unsupported extracted Telegram times as `time_is_default` so Smart Update treats them as weak anchors.
- Repair known VK source location defaults for affected source groups.
- Add missing venue references and aliases.

## Follow-up Actions

- [ ] Add the May 1 live posts to a compact Telegram Monitoring Gemma 4 eval pack when production evidence is stable.
- [ ] Consider an operator report that lists newly imported active events whose venue field required Gemma location-review.

## Release And Closure Evidence

- deployed SHA: `c9266892`, reachable from `origin/main`.
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia` from clean linked worktree; image `deployment-01KQH6A59543PCQPC298XB8RBF`; machine `48e42d5b714228` version `1029`.
- regression checks: `46 passed` for `tests/test_tg_monitor_gemma4_contract.py`, `tests/test_tg_candidate_location_grounding.py`, `tests/test_smart_event_update_duplicate_guards.py`, `tests/test_smart_event_update_location_aliases.py`, `tests/test_vk_default_time.py`; `py_compile` passed for `kaggle/TelegramMonitor/telegram_monitor.py`, `source_parsing/telegram/handlers.py`, `db.py`, `smart_event_update.py`.
- production DB repair: backup tables contain `22` event rows and `32` event-source rows; duplicate rows `4288`, `4117`, `4152`, `4261`, `4199`, `4456`, `4465` were set non-active/silent; active affected rows now have grounded venues.
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, `db=ok`; Fly status showed `1 total, 1 passing`; daily preview returned `bad_hits=[]`; corrected catch-up sent `12` posts to each daily channel.

## Prevention

- This incident locks the strategy: arbitrary bad `location_name` text is handled by LLM-first prompt/schema or staged Gemma review. Deterministic code may enforce syntax, grounding, reference normalization, and safe drops, but must not become a growing phrase dictionary.
