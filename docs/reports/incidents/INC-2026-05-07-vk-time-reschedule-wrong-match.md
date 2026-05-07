# INC-2026-05-07-vk-time-reschedule-wrong-match VK time reschedule matched wrong old event

Status: open
Severity: sev1
Service: VK auto-import / Smart Update
Opened: 2026-05-07
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-05-event-quality-regression`, `INC-2026-05-01-future-event-quality-audit`, `INC-2026-04-20-club-znakomstv-duplicate-event-cards`
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/incident-management.md`

## Summary

Manual `/vk_auto_import 1` on 2026-05-07 16:02 Europe/Kaliningrad processed `https://vk.com/wall-222857709_1116`, a live notice that the `8 мая` wine evening start time moved to `19:30`. VK auto-import classified it as cancellation/postponement and marked unrelated old `event_id=2029` inactive.

## User / Business Impact

- Operator saw Smart Update details for `event_id=2029`, an unrelated old blank-title event dated `2026-01-01`.
- The real VK post was not imported as a normal event/update.
- The wrong event gained a `vk_cancel` source and `lifecycle_status=postponed`.

## Detection

- Detected immediately by operator report in Telegram after `/vk_auto_import 1`.
- Runtime file mirror was checked: `ENABLE_RUNTIME_FILE_LOGGING=0`; `/data/runtime_logs` exists but has no active logs. Evidence came from Fly logs and production SQLite.

## Timeline

- 2026-05-07 14:02 UTC — `/vk_auto_import 1` processed `wall-222857709_1116`; bot reported `event_id=2029` as postponed.
- 2026-05-07 14:08 UTC — production DB confirmed `event_id=2029` had `lifecycle_status=postponed` and new `event_source.source_type=vk_cancel` from the VK post.
- 2026-05-07 14:12 UTC — root cause isolated in `vk_auto_queue.py` cancellation/date helper.

## Root Cause

1. `_looks_like_cancellation_notice()` treated `время начала ... перенесено на 19.30` as a cancellation/postponement notice, although the event still happens and should go through the normal LLM-first VK import path.
2. `_parse_ru_date_from_text()` first matched `19.30` as `dd.mm`, failed on invalid month `30`, and returned `None` before scanning `8 мая`.
3. The same helper imported `_RU_MONTHS_GENITIVE` from `smart_event_update`; that import is unsafe for this low-level queue helper and can fail or side-effect during runtime import, leaving month parsing unavailable.
4. With no parsed date and no title hint, `_cancel_matching_event_from_notice()` allowed weak matching against all active events and selected unrelated `event_id=2029`.

## Contributing Factors

- Cancellation shortcut bypassed the normal LLM-first Smart Update path.
- Matcher allowed deactivation without a date or title anchor.
- Runtime file mirror is intentionally disabled after disk-pressure incidents, so evidence relied on Fly log buffer and DB state.

## Automation Contract

### Treat as regression guard when

- changing `vk_auto_queue.py` cancellation/postponement detection;
- changing VK date/time parsing helpers;
- changing Smart Update/VK event matching or lifecycle status updates;
- changing VK auto-import replay behavior for transfer/time-change posts.

### Affected surfaces

- `vk_auto_queue.py` cancellation shortcut;
- VK auto-import queue row status and `event_source` attachment;
- `event.lifecycle_status`;
- operator Smart Update report.

### Mandatory checks before closure or deploy

- Unit tests proving `8 мая ... перенесено на 19.30` parses as `2026-05-08` and does not enter cancellation shortcut.
- Regression test proving cancellation notices with explicit `не состоится` still mark the matching event inactive.
- Replay offending source artifact through VK auto-import boundary + Smart Update on a shadow/prod-snapshot DB.
- Production repair: remove wrong `vk_cancel` source/fact from `event_id=2029`, restore lifecycle status, and reset or reprocess offending `vk_inbox` row.
- Post-deploy `/healthz` and env/release SHA verification.

### Required evidence

- deployed SHA reachable from `origin/main`;
- targeted pytest output;
- replay artifact/query output for `wall-222857709_1116`;
- production DB repair query output;
- post-deploy Fly health/status.

## Immediate Mitigation

- Hotfix in progress: prevent time-reschedule notices from entering cancellation shortcut and require date or title anchor before deactivating any event.

## Corrective Actions

- Add local Russian month map to `vk_auto_queue.py` instead of importing Smart Update.
- Continue scanning month-name dates after invalid `dd.mm` candidates such as `19.30`.
- Treat explicit time-reschedule notices as normal import/update posts.
- Reject cancellation matching when both date and title hints are absent.

## Follow-up Actions

- [ ] Add a production-like replay artifact after the hotfix deploy.
- [ ] Decide whether to temporarily enable runtime file mirror during VK auto-import canary windows, with disk budget and retention check.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Keep transfer/time-change notices on the LLM-first import path unless the text explicitly says the event will not happen.
- Cancellation shortcut must remain conservative and must not deactivate events without date/title anchors.
