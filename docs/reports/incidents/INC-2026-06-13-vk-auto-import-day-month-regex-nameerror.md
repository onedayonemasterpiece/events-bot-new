# INC-2026-06-13-vk-auto-import-day-month-regex-nameerror VK auto-import day/month regex NameError

Status: closed
Severity: sev1
Service: VK auto-import draft extraction
Opened: 2026-06-13
Closed: 2026-06-13
Owners: Codex
Related incidents: `INC-2026-06-12-tg-monitoring-deploy-crash-no-watchdog`
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-13 production `/vk_auto_import` failed during draft extraction with
`NameError: name '_DAY_MONTH_NUM_RE' is not defined`. The affected helper lived
in `vk_intake._source_text_has_absolute_date_anchor`, so valid VK posts that
reached Gemma draft parsing could fail after spending LLM/OCR time.

## User / Business Impact

- Operator-facing bot messages reported `ошибка извлечения событий (drafts)`.
- At least two visible rows failed in the user-triggered/scheduled run:
  `https://vk.com/wall-149955604_23371` and
  `https://vk.com/wall-212760444_5352`.
- The run did not reliably import the selected batch; failed rows required
  catch-up after the code fix.

## Detection

- Reported from production bot messages at 2026-06-13 10:48 and 10:50
  Europe/Kaliningrad.
- Runtime file mirror was enabled in production and contained the traceback.

## Timeline

- 2026-06-13 08:49:27 UTC — `vk_auto_queue` logged
  `build_event_drafts failed inbox_id=8605 source=https://vk.com/wall-149955604_23371`.
- 2026-06-13 08:49:28 UTC — row `8605` finished with `drafts=0 ok=0`.
- 2026-06-13 08:50:59 UTC — `vk_auto_queue` logged
  `build_event_drafts failed inbox_id=8597 source=https://vk.com/wall-212760444_5352`.
- 2026-06-13 08:50:59 UTC — row `8597` finished with `drafts=0 ok=0`.
- 2026-06-13 09:03 UTC — hotfix SHA `9187dc60` deployed to Fly image
  `deployment-01KV03GRG4KBAQN7WHVABCA2PX`.
- 2026-06-13 09:06-09:13 UTC — targeted production catch-up processed rows
  `8605` and `8597` through the same VK auto-import row processor.
- 2026-06-13 09:15 UTC — DB verification showed both rows `imported` with
  locks cleared.

## Root Cause

1. `vk_intake._source_text_has_absolute_date_anchor` referenced
   `_DAY_MONTH_NUM_RE` and `_DAY_MONTH_WORD_RE`.
2. These regex constants existed in `smart_event_update.py`, but not in
   `vk_intake.py`.
3. The missing constants were not covered by a direct unit test for the helper.

## Contributing Factors

- The failing helper runs after the expensive LLM/OCR portion, so the failure
  looked like extraction failure instead of an immediate import-start crash.
- Operator-visible auto-import output included only the exception message, not
  the owning helper name.

## Automation Contract

### Treat as regression guard when

- changing `vk_intake.py` date-anchor detection;
- changing VK auto-import draft extraction or post-LLM draft cleanup;
- changing scheduled/manual `/vk_auto_import` handling of `failed` rows.

### Affected surfaces

- `vk_intake.py`
- `vk_auto_queue.py`
- production `/vk_auto_import`
- runtime logs and `vk_inbox` row statuses

### Mandatory checks before closure or deploy

- `python3 -m py_compile vk_intake.py vk_auto_queue.py`
- targeted test for `_source_text_has_absolute_date_anchor` numeric and text
  month formats;
- production health check after deploy;
- production log check that fresh rerun no longer emits `_DAY_MONTH_NUM_RE`;
- catch-up the affected `vk_inbox` rows after deploy.

### Required evidence

- deployed SHA reachable from `origin/main`;
- Fly deployment image;
- post-deploy `/healthz`;
- production catch-up result for failed rows `8605` and `8597`.

## Immediate Mitigation

- Add the missing day/month regex constants to `vk_intake.py`.
- Reset the failed production rows caused by this NameError and rerun
  `/vk_auto_import` catch-up after deploy.

## Corrective Actions

- Add direct regression coverage for numeric and text month date anchors.
- Keep the regex constants local to `vk_intake.py` instead of relying on
  `smart_event_update.py` internals.

## Follow-up Actions

- [ ] Consider exposing exception class/helper in operator-facing draft failure
  details while keeping messages compact.

## Release And Closure Evidence

- deployed SHA: `9187dc60` (`origin/main`)
- deploy path: manual `fly deploy --remote-only -a events-bot-new-wngqia`
- Fly image: `registry.fly.io/events-bot-new-wngqia:deployment-01KV03GRG4KBAQN7WHVABCA2PX`
- regression checks:
  - `python3 -m py_compile vk_intake.py vk_auto_queue.py tests/test_vk_intake_keywords_dates.py`
  - production image smoke:
    `_source_text_has_absolute_date_anchor("15.06") == True`,
    `_source_text_has_absolute_date_anchor("15 июня") == True`,
    `_source_text_has_absolute_date_anchor("завтра") == False`
  - local `pytest` was not available in the hotfix worktree
    (`No module named pytest`), so the targeted test is committed but was not
    executed locally.
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`;
  - runtime logs after deploy showed `smart_update.start` and
    `persist_event_and_pages` for both failed URLs, with no new
    `_DAY_MONTH_NUM_RE` NameError;
  - `vk_inbox.id=8605` is `imported`, `imported_event_id=5991`, lock cleared;
  - `vk_inbox.id=8597` is `imported`, `imported_event_id=5370`, lock cleared;
  - `vk_inbox_import_event` maps `8605 -> 5991` and `8597 -> 5370`;
  - event `5991` has managed VK URL `https://vk.com/wall-231920894_3248`;
  - event `5370` was updated; `telegraph_build`, `ics_publish`,
    `tg_ics_post`, and `vk_sync` are `done`, while `tg_event_publish` remains
    scheduled.

## Prevention

- `vk_intake` date-anchor helpers now have direct regression coverage so missing
  regex definitions fail before production auto-import.
