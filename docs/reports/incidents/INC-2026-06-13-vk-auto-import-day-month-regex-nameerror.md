# INC-2026-06-13-vk-auto-import-day-month-regex-nameerror VK auto-import day/month regex NameError

Status: open
Severity: sev1
Service: VK auto-import draft extraction
Opened: 2026-06-13
Closed: —
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

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- `vk_intake` date-anchor helpers now have direct regression coverage so missing
  regex definitions fail before production auto-import.
