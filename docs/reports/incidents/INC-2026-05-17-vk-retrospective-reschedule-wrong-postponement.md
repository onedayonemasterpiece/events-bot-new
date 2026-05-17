# INC-2026-05-17 VK Retrospective Reschedule Wrong Postponement

Status: mitigated
Severity: sev2
Service: VK auto-import / event lifecycle matching
Opened: 2026-05-17
Closed: —
Owners: bot/runtime
Related incidents: `INC-2026-05-07-vk-time-reschedule-wrong-match`
Related docs: `docs/operations/runtime-logs.md`, `docs/features/promo-campaigns/README.md`

## Summary

Production event `4617` (`Спектакль 8 ЖЕНЩИН`, 2026-05-22 19:00) was marked
`postponed` by a wrong `vk_cancel` source. The source was not about the play at
all: it was a new lecture announcement, `Прерафаэлиты: братство, приручившее
вечность`, whose text said the lecture was a replacement for a missed April
meeting. The cancellation shortcut treated the retrospective word `перенос` as
a postponement notice and matched by shared date/time.

## User / Business Impact

- The promoted `8 женщин` event was hidden from active future-event selection
  immediately before a requested CherryFlash promo campaign.
- A real new VK lecture post could also have been diverted away from the normal
  LLM-first import path.
- The same weak lifecycle shortcut could silently deactivate other unrelated
  events that share a date/time with a retrospective reschedule announcement.

## Detection

During the 2026-05-17 promo setup and CherryFlash investigation, production DB
inspection showed event `4617` in `lifecycle_status='postponed'` with
`event_source.source_type='vk_cancel'` from `https://vk.com/wall-190663987_8758`.

## Timeline

- 2026-05-17 07:00 UTC: operator requested a promo campaign for `8 женщин` and
  CherryFlash eco selection investigation; no deploy requested.
- 2026-05-17 07:30 UTC: production DB inspection found event `4617` was
  `postponed`.
- 2026-05-17 07:40 UTC: source audit found the `vk_cancel` source text was an
  unrelated `Прерафаэлиты` lecture announcement with retrospective wording
  `Эта лекция - перенос несостоявшейся встречи в апреле`.
- 2026-05-17 07:50 UTC: production data was repaired: event `4617` restored to
  `active`, the bad `vk_cancel` event source/fact/link were removed, and the
  offending `vk_inbox` row was marked `skipped` so current production code does
  not reapply the same bad match before a later deploy.

## Root Cause

1. `_looks_like_cancellation_notice()` treated broad `перенос*` and
   `несостоявш*` wording as enough to enter the cancellation/postponement
   shortcut.
2. The shortcut then used date/time anchors from the current lecture
   announcement, not a source-grounded title/venue match to the event being
   changed.
3. There was no guard for retrospective context like "this current lecture is a
   replacement for an old missed meeting", which should remain in the normal
   LLM-first import path.

## Contributing Factors

- The 2026-05-07 time-reschedule guard covered explicit start-time changes, but
  not retrospective "new event replaces an old missed meeting" wording.
- Shared date/time with another active event was enough to choose a candidate
  when no title overlap existed.

## Automation Contract

### Treat as regression guard when

- Changing `vk_auto_queue.py` cancellation/postponement detection.
- Changing VK lifecycle matching, `vk_cancel` source creation, or
  `vk_inbox.status` handling for cancellation shortcuts.

### Affected surfaces

- `vk_auto_queue.py`
- `tests/test_vk_auto_queue_import.py`
- production `event`, `event_source`, `event_source_fact`,
  `vk_inbox`, `vk_inbox_import_event`

### Mandatory checks before closure or deploy

- `py_compile` for `vk_auto_queue.py`.
- `tests/test_vk_auto_queue_import.py` must prove explicit time changes and
  retrospective replacement announcements stay on the normal LLM-first path.
- Production check after deploy that the repaired event `4617` remains `active`
  and no new `vk_cancel` source for `wall-190663987_8758` is attached to it.

### Required evidence

- deployed SHA:
- tests:
- production DB verification:
- fix reachable from `origin/main`:

## Immediate Mitigation

- Restored event `4617` to `active`.
- Removed the bad `vk_cancel` source/fact/link from event `4617`.
- Marked `vk_inbox.id=6935` as `skipped` with `imported_event_id=NULL` to avoid
  reprocessing by the currently deployed code before the code fix is released.

## Corrective Actions

- Add a narrow guard for retrospective replacement wording such as
  `Эта лекция - перенос несостоявшейся встречи в апреле`.
- Keep those posts on the normal LLM-first VK import path instead of the
  cancellation/postponement shortcut.

## Follow-up Actions

- [ ] Consider requiring title or venue overlap, not date/time alone, before a
  `перенос` shortcut can mark an unrelated event `postponed`.

## Release And Closure Evidence

- deployed SHA: `bba67b5aa78c4bd6c516348e4e5b4cfd26cd9c35`
- deploy path: clean linked worktree `hotfix/2026-05-17-cherryflash-eco-promo`, pushed to `origin/main`, deployed with `flyctl deploy -a events-bot-new-wngqia`
- regression checks:
  - `/home/dev/projects/events-bot-new/.venv/bin/python -m py_compile promo.py video_announce/popular_review.py vk_auto_queue.py source_parsing/telegram/handlers.py location_reference.py kaggle/TelegramMonitor/telegram_monitor.py`
  - `/home/dev/projects/events-bot-new/.venv/bin/pytest tests/test_promo.py tests/test_video_announce_popular_review.py tests/test_vk_auto_queue_import.py tests/test_tg_candidate_location_grounding.py tests/test_tg_monitor_gemma4_contract.py -q` -> `91 passed`
- post-deploy verification:
  - Fly image `events-bot-new-wngqia:deployment-01KRTH9RXB7P1NV3X86S4CDWAT`
  - Fly machine `48e42d5b714228`, version `1100`, state `started`, checks `1 passing`
  - `/healthz` returned `ok=true`, `ready=true`, `db=ok`, scheduler/tasks ok, `issues=[]`
  - production event `4617` remains `active`; no `event_source` for `wall-190663987_8758` remains attached to it; `vk_inbox.id=6935` is `skipped` with `imported_event_id=NULL`.

## Prevention

The incident record is now a regression contract for VK lifecycle shortcuts:
retrospective "this is a replacement for an old missed event" context must not
deactivate another event.
