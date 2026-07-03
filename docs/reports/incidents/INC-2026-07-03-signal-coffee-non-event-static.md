# INC-2026-07-03 Signal coffee non-event static page

Status: mitigated
Severity: sev2
Service: Smart Update / static event pages / public event inventory
Opened: 2026-07-03
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-05-05-smart-update-gemma3-fallback-hallucination.md`, `INC-2026-05-17-future-event-quality-regressions.md`
Related docs: `docs/features/event-issue-reporting/README.md`, `docs/operations/incident-management.md`, `docs/llm/request-guide.md`

## Summary

Static preview page for event `6045` showed a public event card dated `2027-03-01` for a `Сигнал` source post about coffee beans and Charlie Mingus. Source evidence contains no concrete future event/date/programme; the generated description was generic and unsupported. This is a production-quality incident because the canonical event row can flow into static pages, Telegraph, Telegram/VK surfaces, and future-event recommendations.

## User / Business Impact

- Users could see a fabricated future event date and generic event copy.
- Static generation would keep publishing the page until canonical DB content is repaired or removed.
- The incident repeats the future-date / unsupported writer-copy family and therefore requires LLM-first prevention, not deterministic semantic rewriting.

## Detection

- Detected from admin static-page issue report submitted through the new event issue UI.
- Report text: `Крайневероятно, что событие состоится 1 марта 2027; проверь источники, описание похоже вообще выдумка.`
- Production DB evidence: event `6045` sources `https://t.me/signalkld/11052` and `https://vk.com/wall-231920894_3468` are coffee/editorial text; no source-grounded `2027-03-01` event exists.

## Timeline

- 2026-06-15 23:09 UTC — source rows imported for `signalkld/11052` and VK mirror.
- 2026-07-03 08:56 UTC — static-page UI submitted issue report `0b935c6f-2a5f-40bb-a1eb-9b1ef71ce447`.
- 2026-07-03 09:00 UTC — ArtKodex created task `T-000060` in forum thread `https://t.me/c/4495320105/1188`.
- 2026-07-03 09:xx UTC — prevention patch routes ungrounded date/no-time Telegram/VK candidates to LLM eventness review and adds product/menu editorial non-event instruction.

## Root Cause

1. Smart Update allowed an auto-ingested Telegram/VK candidate with a date not grounded in source text or poster OCR to continue without LLM eventness confirmation.
2. The eventness review prompt did not explicitly call out product/menu/editorial posts with art/music associations as non-events when no concrete schedule/programme exists.
3. The writer path then produced generic programme/ticket copy unsupported by the source.

## Contributing Factors

- Source text contains an inviting phrase (`Приходите в Сигнал`), but it is about visiting a cafe for coffee, not attending a scheduled event.
- Existing non-event guards covered many promo/logistics classes but not this ungrounded-date, no-time product/editorial shape.

## Automation Contract

### Treat as regression guard when

- Changing Smart Update eventness routing for Telegram/VK candidates.
- Changing Smart Update writer prompts or generated description grounding.
- Changing static event page generation or issue-report intake.
- Repairing/deleting event `6045` or similar coffee/menu/editorial posts.

### Affected surfaces

- `smart_event_update.py`
- `tests/test_smart_event_update_non_event_guards.py`
- production SQLite `event`, `event_source`, `eventposter`
- static event pages, Telegraph, Telegram/VK public event fanout

### Mandatory checks before closure or deploy

- Verify event `6045` source rows do not support a concrete event/date.
- Run the focused non-event guard test for the coffee/music post and existing non-event guard suite.
- Repair canonical production DB row so the next static generation removes or corrects the page.
- If public Telegram/VK/Telegraph surfaces are repaired, verify those URLs after edit/delete.
- Confirm the prevention SHA is reachable from `origin/main` before declaring released.

### Required evidence

- Prod DB before/after query for event `6045`.
- Source text evidence from `event_source` and/or public source URLs.
- Test output.
- Branch/SHA and release/deploy evidence.

## Immediate Mitigation

- New static-page report was accepted and handed to ArtKodex task `T-000060`.
- Code prevention patch added in the feature branch.

## Corrective Actions

- Smart Update now routes auto-ingested Telegram/VK candidates with an ungrounded date, no candidate time, and no concrete source date/time signals to LLM eventness review before create/merge.
- Eventness prompt now states that menu/drink/product/editorial art/music-association posts are non-events unless a concrete schedule/programme exists.
- Regression test added for the `signalkld/11052` coffee/music source shape.

## Follow-up Actions

- [x] Repair production event `6045` canonical row after source-grounded review (`lifecycle_status=cancelled`, `silent=1`, backup `codex_backup_event6045_non_event_20260703_*`).
- [ ] Public Telegram/Telegraph cleanup/rebuild verification remains to be handled by the ArtKodex incident task if applicable.
- [ ] Replay `signalkld/11052` through the production import + Smart Update boundary on a snapshot before closure.
- [ ] Back-merge/release prevention patch to `origin/main` and verify deployed SHA.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: `.venv/bin/python -m pytest -q tests/test_smart_event_update_non_event_guards.py` -> `28 passed`; `.venv/bin/python -m py_compile smart_event_update.py` -> ok
- production data repair: event `6045` backed up to `codex_backup_event6045_non_event_20260703_*` and updated to `lifecycle_status=cancelled`, `silent=1`; exporter filters non-active rows, so the next static generation will remove the page
- post-deploy verification: pending

## Prevention

This incident is kept as an active regression contract for ungrounded future dates and unsupported writer copy. Closure requires both source replay and canonical content repair, not only the prompt/routing patch.
