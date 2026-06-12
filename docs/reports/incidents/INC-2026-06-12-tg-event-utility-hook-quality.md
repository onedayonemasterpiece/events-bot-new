# INC-2026-06-12 Telegram Event Utility Hook Quality

Status: open
Severity: sev3
Service: Telegram event publishing (`@kldevents`)
Opened: 2026-06-12
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-04-tg-monitoring-media-and-digest-quality`
Related docs: `docs/features/tg-publishing/README.md`

## Summary

Telegram post `https://t.me/kldevents/354` for event `5923` (`Прием шин`) received a badly mismatched intro/hook because `tg_event_publish` used the public `event.description` when it was not a technical SDK dump, even though that description was semantically wrong for the source. The source text clearly described a free tire collection/recycling point, while the description/search digest talked about a program with musical numbers, theatre performances, spectators, and tickets.

## User / Business Impact

- Channel subscribers saw an event intro that did not match the actual useful city-service action.
- The post framed a practical recycling/drop-off event as entertainment, reducing trust in `@kldevents` event announcements.
- The defect could recur for other utility/service imports where Smart Update writer output is syntactically valid but semantically conflicts with source text.

## Detection

- Operator reported `https://t.me/kldevents/354` as "очень плохо подходящий хук".
- Production DB confirmed event `5923` had a clear `source_text` about tire recycling and a conflicting hallucinated `description/search_digest`.

## Timeline

- 2026-06-11 23:14 UTC — Telegram source `ecoklgd/3253` imported as event `5923`.
- 2026-06-12 — `tg_event_publish` published `@kldevents/354` using the bad description-derived intro.
- 2026-06-12 — Operator reported the public mismatch.
- 2026-06-12 — Fix scoped to Telegram event intro source selection, prompt format, and repair of post `354`.

## Root Cause

1. `job_publish_tg_event_post()` preferred `event.description` over `event.source_text` unless the description looked like a GenAI SDK dump.
2. The Telegram rewrite prompt forced a hook question as the first phrase, even for utility/service posts where a practical benefit paragraph is more suitable.
3. There was no guardrail for syntactically valid but semantically conflicting entertainment wording on utility/service sources.
4. Telegram type hashtags were built from the same conflicting description, so a repaired utility intro could still leak unrelated tags such as `#спектакль`.

## Contributing Factors

- `Прием шин` had a bad stored `event_type='ярмарка'` and hallucinated public description, increasing the chance of entertainment-style copy.
- The publisher did not distinguish "bad meaning" from "bad technical dump".

## Automation Contract

### Treat as regression guard when

- Changing `tg_event_publish`, `build_tg_event_hook_text`, `build_tg_event_announcement_for_publish`, or source selection for Telegram event captions.
- Changing Smart Update description fallback policy for utility/service events.
- Repairing or re-importing event `5923` / `@kldevents/354`.

### Affected surfaces

- `main.py::job_publish_tg_event_post`
- `main_part2.py::build_tg_event_hook_text`
- `main_part2.py::select_tg_event_text_for_publish`
- Telegram Bot API edit/publish path for `@kldevents`
- Production DB fields: `event.description`, `event.source_text`, `event.tg_event_source_hash`

### Mandatory checks before closure or deploy

- `pytest tests/test_tg_event_publish.py -q`
- Verify utility/service source text wins over conflicting entertainment description.
- Verify hook rewrite may return a useful non-question intro without automatic `Что здесь стоит увидеть?` prefix.
- Verify Telegram type hashtags ignore a utility event description that conflicts with source text.
- Production repair evidence for `@kldevents/354`: edited caption or explicit reason why edit was not possible.
- `/healthz` after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test output for `tests/test_tg_event_publish.py`.
- Production DB evidence for event `5923`.
- Bot API edit evidence or final Telegram post URL after repair.

## Immediate Mitigation

- Repair `@kldevents/354` caption so it explains the useful action: residents can bring clean tires for recycling, with the per-person limit and practical benefit.

## Corrective Actions

- Add utility/service source detection for Telegram intro generation.
- Prefer `source_text` over conflicting hallucinated descriptions for utility/service posts.
- Update Telegram intro prompt to allow a useful paragraph or friendly intro instead of requiring a hook question.
- Reject entertainment wording in LLM output for tire collection/recycling posts and fall back to a practical utility intro.
- Exclude the same conflicting description from Telegram type hashtag selection.

## Follow-up Actions

- [ ] Audit whether event `5923` Telegraph description should also be repaired by Smart Update replay/data repair, because the public Telegraph page may still contain the hallucinated description.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- Regression tests in `tests/test_tg_event_publish.py` pin the utility/source-text selection and non-question useful intro behavior.
