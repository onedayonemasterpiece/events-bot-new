# INC-2026-07-09 Recurring occurrence date drift

Status: open
Severity: sev2
Service: VK auto-import / Smart Update / public `@kldevents` and `klgdevents` event posts
Opened: 2026-07-09
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-07-07-new-event-quality-degradation.md`, `INC-2026-07-03-event-6045-static-defect.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-07-03-current-import-vector-vk-publication.md`, `INC-2026-05-30-active-duplicate-events-recall-gate.md`
Related docs: `docs/features/smart-event-update/README.md`, `docs/llm/request-guide.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`

## Summary

A fresh 2026-07-09 VK source for `Рыцарский турнир` explicitly announced the next occurrence on **10 July 2026 at 20:00**, and the new poster OCR also said `10 июля 20:00`. Smart Update attached that source and fresh media to the old recurring/season row dated **1 May 2026** with `end_date=2026-09-30`. Public Telegram/VK fanout then showed stale May/season dates with fresh July media.

This is a production incident because users saw a wrong logistics date in published event posts immediately before the real occurrence.

## User / Business Impact

- VK `klgdevents` showed `📅 1 мая 20:00` for a post whose image/source advertised `10 июля 20:00`.
- Telegram `@kldevents` showed `🎟 1 мая–30 сентября 20:00` for the same fresh occurrence.
- Readers could attend on the wrong date or distrust event logistics.
- The same recurrence/season merge shape can affect other repeating events when a fresh exact occurrence is imported.

## Detection

- Detected by operator screenshot on 2026-07-09.
- Production DB evidence found event `3980`, source `https://vk.com/wall-222073295_9296`, imported `2026-07-09 04:18:44 UTC`, with source text and poster OCR grounded to `10 июля 20:00` while `event.date='2026-05-01'` and `event.end_date='2026-09-30'`.
- Authenticated VK API resolved the live managed post as `https://vk.com/wall-231920894_6701` (stored `6681` was stale/unavailable) and confirmed the public stale date.
- Local Telethon E2E inspection confirmed Telegram `https://t.me/c/3954607218/2154` showed the stale May–September range.

## Timeline

- 2026-04-16 16:57 UTC — event `3980` created from the first season/opening source with `date=2026-05-01`, `end_date=2026-09-30`.
- 2026-07-09 04:18 UTC — source `wall-222073295_9296` imported with explicit `10 июля` occurrence and new media.
- 2026-07-09 04:19 UTC — Telegraph/ICS/VK jobs ran from stale canonical date; managed VK post later resolved to live id `6701`.
- 2026-07-09 07:02 UTC — Telegram event post `@kldevents/2154` published from the stale recurring row.
- 2026-07-09 — operator reported screenshot; incident investigation opened.

## Root Cause

1. **Smart Update identity matching treated an exact occurrence as the same public row as the recurring season.** A single-date source (`10 июля 20:00`) matched the broader `1 мая — 30 сентября` row because title/place/ticket were shared.
2. **The merge path intentionally did not update non-parser `event.date` in ordinary conflicts.** That conservative rule prevented random social sources from rewriting dates, but here it left the stale season start date while allowing source/media/publication side effects.
3. **LLM match/merge prompts did not distinguish “same recurring programme” from “same attendee-facing occurrence”.** They warned about long-running exhibitions, but not recurring/season event rows.
4. **Deterministic identity rails considered shared ticket/title/place strong enough.** That could veto creating a new occurrence or suppress a structural merge veto.
5. **Public fanout jobs reused canonical anchors after side effects.** Fresh July posters and sources caused republish/edit jobs, but the date line still came from the old canonical row.

## Contributing Factors

- The schema stores recurrence/season rows as ordinary events with `end_date`, not as first-class recurrence rules plus occurrences.
- The stored managed VK URL can become stale; authenticated search was required to find the live wall id.
- Runtime log retention is short; DB/source/public API evidence is the durable proof for this import.

## Automation Contract

### Treat as regression guard when

- Changing Smart Update match/create prompts, merge identity gate prompts, identity deterministic rails, vector identity gate, or duplicate probes.
- Changing recurring/season event handling, `end_date` semantics, or publication date formatting.
- Changing VK auto-import or Telegram Monitoring paths that attach fresh sources/media to existing rows.
- Repairing public event posts for recurring events.

### Affected surfaces

- `smart_event_update.py` (`_llm_match_or_create_bundle`, `_llm_merge_identity_gate`, merge side effects).
- `smart_update_identity.py` create/merge identity verdicts.
- Production SQLite `event`, `event_source`, `eventposter`, `joboutbox`.
- Public Telegram `@kldevents`, VK `klgdevents`, Telegraph event page, ICS URL.

### Mandatory checks before closure or deploy

- Unit tests proving:
  - LLM match/create prompt contains the recurring occurrence rule.
  - merge identity gate blocks `single occurrence -> recurring series` side effects even when title/place/ticket match.
  - create identity gate does not veto a fresh single occurrence merely because a recurring season row shares title/place/ticket.
- Replay fixture `tests/replays/INC-2026-07-09-recurring-occurrence-date-drift/knight_occurrence_source.json` must remain available; closure-grade replay should run the source through VK import + Smart Update on a prod snapshot/shadow DB.
- Negative control: a source that explicitly updates the whole recurring season/range may still merge after LLM identity-gate approval.
- Production repair must back up touched rows before mutation.
- Public Telegram, VK, and Telegraph surfaces must be verified after repair.
- Runtime log mirror or documented fallback evidence must be checked.
- Release governance: fix SHA reachable from `origin/main` before closure.

### Required evidence

- Test output for the recurrence identity guard.
- Prod DB before/after for event `3980` and affected side tables.
- VK API before/after for managed wall post.
- Telegram Telethon/Bot API before/after for `@kldevents/2154` or replacement URL.
- Telegraph URL after rebuild/edit.
- Deployed SHA and post-deploy health if code is deployed.

## Immediate Mitigation

- Pending at incident open: canonical/public repair is required for event `3980` so the public date shows 10 July 2026 at 20:00 and stale May/season anchors do not remain in event fanout.

## Corrective Actions

- Added LLM-first Smart Update prompt rules: exact single-date occurrences inside recurring/season ranges must create/stay distinct unless the source explicitly updates the whole series.
- Added a narrow identity guardrail so shared ticket/title/place/poster anchors do not force a single occurrence to mutate a recurring season row or veto occurrence creation.
- Added regression tests and the minimal replay fixture for the 10 July `Рыцарский турнир` source.

## Follow-up Actions

- [ ] Run closure-grade VK import + Smart Update replay on a prod snapshot/shadow DB, including the negative season-update control.
- [ ] Design first-class recurrence/occurrence modeling so season rows and attendee-facing occurrence rows do not share one mutable public identity.
- [ ] Add a fresh-publication audit that compares public date lines with newest source text/OCR when a row has `end_date` and newly imported single-date evidence.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending full run; targeted recurrence tests added.
- post-deploy verification: pending.

## Prevention

The durable prevention is to keep recurring/season cards and exact occurrences as separate attendee-facing identities at the LLM match/merge boundary, with deterministic guardrails only as fail-closed support. Row-level repair alone is not sufficient because future Friday occurrence posts would otherwise keep refreshing the old season row.
