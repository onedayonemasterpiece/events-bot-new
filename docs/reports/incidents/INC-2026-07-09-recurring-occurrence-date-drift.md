# INC-2026-07-09 Recurring occurrence date drift

Status: mitigated
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
- 2026-07-09 08:07 UTC — prevention commit `3adbd0ef` deployed to Fly image `deployment-01KX2YJW9H6JN3DRD9W6PWNGQ6`; `/healthz` ready after deploy.
- 2026-07-09 08:09–08:13 UTC — production event `3980` backed up and repaired; ICS, Telegraph, VK and Telegram surfaces updated and verified.

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

- Created row-level production backup tables before mutation:
  - `codex_backup_20260709_knight_date_event` (`1` row)
  - `codex_backup_20260709_knight_date_event_source` (`6` rows)
  - `codex_backup_20260709_knight_date_eventposter` (`7` rows)
  - `codex_backup_20260709_knight_date_joboutbox` (`6` rows)
- Repaired event `3980` canonical anchors: `date 2026-05-01 -> 2026-07-10`, `time=20:00`, `end_date -> NULL`, `date_provenance='source_text'`, `date_confidence=0.95`.
- Resolved stale managed VK URL `wall-231920894_6681` to live managed post `https://vk.com/wall-231920894_6701`.
- Rebuilt ICS (`event-3980-2026-07-10.ics`), Telegram calendar post (`https://t.me/kenigeventscalendar/7377`), Telegraph page, VK post and Telegram `@kldevents/2154`.
- A duplicate body block introduced by the first VK edit was removed with a narrow authenticated `wall.edit` that preserved the two photo attachments.

## Corrective Actions

- Added LLM-first Smart Update prompt rules: exact single-date occurrences inside recurring/season ranges must create/stay distinct unless the source explicitly updates the whole series.
- Added a narrow identity guardrail so shared ticket/title/place/poster anchors do not force a single occurrence to mutate a recurring season row or veto occurrence creation.
- Added regression tests and the minimal replay fixture for the 10 July `Рыцарский турнир` source.

## Follow-up Actions

- [ ] Run closure-grade VK import + Smart Update replay on a prod snapshot/shadow DB, including the negative season-update control.
- [ ] Design first-class recurrence/occurrence modeling so season rows and attendee-facing occurrence rows do not share one mutable public identity.
- [ ] Add a fresh-publication audit that compares public date lines with newest source text/OCR when a row has `end_date` and newly imported single-date evidence.

## Release And Closure Evidence

- deployed SHA: `3adbd0ef01299c5e974fa6a2e0539c70ef6948be`, reachable from `origin/main`.
- deploy path: clean linked worktree `.worktrees/incident-20260709-date`, `flyctl deploy -a events-bot-new-wngqia`; image `events-bot-new-wngqia:deployment-01KX2YJW9H6JN3DRD9W6PWNGQ6`; Fly machine `2860d45f312248`, version `1614`, `1 passing` check.
- regression checks:
  - `python3 -m py_compile smart_event_update.py smart_update_identity.py tests/test_smart_update_merge_identity_gate.py tests/test_smart_event_update_duplicate_guards.py` — passed.
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 uv run --with-requirements requirements.txt python -m pytest -q -p pytest_asyncio.plugin tests/test_smart_update_merge_identity_gate.py tests/test_smart_event_update_duplicate_guards.py::test_match_create_prompt_distinguishes_time_conflict_from_multi_session` — `9 passed`.
- production repair evidence:
  - DB `PRAGMA quick_check=ok`; event `3980` now has `date='2026-07-10'`, `time='20:00'`, `end_date=NULL`, `source_vk_post_url='https://vk.com/wall-231920894_6701'`, new ICS URL `event-3980-2026-07-10.ics`, fresh Telegram/VK/Telegraph hashes.
  - Telegram `https://t.me/c/3954607218/2154` inspected via local E2E Telethon: text starts `📅 10 июля 20:00`, no stale May range.
  - VK `https://vk.com/wall-231920894_6701` inspected via VK API: text starts `📅 10 июля 20:00`, two photo attachments preserved, duplicate block removed.
  - Telegraph `https://telegra.ph/Rycarskij-turnir-Anny-Marii-04-16` text contains `10 июля` / `20:00` and no `1 мая` / `30 сентября`.
  - `/healthz` after repair returned `ok=true`, `ready=true`, `db=ok`, `issues=[]`.
- runtime evidence: file mirror checked; `ENABLE_RUNTIME_FILE_LOGGING=0`, `/data/runtime_logs/events-bot.log` existed but was stale (`mtime=2026-07-08T13:22:56Z`) for the 2026-07-09 incident window, so DB/joboutbox/public API artifacts are the durable fallback evidence.

## Prevention

The durable prevention is to keep recurring/season cards and exact occurrences as separate attendee-facing identities at the LLM match/merge boundary, with deterministic guardrails only as fail-closed support. The incident remains `mitigated` until a closure-grade VK import + Smart Update replay on a prod snapshot/shadow DB is added for this fixture and its negative control.
