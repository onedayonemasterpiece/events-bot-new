# INC-2026-07-27 City Jazz title/OCR conflict

Status: mitigated
Severity: sev2
Service: Telegram Monitoring / Smart Update / public event publications
Opened: 2026-07-27
Closed: —
Owners: events-bot
Related incidents: `INC-2026-07-02-exhibition-duplicates-static-site`, `INC-2026-05-17-future-event-quality-regressions`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/incident-management.md`

## Summary

Production event `7036` for «Калининград Сити Джаз» was created with title
`КАЛИНИНГРАД СИТИ ДА БИСТРО ЯНТАРЬ`, `event_type=выставка`, and exhibition prose.
The Telegram caption explicitly named the festival and supplied its three-day
program, but a noisy visual/OCR title won during Telegram extraction. Smart Update
later proposed the grounded title `Калининград Сити Джаз '26`, then correctly
rejected it as a semantic mismatch against the already-persisted bad candidate
title.

## User / Business Impact

- Telegram, VK, Telegraph and the canonical/static event projection could show a
  meaningless title and describe a live music festival as an exhibition.
- The wrong type and prose hid the actual daily roster and international headliners.
- Event discovery/search and future participant extraction inherited the bad public
  identity.

## Detection

- Found during the 2026-07-27 participant/headliner audit and confirmed by the user.
- Authenticated Telegram inspection confirmed the source caption and managed
  `@kldevents/2758` text.
- Authenticated VK API inspection confirmed managed post
  `wall-231920894_7874` carried the same wrong title/exhibition copy.
- Runtime mirror was enabled (`/data/runtime_logs`, 48-hour retention); event `7036`
  and later Smart Update/vector/publication activity were present.

## Timeline

- 2026-07-22 13:11 UTC — source `@meowafisha/8017` published the explicit festival
  caption and three-day program.
- 2026-07-23 — event `7036` materialized with the OCR-conflict title and exhibition
  semantics.
- 2026-07-25 08:42 UTC — the wrong row was published to managed Telegram.
- 2026-07-27 — audit identified and reproduced the caption/OCR conflict; incident
  opened and production repair started.
- 2026-07-27 15:44–15:47 UTC — repaired canonical data was drained through the
  event publication outbox; ICS, Telegraph, calendar, VK and the manually
  repaired Telegram event post were reconciled and every event `7036` job
  reached `done`.

## Root Cause

1. Telegram Monitoring's first LLM extraction selected a noisy OCR-only phrase as
   title even though the original caption explicitly named the festival.
2. The existing title-review pass only triggered for syntactic service headings
   such as dates, prices or `НАЧАЛО В ...`; a plausible-looking OCR phrase did not
   trigger review.
3. Smart Update's semantic mismatch guard prevented the later correct title from
   overwriting an unrelated persisted title. That guard limited cross-event damage,
   but could not repair the upstream title identity.

## Contributing Factors

- The post was a long festival program with several artists and multiple media items.
- The wrong `event_type=выставка` then steered fact-first public prose toward an
  invented exhibition narrative.
- No alert compared an OCR-only extracted title with the named event in the original
  caption before publication.

## Automation Contract

### Treat as regression guard when

- Changing Telegram Monitoring title extraction, OCR merge, title review or festival
  program handling.
- Changing Smart Update create/merge title guards.
- Repairing event `7036` or republishing its Telegram/VK/Telegraph surfaces.

### Affected surfaces

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `smart_event_update.py`
- production SQLite `event`, `event_source`, `event_source_fact`, `joboutbox`
- Telegraph, `@kldevents`, VK `klgdevents`, static event export/search vectors

### Mandatory checks before closure or deploy

- Unit regression proves an OCR-only title conflict triggers the LLM title-review
  lane, while a caption-grounded title and a short caption do not.
- Replay `tests/replays/INC-2026-07-27-city-jazz-title-ocr-conflict/source.json`
  through Telegram Monitoring extraction and Smart Update on a production snapshot
  or shadow DB.
- Negative control keeps a legitimate title that exists only on a poster when the
  caption does not name another event.
- Verify production event `7036` title, type, description and facts after repair.
- Verify Telegraph, authenticated Telegram and authenticated VK text after repair.
- Confirm vector/static exports no longer carry the wrong title.

### Required evidence

- Production before/after SQL and row-level backup table.
- Authenticated Telegram/VK before/after receipts and Telegraph browser check.
- Replay output with Smart Update DB diff.
- Focused test output.
- Deployed SHA reachable from `origin/main`.

## Immediate Mitigation

- Production event `7036` now has title `Калининград Сити Джаз '26'`,
  `event_type=фестиваль`, the 2026-07-31–2026-08-02 program and explicit
  performer/country facts from the source caption.
- Narrow pre-repair rows are preserved in
  `codex_backup_20260727_city_jazz_7036_{event,event_source,event_source_fact,eventposter,joboutbox}`.
- Telegraph, authenticated `@kldevents/2758`, authenticated VK
  `wall-231920894_7874`, ICS and calendar post were repaired. The legacy
  Telegraph path retains its old slug, but its title and body are correct.
- Final `joboutbox` state for `ics_publish`, `telegraph_build`, `tg_ics_post`,
  `vk_sync` and `tg_event_publish` is `done`; SQLite `quick_check=ok` and
  `/healthz` is ready with no reported issue.

## Corrective Actions

- Telegram Monitoring now sends both service-heading titles and OCR-only titles that
  are not grounded in a substantial original caption to the existing LLM title-review
  stage.
- The deterministic helper only decides whether review is needed; the title decision
  remains LLM-owned.
- The incident source and OCR conflict are preserved under
  `tests/replays/INC-2026-07-27-city-jazz-title-ocr-conflict/`.

## Follow-up Actions

- [ ] Complete full production-boundary Telegram Monitoring + Smart Update replay and
  record the DB diff.
- [ ] Add monitoring for caption/OCR title conflicts that survive review or fail
  because the review provider is unavailable.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: focused Telegram Monitoring contract
  `41 passed`; full production-boundary extraction + Smart Update replay pending
- production repair: complete, with narrow backup and final `quick_check=ok`
- public verification: complete for authenticated Telegram and VK plus
  Telegraph API/browser; all event-specific publication jobs are terminal
  `done`
- post-deploy verification: pending because the prevention code has not been
  deployed

## Prevention

Title review must compare independent caption and OCR evidence lanes. OCR may supply
the only title when the caption is generic, but it must not silently replace a named
attendee-facing event in the original caption.
