# INC-2026-07-13 Telegram media downgrade and non-CDN posters

Status: open
Severity: sev2
Service: Smart Update event media / `@kldevents` / static site
Opened: 2026-07-13
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-06-09-event-media-duplicates`, `INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped`, `INC-2026-05-05-event-source-media-aggregation-gap`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`
Related docs: `docs/features/event-media/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/tg-publishing/README.md`

## Summary

Fresh Telegram event publications `@kldevents/2341`, `/2342`, `/2351` and
`/2352` were text-only. Production also persisted recent public gallery URLs on
raw Object Storage and source-CDN hosts instead of the required
`static.kenigevents.ru` CDN.

## User / Business Impact

- Four reported announcements lost their event image.
- New static-site cards could load slowly from origin/source hosts.
- A source image becoming unavailable could destructively downgrade an existing
  photo publication to text.

## Detection

- User report on 2026-07-13.
- Read-only evidence: `artifacts/codex/INC-2026-07-13-tg-publications-without-media-cdn/`.
- The host audit did not distinguish CDN from raw Object Storage and had no
  strict failure mode.

## Timeline

- 2026-07-13 12:37 UTC — event `6318` published as text (`/2341`).
- 2026-07-13 12:47 UTC — event `5298` published as text (`/2342`).
- 2026-07-13 — events `5297` and `5296` later published as `/2351` and `/2352`.
- 2026-07-13 — logs showed stale Tretyakov URLs returning 404 and an invalid
  Dram Theatre candidate; cleanup removed them and publisher accepted text mode.

## Root Cause

1. The parser's existing-event ticket-status path did not reconcile current media.
2. `SOURCE_PARSING_DISABLE_OCR_SOURCES` dropped Tretyakov photos instead of only
   skipping OCR; Dram Theatre fallback regexes matched literal escape sequences.
3. The media ledger could approve/project source URLs before CDN materialization.
4. Telegram publisher accepted an empty approved gallery as text mode.
5. TelegramMonitor emitted raw Object Storage URLs and defaulted to the legacy bucket.

## Contributing Factors

- Single-source events with zero usable posters were excluded from rehydration.
- Host observability did not enforce the CDN host.

## Automation Contract

### Treat as regression guard when

- changing Smart Update poster ingestion/projection, source parsers,
  TelegramMonitor storage, static gallery export, or Telegram event publishing.

### Affected surfaces

- `event_media.py`, `smart_event_update.py`, `source_parsing/handlers.py`;
- `kaggle/TelegramMonitor/telegram_monitor.py` and launch payload;
- scheduling, `job_event_media_review`, `publish_tg_event_announcement`;
- `EVENT_MEDIA_REQUIRE_CDN`, CDN base URL and Yandex bucket config.

### Mandatory checks before closure or deploy

- strict gate cannot project a source/non-CDN URL;
- current-bucket raw URLs canonicalize to CDN without copying;
- existing parser updates submit images to the Smart Update gate;
- Telegram cannot send text without approved CDN media or delete the old post;
- current/future production gallery host audit has zero non-CDN references;
- `/2341`, `/2342`, `/2351`, `/2352`, static, Telegraph and managed VK are verified.

### Required evidence

- deployed SHA reachable from `origin/main` and Fly release;
- targeted tests and strict production host audit;
- before/after production DB/log/public evidence in the incident artifact directory.

## Immediate Mitigation

- Pending deployment and targeted repair. Once live, the publisher fails closed
  instead of creating further text-only announcements.

## Corrective Actions

- Materialize every Smart Update poster in the CDN-fronted current bucket before
  approval/projection; retry via `event_media_review`.
- Preserve source URL as provenance and publish only the CDN URL.
- Reconcile current images even on existing-event parser updates.
- Separate “skip OCR” from “drop photos”; fix Dram Theatre extraction.
- Require CDN media in Telegram production publishing.
- Emit CDN URLs from TelegramMonitor and add strict host-audit failure mode.

## Follow-up Actions

- [ ] Deploy from clean SHA reachable from `origin/main`.
- [ ] Backup and repair the four reported events through the media gate.
- [ ] Backfill canonical active current/future non-CDN gallery references.
- [ ] Verify Telegram, static, Telegraph and managed VK public surfaces.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The invariant is enforced at both boundaries: Smart Update cannot approve or
project non-CDN media, and Telegram cannot publish without approved CDN media.
All producers feed the same gate rather than writing a parallel gallery path.
