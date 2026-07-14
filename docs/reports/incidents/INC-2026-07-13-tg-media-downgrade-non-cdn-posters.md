# INC-2026-07-13 Telegram media downgrade and non-CDN posters

Status: closed
Severity: sev2
Service: Smart Update event media / `@kldevents` / static site
Opened: 2026-07-13
Closed: 2026-07-13
Owners: events-bot production
Related incidents: `INC-2026-06-09-event-media-duplicates`, `INC-2026-05-11-poster-near-duplicate-and-tram-photo-dropped`, `INC-2026-05-05-event-source-media-aggregation-gap`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`
Related docs: `docs/features/event-media/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/tg-publishing/README.md`, `docs/features/static-site-pages/README.md`

## Summary

Fresh Telegram event publications `@kldevents/2341`, `/2342`, `/2351` and
`/2352` were text-only. The same failure family also affected `/2353` and
`/2356`. Production persisted active gallery URLs on raw Object Storage and
source-CDN hosts rather than the required `static.kenigevents.ru` CDN.

## User / Business Impact

- Six announcements were published without an event image.
- The then-current static preview rendered some event images from external VK
  CDN/source hosts, so load time and availability were outside our CDN contract.
- A source image becoming unavailable could destructively downgrade an existing
  photo publication to text.

## Detection

- User report on 2026-07-13.
- Evidence: `artifacts/codex/INC-2026-07-13-tg-publications-without-media-cdn/`.
- The old host audit accepted raw Object Storage as managed and had no strict
  failure mode for the final static projection.

## Timeline

- 2026-07-13 12:37 UTC — event `6318` published as text (`/2341`).
- 2026-07-13 12:47 UTC — event `5298` published as text (`/2342`).
- 2026-07-13 — events `5297` and `5296` published as `/2351` and `/2352`;
  related events `3742` and `5269` were also found as `/2353` and `/2356`.
- 2026-07-13 — runtime evidence showed stale Tretyakov URLs returning 404 and
  an invalid Dram Theatre candidate; cleanup removed them and the publisher
  accepted a text-only mode.
- 2026-07-13 — Smart Update/media-gate fixes deployed in `2c6ba2fe` and source
  recovery fix deployed in `33b20342`; both are reachable from `origin/main`.
- 2026-07-13 — all six Telegram publications replaced with photo posts; all
  corresponding Telegraph and managed VK surfaces verified with media.
- 2026-07-13 — canonical active/non-silent current-or-ongoing inventory
  backfilled through the same media gate: `272` events, `672` approved gallery
  refs, zero non-CDN projections after a bounded retry.
- 2026-07-13 — fresh static preview built from the final snapshot: `267` public
  events and `14,656` rendered HTML image references, all on
  `static.kenigevents.ru`. The prefix was published and the public root landing
  was promoted to it.
- 2026-07-13 18:55 UTC — final hardening SHA `215c7c36` deployed as Fly
  release `v1660`; health ready, DB ok, machine check passing.

## Root Cause

1. The parser's existing-event ticket-status fast path did not reconcile media.
2. `SOURCE_PARSING_DISABLE_OCR_SOURCES` dropped Tretyakov photos instead of
   merely skipping OCR; Dram Theatre fallback regexes matched literal escape
   sequences.
3. The media ledger could approve/project source URLs before CDN
   materialization.
4. Telegram publisher accepted an empty approved gallery and could replace a
   photo-mode post with text mode.
5. TelegramMonitor emitted raw Object Storage URLs and defaulted to the legacy
   bucket.
6. Static export still had renderer fallbacks to source `catbox_url` and did not
   apply the complete canonical non-silent/current-or-ongoing predicate.
7. During CDN retry, equal downloaded bytes could hit the per-event partial
   unique raw-SHA index and abort the transaction instead of retaining the
   survivor identity and continuing automated pair review.

## Contributing Factors

- Single-source events with zero usable posters were excluded from rehydration.
- Host observability did not enforce the final CDN host.
- The previously published static preview was stale and predated the strict
  consumer gate.

## Automation Contract

### Treat as regression guard when

- changing Smart Update poster ingestion/projection, source parsers,
  TelegramMonitor storage, static gallery export, or Telegram event publishing.

### Affected surfaces

- `event_media.py`, `smart_event_update.py`, `source_parsing/handlers.py`;
- `kaggle/TelegramMonitor/telegram_monitor.py` and launch payload;
- scheduling, `job_event_media_review`, `publish_tg_event_announcement`;
- static production exporter and `EVENT_MEDIA_REQUIRE_CDN`/CDN/bucket config.

### Mandatory checks before closure or deploy

- strict gate cannot project a source/non-CDN URL;
- current-bucket raw URLs canonicalize to CDN without copying;
- existing parser updates submit images to the Smart Update gate;
- Telegram cannot send text without approved CDN media or delete the old post;
- static exporter rejects source-CDN/Supabase/legacy fallbacks and excludes
  silent rows while retaining ongoing events by `end_date`;
- canonical current/ongoing production gallery audit has zero non-CDN refs;
- Telegram, static, Telegraph and managed VK surfaces are verified.

### Required replay

`tests/replays/INC-2026-07-13-tg-media-downgrade-non-cdn-posters/source-media.json`
replays the exact Tretyakov and Dram parser shapes through the existing-event
Smart Update boundary. `tests/test_source_parsing.py` proves both rows emerge as
approved CDN media and project one image. The negative control
`test_strict_cdn_gate_does_not_project_unmaterialized_source_url` proves that a
failed materialization cannot fall back to the source URL.

## Immediate Mitigation

- Telegram publisher now fails closed when strict CDN media is absent and never
  creates/downgrades to a text-only event post.
- The six affected announcements were replaced with photo publications:
  - `/2341` → <https://t.me/kldevents/2365> (`6318`)
  - `/2353` → <https://t.me/kldevents/2366> (`3742`)
  - `/2356` → <https://t.me/kldevents/2367> (`5269`)
  - `/2352` → <https://t.me/kldevents/2368> (`5296`)
  - `/2351` → <https://t.me/kldevents/2369> (`5297`)
  - `/2342` → <https://t.me/kldevents/2370> (`5298`)
- Old text-only messages were removed only after replacement verification.

## Corrective Actions

- Materialize every Smart Update poster in the CDN-fronted current bucket before
  approval/projection; retry via `event_media_review`.
- Preserve source URL as provenance and publish only the CDN URL.
- Reconcile current images even on existing-event parser updates.
- Separate “skip OCR” from “drop photos”; fix Dram Theatre extraction.
- Require CDN media in Telegram production publishing.
- Emit CDN URLs from TelegramMonitor and add strict host-audit failure mode.
- Make the static exporter a second fail-closed consumer gate: approved ledger
  rows only, `static.kenigevents.ru` only, with safe canonicalization of the
  current raw bucket and no source/Supabase/legacy fallback.
- Keep equal raw SHA on the existing survivor during CDN retry so the unique
  index cannot abort the repair transaction.

## Production Repair Evidence

All changes were backup-first and scoped. Backup tables:

- `incident_backup_20260713_tg_media_event`
- `incident_backup_20260713_tg_media_eventposter`
- `incident_backup_20260713_tg_media_joboutbox`
- `incident_backup_20260713_cdn_rewrite_event` / `_eventposter`
- `incident_backup_20260713_cdn_backfill_event` / `_eventposter`
- `incident_backup_20260713_cdn_ongoing_event` / `_eventposter`

Final read-only production inventory audit:

- canonical eligible events: `272`;
- approved gallery rows / projected refs: `672 / 672`;
- non-CDN approved posters: `0`;
- non-CDN `Event.photo_urls` projections: `0`.

Final snapshot: `prod-final-cdn.sqlite`, SQLite `quick_check=ok`, `251,301,888`
bytes, SHA-256
`6a22424fefbb04559538b1eaef903a6eb0deb670d8f4317133b639973c808516`.

Public surface evidence:

- Telegram `/2365`–`/2370`: authenticated Telethon check, all `photo=true`;
- Telegraph: all six pages HTTP 200, one image each, only
  `static.kenigevents.ru`;
- managed VK: posts `7381`, `7382`, `7383`, postponed `7384`, and posts `7397`,
  `7398` all contain photo attachments; stale postponed ids were reconciled;
- static fresh build: `267` events, `925` exported event-gallery refs and
  `14,656` rendered HTML image refs, with zero non-CDN hosts.

## Follow-up Actions

- [x] Deploy the core Smart Update/Telegram fixes from clean SHAs reachable from
  `origin/main`.
- [x] Backup and repair all six affected events through the media gate.
- [x] Backfill the complete canonical active non-silent current/ongoing inventory.
- [x] Verify Telegram, Telegraph and managed VK public surfaces.
- [x] Publish/promote and verify the fresh strict-CDN static preview.
- [x] Deploy the final static consumer/retry hardening SHA from `origin/main`.

## Release And Closure Evidence

- core deployed SHAs: `2c6ba2fe`, `33b20342` (both in `origin/main`);
- final hardening SHA: `215c7c36`, reachable from `origin/main`;
- Fly release: `v1660`, machine `2860d45f312248`, check `1/1` passing;
- post-deploy health: `ok=true`, `ready=true`, DB `ok`, issues `[]`;
- targeted regression suite: `58 passed` plus incident source replay `1 passed`;
- post-deploy production audit: `272` eligible / `672` approved / `672`
  projections, zero non-CDN refs;
- public static preview: <https://kenigevents.ru/preview-20260713t183933-4dc287e3/__preview/>;
  all `288/288` HTML pages returned 200 and all `14,656` image refs used
  `static.kenigevents.ru`; root landing points to this build.

## Prevention

This is not a manual review process. Every producer—including normal Smart
Update, existing-event source refresh and TelegramMonitor—feeds the same media
ledger/materialization gate. Non-CDN media stays pending with a durable retry;
it cannot enter `Event.photo_urls`. Telegram and static export independently
fail closed if that invariant is violated, so a future source outage cannot
silently create a text-only announcement or make the site render a source URL.
LLM/VLM pair review remains bounded and is used only for ambiguous visual
similarity; exact transport/CDN enforcement is deterministic.
