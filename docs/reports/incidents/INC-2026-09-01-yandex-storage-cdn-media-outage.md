# INC-2026-09-01 Yandex Storage/CDN Media Outage

Status: monitoring
Severity: sev1
Service: event media materialization, VK/TG event publishing, Yandex Object Storage/CDN
Opened: 2026-09-01
Closed: —
Owners: bot/runtime, Yandex Cloud operations, social publishing
Related incidents: `INC-2026-08-03-cherryflash-cdn-tls-retry-storm`, `INC-2026-08-03-yandex-cloud-reaper-service-suspension`, `INC-2026-07-13-tg-media-downgrade-non-cdn-posters`
Related docs: `docs/features/event-media/README.md`, `docs/features/vk-publishing/README.md`, `docs/features/static-site-pages/cdn-asset-delivery.md`, `docs/operations/runtime-logs.md`

## Summary

Object Storage stopped accepting writes after the `kenigevents.ru` bucket
exceeded its configured 50 GiB maximum. Event media therefore stopped
materializing to `static.kenigevents.ru` on 2026-08-24. VK-origin event jobs had
no guard for the state “poster candidate exists, but canonical URL is empty” and
silently created text-only posts. On 2026-09-01 a separate non-payment suspension
also detached effective CDN delivery/TLS; restoring billing alone did not restore
the edge certificate.

## User / Business Impact

- In managed VK wall ids `10070..10430`, 143 of 178 direct event-like posts
  (80.3%) had no attachment; the first affected public post was `10071`.
- Since the storage onset, 159 of 189 managed event records had
  `photo_count=0`; 147 of those had source poster candidates.
- Telegram did not silently downgrade to text-only: its fail-closed contract
  held. Publication instead stalled; the last managed event post observed was
  `@kldevents/3770` on 2026-08-30, with a media/publication backlog behind it.
- Static secret-candidate builds and every producer sharing the bucket were at
  risk; the CDN TLS regression also affected strict poster and ICS consumers.

## Detection

- The operator noticed that most recent VK posts were missing images.
- Existing runtime logging collapsed Object Storage write failures to `None`,
  so normal media jobs exposed only generic materialization failures.
- The exact provider code was recovered from `JobOutbox` row `66921` and
  reproduced with a controlled Object Storage write canary.

## Timeline

- 2026-08-03: an earlier cleanup reduced the bucket from 38.16 GiB to
  14.737 GiB; a manifest-aware `_review/` reaper was recommended but not built.
- 2026-08-13: retained inventory already measured 45.54 GiB.
- 2026-08-24 06:26:58 UTC: last successful affected media-review boundary.
- 2026-08-24 06:27:08 UTC: last new object under `p/image/v2/`.
- 2026-08-24 07:32 UTC: static build job `66921` recorded exact provider error
  `BucketMaxSizeExceeded`.
- 2026-08-24 08:32:10 UTC: first affected event `8308` completed `vk_sync`
  with empty canonical media; public post `wall-231920894_10071` followed at
  08:43 UTC.
- 2026-09-01 14:53:41 UTC: first retained strict TLS mismatch for
  `static.kenigevents.ru`; the edge served `*.yccdn.cloud.yandex.net`.
- 2026-09-01 20:20 UTC: 2,010 objects (368,390,764 bytes) from the exact failed,
  non-current static build prefix were deleted; the current candidate hash was
  explicitly protected.
- 2026-09-01 20:23 UTC: the issued Certificate Manager binding and active flag
  were re-applied to the exact CDN resource and its cache was purged.
- 2026-09-01 20:28 UTC: PUT/HEAD/public-origin-GET/DELETE canary passed, proving
  Object Storage writes had recovered after capacity accounting converged.
- 2026-09-01 20:39 UTC: public CDN recovery converged: 10/10 TLS handshakes
  served the `static.kenigevents.ru` SAN and 20/20 strict ICS GETs returned 200.
- 2026-09-01 20:47 UTC: the new durable-current-pointer-aware reaper dry-run selected 72 of
  75 candidate trees while retaining the exact current plus two newest
  non-current trees; the guarded apply removed 198,767 objects / 40,196,576,984
  bytes without exposing bearer tokens.
- 2026-09-01 20:43–21:00 UTC: normal media materialization resumed. Authenticated
  VK readback confirmed repaired photos on managed posts `10401`, `10410`,
  `10413`, and `10421..10425` (eight latest recoverable posts, 1–2 photos each).
  Post `10411` remains text-only: VK returned an empty upload response for both
  the official source image and one deterministic re-encode, so the incident
  handler stopped retrying the same provider failure instead of guessing.

## Root Cause

1. Immutable `_review/` candidates accumulated without a manifest-aware
   retention/reaper policy and consumed 43.13 GB. Total visible content reached
   53,859,720,065 bytes, above the bucket's 50 GiB maximum.
2. `upload_yandex_public_bytes()` swallowed every provider exception and
   returned `None`, hiding `BucketMaxSizeExceeded` from the runtime log.
3. The VK fail-closed guard covered Telegram-origin events and already
   materialized `photo_urls`, but not VK-origin events with accepted/pending
   `EventPoster` rows whose public URL could not be written.
4. A same-day unpaid-billing suspension independently halted CDN delivery.
   Yandex Cloud documents that unsuspending billing does not restore CDN content
   automatically; the resource must be restored by the operator.

## Contributing Factors

- Bucket capacity and strict public CDN TLS were not external readiness checks.
- Control-plane `CM/READY` did not prove which certificate the public edge served.
- The 2026-08-03 cleanup recommendation remained an unimplemented follow-up.

## Automation Contract

### Treat as regression guard when

- changing Yandex media upload/error handling or bucket retention;
- changing VK event-media fallbacks/fail-closed behavior;
- changing `static.kenigevents.ru` CDN, certificate or billing recovery;
- changing Telegram event-publication media gates.

### Affected surfaces

- `yandex_storage.py`, `event_media.py`, `main_part2.py`
- `JobOutbox` tasks `event_media_review`, `vk_sync`, `tg_event_publish`,
  `static_site_build`
- bucket `kenigevents.ru`, `_review/`, `p/`, and CDN resource
  `bc8rani5q2j4yfpl7oge`

### Mandatory checks before closure or deploy

- a provider storage failure logs bounded HTTP/error code without credentials;
- a new VK-origin event with an approved/pending poster but no materialized URL
  cannot cross `wall.post`;
- Object Storage PUT/HEAD/public-origin GET/DELETE canary passes;
- public TLS serves a SAN for `static.kenigevents.ru`, with 10/10 handshakes and
  20/20 strict representative object GETs;
- latest affected VK posts are edited in place with photo attachments;
- same-day media and Telegram backlog receives a compensating retry and is
  verified without text-only downgrade or duplicate posts;
- deployed SHA is clean, reachable from `origin/main`, and matches the Fly image.

### Required evidence

- sanitized bucket inventory and exact `BucketMaxSizeExceeded` row;
- guarded cleanup receipt and storage canary output;
- strict public CDN TLS/HTTP series;
- VK/TG authenticated readback and outbox status;
- tests, merged SHA, deployed SHA and `/healthz`.

## Immediate Mitigation

- Disproved the wrong-token hypothesis before making changes: all photo upload
  calls used the valid user actor; group-token photo calls were not attempted.
- Deleted only the exact failed/non-current static candidate prefix while
  protecting the current candidate by token hash.
- Re-applied the exact existing Certificate Manager binding and purged only the
  affected CDN resource.
- Applied the tested durable-current-pointer-aware retention pass: the current candidate and
  two rollback trees were retained; 72 superseded trees were removed.

## Corrective Actions

- Object Storage write failures now retain bounded provider error type, HTTP
  status and code in runtime logs without provider messages or credentials.
- New VK event posts fail closed when accepted/pending poster candidates exist
  but canonical media materialization is empty.
- Existing text-only VK posts also fail closed when canonical media exists but
  VK returns zero uploaded attachments; an edit cannot be reported as repair.
- Static candidate publication now performs bounded durable-current-pointer-aware retention
  before upload, protecting the durable current token hash plus two rollback
  candidates and a 48-hour grace cohort.
- Production repair reuses the normal media gate and edits existing latest VK
  posts rather than creating duplicates.

## Follow-up Actions

- [x] Implement a durable-current-pointer-aware `_review/` retention/reaper that
  always protects the durable current candidate and keeps bounded rollback
  candidates. Deployment and production receipt remain part of closure.
- [ ] Add external bucket-headroom and strict public CDN SAN/HTTP alerts.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending exact `origin/main`
- local regression checks: `80 passed` across Yandex storage, VK source and
  static-site release suites; `git diff --check` clean
- post-deploy verification: Object Storage canary passed; strict CDN recovery
  is 10/10 TLS and 20/20 GET. Deployment, backlog catch-up and public repair
  remain in progress; authenticated VK readback passed for eight repaired
  latest posts, with `10411` retained as the explicit provider-rejected
  exception.

## Prevention

Storage provider failures can no longer be silent, known-but-unmaterialized
media cannot degrade a new VK event post to text-only, and future candidate
uploads prune only safely superseded immutable trees. Closure remains blocked
on deployment, Telegram publication catch-up, and the explicit `10411` media
exception.
