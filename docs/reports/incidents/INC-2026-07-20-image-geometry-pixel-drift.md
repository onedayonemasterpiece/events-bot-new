# INC-2026-07-20 Image geometry pixel drift and incomplete consumption

Status: open
Severity: sev2
Service: Smart Update event media / static event pages
Opened: 2026-07-20
Closed: —
Owners: events bot operator / event-media and static-site maintainers
Related incidents: `INC-2026-07-18-vk-captcha-publication-cadence-gap`, `INC-2026-07-17-vk-auto-provider-quota-false-reject`, `INC-2026-06-03-smart-update-flash-lite-rpd`, `INC-2026-07-16-static-event-media-action-regressions`
Related docs: `docs/features/event-media/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/static-site-pages/image-framing.md`, `docs/operations/runtime-logs.md`

## Summary

The deployed Smart Update media worker did enqueue and accumulate image-geometry
records, but managed poster objects were mutable: different encoded pixels could
overwrite the same perceptual-dHash object key. `EventPoster.image_geometry_id`
then remained attached because current-geometry lookup and candidate selection
did not compare exact pixel hashes. Static export also omitted the stored
face/value boxes, so downstream renderers could neither use the enrichment nor
prove that a crop referred to the current pixels.

## User / Business Impact

- Stored face/value coordinates and semantic role/crop evidence could refer to
  older pixels while appearing linked to the current approved poster.
- With one-year immutable cache headers, different CDN caches could show
  different pixel generations for one supposedly content-addressed URL.
- Missing, transiently failed and stale rows did not reliably converge through
  the ordinary operational enqueue path.
- Static pages did not receive current geometry metadata and therefore could not
  protect faces or the viewer-value region in crop decisions.

## Detection

- Detected during a production audit of the 15-image VK/Smart Update cohort from
  the 2026-07-18 acceptance.
- Events `6954` and `6956` had current poster pixel hashes different from their
  linked `event_image_geometry.pixel_sha256`.
- The reported current cohort audit had 7/15 linked rows, 6/15 exact current
  pixel/model/prompt rows and 0/15 rows passing the complete semantic safe-crop
  gate. A separate targeted pre-deploy read confirmed event `6956` was still
  linked to geometry for older pixels.

## Timeline

- 2026-07-18 07:24 UTC — event `6956` geometry row `477` was generated and
  visually accepted against the then-current exact pixel hash.
- 2026-07-19 23:55 UTC — `event_media_review:6956` rehydrated source media and
  overwrote the same dHash object; live pixels changed while geometry stayed linked.
- 2026-07-20 00:37 UTC — the managed object involved in event `6954` was also
  overwritten during later materialization/rehydration.
- 2026-07-20 — audit localized the exact-pixel drift and opened this incident.

## Root Cause

1. Managed poster paths used perceptual dHash (`p/dh16/...`) rather than the
   SHA-256 of encoded bytes. Materialization performed unconditional PUTs to
   those paths, so similar/re-encoded images could mutate an existing URL.
2. `_current_geometry_for_poster()` and `_next_geometry_candidate_id()` did not
   require exact poster/geometry `pixel_sha256` equality.
3. Fingerprint and display-URL refresh paths did not invalidate geometry or
   image-dependent semantic role/focal/safe-crop evidence.
4. After the final pending pair became `distinct`/approved, no enrichment
   follow-up was guaranteed when no other pair remained.
5. Transient semantic provider failures were stored as terminal `error`, and
   the operational selector omitted geometry-only stale rows.
6. The static exporter emitted empty face boxes and no viewer-value region
   instead of joining pixel-current `event_image_geometry` rows.

## Contributing Factors

- Coverage monitoring counted links rather than exact current pixel provenance.
- Materialization recorded the source-byte digest while its URL served encoded
  WebP bytes, weakening raw-identity evidence.
- Existing renderer gates had no deterministic consumer for reusable geometry.

## Automation Contract

### Treat as regression guard when

- changing event-poster materialization, managed object paths or fingerprints;
- changing Smart Update media enqueue/candidate/retry logic;
- changing `event_image_geometry`, static media export or cover/contain routing.

### Affected surfaces

- `event_media.py`, Smart Update poster merge and legacy CDN backfill paths;
- Kaggle `TelegramMonitor` poster writer and Telegram payload ingestion;
- `scripts/enqueue_static_event_media_enrichment.py`;
- production `eventposter`, `event_image_geometry`, `event_media_review_usage`
  and `joboutbox` rows;
- `site/scripts/export-production-preview-data.py` and event image renderers;
- Yandex Object Storage `/p/**` managed media keys and CDN responses.

### Mandatory checks before closure or deploy

- Two distinct encoded posters with the same perceptual dHash never overwrite
  one managed object key.
- A display-URL or normalized-pixel change invalidates old geometry and
  image-dependent semantic evidence; current lookup and candidate selection
  reject every pixel mismatch.
- A new single approved poster and a final pair-reconciled approved poster both
  schedule the ordinary durable enrichment job and can reach current geometry.
- Transient role `429`/RPM/RPD/timeout uses only the configured normal key pool,
  schedules a bounded delayed retry and never becomes a tight loop or fallback.
- Export includes normalized boxes only for an exact current classified row;
  missing/stale rows fail closed.
- Crop solver tests prove protected-region containment when feasible and
  `contain` fallback when it is not.
- Replay the `INC-2026-07-17` positive and negative VK controls and retain its
  provider/key-rotation checks.
- Production canary is limited and paced; inspect DB hashes, logs and visual
  overlays before any larger backfill.
- `/healthz`, Fly checks, SQLite `quick_check`, runtime mirror and static build
  checks pass; deployed SHA is reachable from `origin/main`.

### Required evidence

- targeted backend/static tests and source replay output;
- pre/post production DB rows for canary poster/geometry/job;
- minimal runtime-log excerpts and visual overlay inspection;
- PR/main merge SHA, Fly release/deployed SHA and static build receipt.

## Immediate Mitigation

- Mass backfill is paused until exact object/pixel identity is fixed.
- Production inspection remains read-only except for an explicitly bounded,
  delayed canary after deploy.

## Corrective Actions

- [x] Make new managed poster writes immutable by encoded-content identity.
- [x] Invalidate/reselect geometry and semantic evidence on display/pixel drift.
- [x] Add normal-pool role rotation and bounded transient retries.
- [x] Enqueue after pair reconciliation and include geometry-only stale rows.
- [x] Export pixel-current face/value metadata with provenance.
- [x] Consume it through deterministic safe-cover/contain decisions.

## Follow-up Actions

- [ ] Add current-versus-linked geometry coverage counters to routine operations.
- [ ] Run historical backfill in paced reviewed batches after canary acceptance.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- source regression checks: 146 Python tests and 20 focused Node tests passed;
  Astro built 380 pages. The broad Node suite remained 43/44 because of a
  pre-existing literal class-token assertion in an unchanged layout file.
- post-deploy verification: pending

## Prevention

The contract is exact-pixel-first: managed object identity, geometry cache,
semantic evidence, exported provenance and crop decisions must all refer to the
same pixels. Perceptual hashes remain duplicate evidence, never mutable object
identity or geometry cache keys.
