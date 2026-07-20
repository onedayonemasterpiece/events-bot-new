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
- 2026-07-20 — merge `82fb5ba1` reached `origin/main` and Fly release `v1719`
  deployed the first exact-pixel repair.
- 2026-07-20 10:06–10:09 UTC — bounded event `6956` canary exposed a second
  convergence defect: source rehydrate repeatedly weak-URL-merged a different
  exact rendition into the classified row and caused eight duplicate
  `event_media_role` calls, alternating the intended KEY4/KEY5 pool.
- 2026-07-20 10:09 UTC — only the `event_media_review:6956` follow-up was
  deferred; no mass backfill was running.
- 2026-07-20 10:04 UTC — StaticSiteBuilder built all Astro pages but its stale
  desktop post-build check rejected 372 safe `contain` decisions as legacy
  `expected cover`; only that retry was deferred while the contract was fixed.
- 2026-07-20 10:38–10:57 UTC — deployed convergence retry built 248 pages,
  passed both production and secret-candidate contracts and published 935
  immutable review objects after the stale resource lease was reconciled.
- 2026-07-20 11:00 UTC — bounded event `6956` canary created current geometry
  `566` for poster `14758` with exact poster/geometry pixel-hash equality in one
  KEY4 Gemma call. Its automatic source follow-up then exposed a final edge:
  the source-level poster hash stayed stable while exact encoded bytes changed,
  causing the per-event poster-hash unique constraint to reject a new row.
- 2026-07-20 11:25 UTC — Fly `v1722` on main SHA `ae2336cb` converged that
  stable-source/new-exact follow-up without a uniqueness error and preserved
  approved poster `14758` plus geometry `566`.
- 2026-07-20 11:46 UTC — a deliberately early no-op media pass exposed that a
  future `deferred` pair remained in the ledger but its durable next-day job
  was not re-armed. No provider call was made; the pair stayed quarantined.

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
7. The identity index exposed weak source URLs for rows that already owned an
   exact-v2 object, and source/candidate hash lookup ran before exact served-byte
   identity. Reconciliation could therefore overwrite and invalidate the same
   classified row on every pass.
8. The static production desktop check still inferred `cover` from
   `visual_only`, contradicting the new protected-region rule that responsive
   unknown-aspect surfaces must fail closed to `contain`.
9. The exact-first reconciliation path allowed a new exact rendition to be
   inserted under its unchanged source-level `poster_hash`. The schema correctly
   rejected the duplicate `(event_id, poster_hash)`, so the follow-up could not
   converge even though the approved poster and its new geometry stayed intact.
10. In the no-due-pair branch, future semantic-role retries were re-armed but
    future pair-review retries were not. An unrelated or operator-triggered
    early media job could therefore consume the only durable wake-up while the
    pair remained correctly `deferred` in quarantine.

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
- The single `6956` media follow-up and the failed static-build retry are
  deferred until the convergence hotfix is deployed; unrelated jobs continue.

## Corrective Actions

- [x] Make new managed poster writes immutable by encoded-content identity.
- [x] Invalidate/reselect geometry and semantic evidence on display/pixel drift.
- [x] Add normal-pool role rotation and bounded transient retries.
- [x] Enqueue after pair reconciliation and include geometry-only stale rows.
- [x] Export pixel-current face/value metadata with provenance.
- [x] Consume it through deterministic safe-cover/contain decisions.
- [x] Make exact identity outrank source identity and restrict weak-URL merge to
  legacy rows so repeated source reconciliation converges.
- [x] Align the production desktop build contract with protected-crop
  `cover`/reason evidence and fail-closed responsive `contain`.
- [x] Namespace an unchanged mutable source hash by exact encoded digest when it
  yields a new rendition, preserving old visual evidence and DB uniqueness.
- [x] Re-arm the earliest future deferred pair after any early/no-op media pass,
  without an early provider call or duplicate job.

## Follow-up Actions

- [ ] Add current-versus-linked geometry coverage counters to routine operations.
- [ ] Run historical backfill in paced reviewed batches after canary acceptance.

## Release And Closure Evidence

- first deployed SHA: `82fb5ba12dfc1181a358044cc060c19d441378dd`, Fly
  release `v1719`; convergence SHA `66b4f129719c02c90420e6c56801f7fa65509bf5`,
  Fly release `v1720`; stable-source/exact uniqueness SHA
  `ae2336cb6e6b2518213c702db7a3ced92dc2434a`, Fly release `v1722`; deferred
  pair re-arm follow-up pending
- deploy path: clean `origin/main` Fly deploy; deferred-pair re-arm follow-up pending
- source regression checks: 146 Python tests and 20 focused Node tests passed;
  Astro built 380 pages. The broad Node suite remained 43/44 because of a
  pre-existing literal class-token assertion in an unchanged layout file.
- convergence regression: 55 focused Python tests passed; a full local
  production-profile export/build from the retained failed snapshot passed the
  corrected desktop post-build contract with zero provider calls.
- post-deploy static verification: build
  `production-secret-20260720T123645-19876d03`, 248 events, 935 published
  objects, production and secret-candidate checks all `ok`; final exact-SHA
  build `production-secret-20260720T132607-9e4818dd` published 936 verified
  objects with repo SHA `ae2336cb6e6b2518213c702db7a3ced92dc2434a`

## Prevention

The contract is exact-pixel-first: managed object identity, geometry cache,
semantic evidence, exported provenance and crop decisions must all refer to the
same pixels. Perceptual hashes remain duplicate evidence, never mutable object
identity or geometry cache keys.
