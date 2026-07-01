# Requirements: Anonymous Personalization for Static Event Pages

Status: draft

## Product intent

- Anonymous personalization enhances already rendered static event pages without becoming a prerequisite for SEO/GEO HTML, the primary event content, or CTA availability.
- The MVP-0 surface is the event detail page block `event_detail_related` (“Похожие события”): static fallback first, then local anonymous rerank/filter after consent.
- The related block must stay useful and safe when recommendations are stale, when JavaScript/localStorage/telemetry is unavailable, or when Supabase/API is unavailable.

## Requirements

### Event detail related fallback and personalization

- Static event pages must render a useful “Похожие события” fallback list before any personalization code runs.
- After consent, the frontend may locally rerank/filter the already available related candidates using the anonymous local profile, but the current page context remains dominant.
- Frontend personalization must never require an online LLM call, a Supabase read by `anon_id`, or telemetry availability in the page-view hot path.

### Related candidate freshness and expiry

- The initially rendered related list and/or same-origin related manifest must be generated only from future active canonical events at build/export time.
- The static candidate generator must exclude the current event, past/ended events, cancelled/postponed/duplicate/merged events, and main-block duplicates for other dates of the same event; sold-out events follow the documented product rule (downrank or explicit handling), not silent removal by accident.
- Static event pages and related manifests must include freshness/invalidation metadata such as `generated_at`/`built_at`, stable `event_id`/slug, and `source_hash`/`content_hash` so stale recommendation payloads can be detected and replaced.
- The export/rebuild path must update affected event pages or their related manifests when a related candidate becomes stale because of date passage, event end, cancellation/postponement, merge/duplicate status, or ranking-relevant event changes.
- Time-based expiry must not depend on user telemetry: even if no visitor opens a page and Supabase is unavailable, ended events must age out through scheduled static export/manifest refresh.
- The browser-side rerank/filter must fail closed for stale candidates: before rendering a personalized order, it must hard-filter candidates whose date/end/lifecycle metadata says they are past or inactive, then preserve the static fallback/CTA if a fresher manifest cannot be loaded.

### Smart Update and data ownership boundary

- Personalization consumes accepted event facts and lifecycle/status fields from Fly SQLite/static export; it must not feed anonymous clicks, hides, or profile state back into Smart Update deduplication, extraction, or factual repair decisions.
- After Smart Update commits a canonical event change affecting ranking or lifecycle, it must schedule/update `static_event_export` and `event_feature_snapshot`/same-origin recommendation manifests.
- Supabase/Postgres is for compact personalization telemetry, debugging/eval, aggregates, and post-MVP ranker evidence; it is not the source of truth for event freshness.

### Verification evidence

- Acceptance checks must cover that current, cancelled, postponed/duplicate/merged, and past/ended events do not appear in `event_detail_related`, both in static fallback and after local rerank.
- Acceptance checks must cover stale-manifest behavior: a stale or unavailable personalization manifest must not break the main event page, CTA, or static fallback.
- Acceptance checks must keep mobile and desktop presentation separate (`layout_mode='module'`, with mobile vertical related and desktop grid/module behavior).

## Open questions

- No product/business questions from the 2026-06-26 stale-related-events intake remain open.

## Decisions log

- Initial draft created.
- 2026-06-26: resolved the pending intake about stale events in the initially rendered related list. No conflict found with the existing design; the new requirement tightens freshness/rebuild/client fail-closed behavior around `event_detail_related`.

## Intake 2026-06-26T15:30:09+00:00

Status: resolved/archived 2026-06-26

### User notes

Страницы событий содержат внизу блок-ленту с похожими, которая подвергается персонализированной фильтрации на фронте, так вот события в изначально отрегдеренном списке могут устаревать и заканчиваться, нужно продумать обновление страниц при устаревании

### Resolution

Integrated into canonical sections above as requirements for related-candidate freshness, static export/manifest invalidation, scheduled time-based expiry, and browser-side fail-closed filtering before local rerank.

### Source references checked

- `source/`: inspected during reconciliation; no source files were present in the feature `source/` directory.
- `docs/features/unsigned-personalization/README.md`
- `docs/features/unsigned-personalization/event-detail-related.md`
- `docs/features/unsigned-personalization/database.md`
- `docs/features/unsigned-personalization/smart-update-contract.md`
- `tests/e2e/features/static_site_personalization.feature`
- `static_site/personalization/personalization.js`

### Reconciliation checklist

- [x] Compare with previous requirements.
- [x] If user notes include automatic voice transcripts, treat them as noisy input: recover likely context but ask about uncertain fragments instead of guessing.
- [x] If there is a contradiction, ask which requirement wins: old, new, or another resolution.
- [x] Move resolved statements into the canonical sections above and remove/close this pending intake.
