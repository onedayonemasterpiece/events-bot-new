# Lane results: static-event-v13-recommendations

## Scope

- Lane: `static-event-v13-recommendations`
- Requirements: R07–R08
- Base SHA: `ecb6dc025be6fa3d55aea72fa104d223b9b7cb56`
- Head SHA: `HEAD` (the committed lane result on `agent/static-event-v13/recommendations`)

## Delivered

- Replaced divergent intrinsic/post-load related-card sizing with one shared `packRelatedCardRows` contract used by desktop `Смотрите дальше` and hydrated `Ещё события` cards.
- The shared classifier is `image_text_mode` based, packs stable three-card desktop rows, reserves the row aspect ratio before image load, and uses bounded cover or deterministic contain so document/OCR crop never exceeds 20%.
- Made the personal-feed cache current-event aware and added a six-hour, bounded recent-served ring.
- Event continuation excludes the current event, the already-rendered related block, recently served events, duplicates, and explicit `llm_rejected`/`gemma_reject` candidates.
- Added deterministic bounded interleaving of profile-ranked candidates with current-event vector/adjacent-tail candidates, then stable generic backfill, with hard caps of three per category and two per venue and a final maximum of six.
- Kept the mixed heading `Ещё события` even when profile signals are available; raw/not-run vector candidates are not presented as LLM-approved or as a separate “similar” block.

## Verification

Commands run:

- `node --test site/tests/event-continuation-contract.test.mjs` — passed (2/2).
- `node --test --test-name-pattern='personal feed keeps|event-detail continuation uses' site/tests/personal-feed-surface.test.mjs` — passed (2/2).
- `git diff --check` — passed.
- `npm run build` from `site/` — started with the shared project `node_modules`; Astro type generation and static-entrypoint collection passed, full large static render was still running at handoff time.

The focused geometry fixture loads production event `3934`, verifies two stable three-card rows, preserves order without dropping incompatible posters, and asserts actual document crop at or below 20%. The selection fixture verifies current/prior/recent/rejected exclusions, profile/vector-tail interleave, uniqueness, hard diversity caps, deterministic backfill, and raw-vector mixed-heading eligibility.

## Exporter provenance gap / fail-closed behavior

Explicit exporter-level `verification_state` was intentionally not added in this bounded lane to avoid expanding into the large related-chain cache/export pipeline during the integration deadline. Exact remaining gap: current `preview-related.json` chain entries do not distinguish raw/not-run pairs with an explicit field. The runtime normalizer therefore treats absent provenance as `not_run`, never as LLM-approved; it rejects explicit `verification_state=llm_rejected` and `gemma_reject=true`. Existing strict Gemma export already removes rejected/low-score pairs before public manifests are built. Adding exporter provenance and a cache-schema bump remains a follow-up.

## Changed files

- `site/src/lib/relatedCardLayout.mjs`
- `site/src/lib/eventContinuation.mjs`
- `site/src/components/DesktopEventPage.astro`
- `site/src/components/EventCard.astro`
- `site/src/components/PersonalFeedSlot.astro`
- `site/src/layouts/EventLayout.astro`
- `site/src/pages/sobytiya/[slug].astro`
- `site/tests/event-continuation-contract.test.mjs`
- `site/tests/personal-feed-surface.test.mjs`
- `.codex/lanes/static-event-v13-recommendations/RESULTS.md`

## Risks

- Full static build completion and browser-level visual acceptance are owned by integration; the focused contract tests cover the selection and geometry logic directly.
- Old cached/public manifests lack explicit pair provenance; absence is treated as `not_run`, not approval.
