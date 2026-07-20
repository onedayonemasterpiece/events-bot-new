# Static geometry lane results

## Scope

- Lane: `static`
- Requirements: `R04`, `R05`
- Base SHA: `c587a0cf86e144a88c0457035866c8325ea59dc5`
- Implementation SHA: `621d6f8ec1797425f96c52ffced1ead129e51c45`

## Requirement status

- **R04 — Done.** The production preview exporter left-joins geometry only for a
  classified record whose pixel hash equals the current `EventPoster` hash. It
  additionally requires the configured expected model (default
  `gemma-4-31b-it`) and prompt `event-image-geometry-v1`. It exports normalized
  face boxes, the valuable region and model/prompt/pixel provenance. Missing,
  stale, obsolete-contract and pre-migration snapshots fail closed.
- **R05 — Done, deliberately narrow.** `imageCrop.mjs` computes a deterministic
  normalized cover window that contains the union of faces and valuable region
  plus margin, or returns `contain`. It also gates on semantic role, current
  pixel provenance and crop safety. Cover is invoked only for desktop related
  cards where the renderer has the exact resolved target aspect. Responsive
  hero/listing/desktop surfaces do not guess a ratio and remain `contain` until
  a target-size-aware consumer is added.

## Evidence

- `/home/dev/.venvs/events-bot-image-geometry/bin/pytest -q tests/test_static_site_public_gate.py`
  — `12 passed`.
- `node --test site/tests/image-crop.test.mjs site/tests/desktop-event-cta.test.mjs site/tests/desktop-editorial-motion.test.mjs site/tests/event-media-quality.test.mjs`
  — `19 passed`.
- `npm run build` (with the existing root `site/node_modules` temporarily made
  available to this isolated worktree) — `380 page(s) built`, success.
- Generated event `5658` contained `data-hero-render-fit="contain"` and only
  fail-closed protected-crop reasons on responsive surfaces.
- `git diff --check` — passed.
- Broad `node --test site/tests/*.test.mjs` — `42/43`; the one failure is an
  existing origin/main test/source inconsistency: the test expects literal
  `event-card__media-shell--dynamic is-image-loading` inside
  `EventLayout.astro`, while origin/main already stores those class tokens as
  separate array entries. The focused tests and full Astro build are green.

## Changed files

- `site/scripts/export-production-preview-data.py`
- `site/src/lib/types.ts`
- `site/src/lib/imageCrop.mjs`
- `site/src/components/EventHero.astro`
- `site/src/components/EventCard.astro`
- `site/src/components/EventListItem.astro`
- `site/src/components/DesktopEventPage.astro`
- `site/tests/image-crop.test.mjs`
- `tests/test_static_site_public_gate.py`

## Risks / integration notes

- This intentionally supersedes the `INC-2026-07-16` acceptance clause that
  allowed pending `visual_only` media to cover. Root integration must update
  the canonical incident/docs/CHANGELOG and explicitly record that new exact-
  geometry contract.
- Responsive surfaces will visibly letterbox until current geometry is present
  **and** a client-size/range-aware solver is introduced. This is intentional:
  a server-side approximate aspect ratio cannot guarantee face/value retention.
- No backend, docs or CHANGELOG files were edited in this lane.

