# R14-ARTIFACTS results

Date: 2026-07-27
Branch: `agent/static-site-review-r14/artifacts`
Base: `c9a710c8`

## Requirements

| Requirement | Status | Evidence |
|---|---|---|
| R10 — random eligible real event | Done for explicit immutable research mode | Canonical current `/vyhodnye/` selects exactly one deduplicated real Saturday/Sunday event by immutable build seed. Adjacent weekend routes do not reroll. Production/root remains fail-closed even if the flag is present. |
| R11 — artifact collection | Done for explicit immutable research mode | `/artefakty/` has five finite slots, local found state, empty hints, accessible detail dialog and disabled exact `Поделиться артефактом · скоро`. Found rail echo opens the collection story. |

## Implementation

- `site/src/lib/artifacts.mjs`
  - deterministic FNV-1a assignment over sorted eligible ids;
  - explicit preview/secret-candidate research gate and production exclusion;
  - versioned `ke_artifact_collection_v1` local schema;
  - idempotent collection plus migration from
    `ke_amber_artifact_prototype_v1:tail`.
- `site/src/pages/vyhodnye/index.astro`
  - owns the only current-weekend assignment using
    `PUBLIC_STATIC_RELEASE_ID || PREVIEW_BUILD_ID`.
- `site/src/components/listings/WeekendListingSurface.astro`
  - accepts the selected id without hardcoded event/fallback logic.
- `site/src/components/listings/AmberRailArtifact.astro`
  - preserves accepted tail geometry/order and reduced motion;
  - records the structured local find and opens the story after collection.
- `site/src/components/artifacts/ArtifactCollection.astro`,
  `site/src/pages/artefakty/index.astro`
  - local-only disclosure, finite found/empty slots, labelled native dialog,
    focus return and disabled coming-soon share;
  - neutral unavailable route when the research gate is off or mode is
    production.

No SQLite, Supabase, auth, profile, raw URL/referrer, pointer coordinate or
swipe path is read or written by the artifact feature. Its CustomEvent remains
local and idempotent.

## Verification

- Focused Node/source suite:
  - `node --test site/tests/artifacts.test.mjs site/tests/mobile-listing-rails.test.mjs site/tests/amber-artifact.test.mjs`
  - `17/17` pass.
- Flagged immutable preview:
  - build `preview-20260727-r14-artifacts-v2`;
  - `433` pages;
  - `npm --prefix site run check:preview`: pass, `288` real events.
- Generated-output:
  - `ARTIFACT_GENERATED_ROOT=.../preview-20260727-r14-artifacts-v2 node --test site/tests/artifact-generated.test.mjs`
  - `2/2` pass; one canonical artifact and zero adjacent-weekend artifacts.
- Playwright against generated output:
  - `ARTIFACT_BASE_URL=http://127.0.0.1:4179/preview-20260727-r14-artifacts-v2 node site/tests/artifact-collection.playwright.mjs`
  - pass at `390×844`, reduced motion, persistence/reload/detail/dialog/share,
    legacy migration and no persistence/API request.
- Production fail-closed probe:
  - `PUBLIC_SITE_MODE=production` with the research flag deliberately present;
  - `/vyhodnye/`: zero artifact controls;
  - `/artefakty/`: neutral unavailable state, no collection UI.
- `git diff --check`: pass.

Build logs are uncommitted under `artifacts/codex/r14-artifacts/`.

## Release blocker

The three supplied/derived amber WebPs still have no committed source
provenance or redistribution/derivative-rights record. This lane deliberately
does not enable the feature in ordinary production/root. A noindex candidate
review does not clear that legal gate. Low-end Android scroll/tap/FPS acceptance
also remains manual.

## Integration notes

- This lane intentionally did not touch `dlya-menya`,
  `Reference4MobileMenu.astro`, `EventLayout.astro`,
  `MobileListingRailRow.astro`, shared docs, or `CHANGELOG.md`.
- Integrator owns the required `CHANGELOG.md` entry and any later navigation
  entry after resolving Search/Auth/Collections shared-file changes.
