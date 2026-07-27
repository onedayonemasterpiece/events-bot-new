# R15-L04-ASTRO results

## Lane contract

- **Lane ID:** R15-L04-ASTRO
- **Requirements:** R01, R02, R05, R07, R10 (Astro/UI portions only)
- **Base SHA:** `31b72b93153c094ca16cd564bfdc6b56c2031867`
- **Validated implementation head:** `16e6d0c5d1b9e1fbfd8631dca4bfb9e7029eae07`
- **Final lane tip:** the immediate child of the validated implementation head containing this report; final SHA is reported in the integration handoff.
- **Writable scope used:** mobile rail presentation/tests; Free materialized collection surface; date availability/calendar; Reference4 menu/header/footer navigation; `/neobychnoe/` feed/runtime; local SVG Repo assets and attribution; lane tests and this result record.
- **Forbidden scope preserved:** no builder/kernel/runner, Supabase migration/favorites/home, canonical `docs/`, or `CHANGELOG.md` changes.

## Delivered

### R01 — mobile listing media

- Preserved the accepted exact `140×112` (`5:4`) donor frame for lone visual-only photos.
- A card now receives `cover` only when both the selected asset and event-level semantic marker explicitly say `visual_only`; contradictory OCR/unknown event semantics fail closed.
- Added rendered fit/ratio diagnostics for deterministic browser acceptance.

### R02 — materialized Free collection

- Added a dedicated Free collection surface while retaining the existing materialized collection data source and shared `EventCard`.
- The first screen owns a large right-hand Free medal on desktop and mobile.
- A compact sticky collection shelf follows it at `top:57px` desktop / `top:64px` mobile without colliding with the global header.

### R05 — event-backed calendar inventory

- Added one inventory builder shared by date route generation and the mobile date UI.
- Multi-day events expose every covered date.
- Calendar months continue through the final day of the furthest active event month.
- Empty dates remain visible as disabled `<span>` cells and never receive an anchor or generated `/date-*` route.
- Added bounded previous/next month controls and explicit horizon diagnostics.

### R07 — Collections menu

- Free stays the first top-level item.
- Children moved into a new Collections plane alongside Unusual, Free, and Clubs.
- Added matching Phosphor Thin icons from SVG Repo, exact metadata, license links, and visual-selection rationale.
- Favorites now routes to the materialized `/izbrannoe/` destination.

### R10 — Unusual feed and unread semantics

- Added noindex `/neobychnoe/`, rendered only from a strict unusual manifest and the shared `EventCard`.
- Fail-closed on missing/invalid/shadow/migration/failed manifests; checked-in fallback is `unavailable` with zero items.
- Approved feeds deduplicate concept and event identity and accept only active, current trusted card snapshots/catalog rows.
- Red dots appear only for unseen post-baseline `core_unusual` items with `notify_eligible=true` and a valid `first_published_at`.
- Device state is bounded (`256` identities), TTL-limited (`180` days), and clears only after a card is actually at least 60% visible for 900 ms or through the explicit mark-seen control.
- Runtime waits for complete DOM parsing so desktop, mobile-menu, page-control, and footer dots stay synchronized.

## Validation evidence

### Automated tests

Passed focused lane suite:

```text
node --experimental-strip-types --test \
  tests/mobile-listing-rail-media.test.mjs \
  tests/mobile-listing-rails.test.mjs \
  tests/event-date-availability.test.mjs \
  tests/free-collection-surface.test.mjs \
  tests/reference4-collections-menu.test.mjs \
  tests/mobile-shell-toast.test.mjs \
  tests/unusual-events.test.mjs
# 30 passed, 0 failed
```

Passed the broad source-only site suite after excluding tests that explicitly require generated `dist/` or artifact environment:

```text
node --experimental-strip-types --test <all site tests except
  artifact-generated, event-detail-runtime-regressions,
  interest-club-catalog.browser, personal-feed-surface>
# 200 passed, 0 failed
```

The initial unfiltered run was `222/231`: seven failures required absent generated `dist/`/artifact inputs, one was the pre-existing medallion inventory count (`28 !== 27`), and one old menu assertion expected Children at top level. The menu assertion was updated to the R07 contract and then passed in both suites.

Other checks:

```text
git diff --check
# passed

GET /neobychnoe/                         200
GET /podborki/besplatnye-sobytiya/       200
GET /segodnya/                           200
GET /date-2026-08-08/                    200
```

Astro dev compiled and served all four routes without server errors. A full `npm run build` compiled entrypoints, then the shared host exhausted disk (`ENOSPC`) during static generation at `/dlya-menya/`; no generated output was committed.

### Browser acceptance (Chromium, 390×844 plus desktop)

- Free mobile: right medal `96×96`, compact shelf `top=64`, horizontal overflow `0`.
- Free desktop: right medal `300×300`, sticky shelf `68px` high, horizontal overflow `0`.
- Calendar: furthest event `2027-04-23`, horizon `2027-04-30`, last month reachable, 32 disabled cells in captured final month, disabled anchors `0`.
- Pianissimo canary `5296`: measured `140×112`, ratio `1.25`, `object-fit:cover`, transparent loaded frame, no horizontal/vertical band.
- Collections plane: all five rows fit, focus/plane transition settled, overflow `0`.
- Unusual fallback: status `unavailable`, zero cards, zero dots.
- Temporary approved-manifest acceptance: unread counts were `1` in mobile, desktop, and footer; visible mobile menu dot rendered; after 900 ms/60% card dwell, local state stored exactly the concept identity and every dot changed to hidden/count `0`. The source fallback was restored and revalidated afterward.

Screenshots (ignored runtime artifacts, not committed):

- `artifacts/codex/R15-L04-ASTRO/r15-pianissimo-rail-mobile.png`
- `artifacts/codex/R15-L04-ASTRO/r15-free-mobile-final.png`
- `artifacts/codex/R15-L04-ASTRO/r15-free-desktop.png`
- `artifacts/codex/R15-L04-ASTRO/r15-calendar-last-month-mobile.png`
- `artifacts/codex/R15-L04-ASTRO/r15-collections-menu-mobile-final.png`
- `artifacts/codex/R15-L04-ASTRO/r15-unusual-fallback-mobile-final.png`
- `artifacts/codex/R15-L04-ASTRO/r15-unusual-dot-menu.png`
- `artifacts/codex/R15-L04-ASTRO/r15-unusual-approved-mobile-final.png`

## Changed files

- `.codex/lanes/R15-L04-ASTRO/RESULTS.md`
- `site/public/assets/icons/reference4-v8/ATTRIBUTION.md`
- `site/public/assets/icons/reference4-v8/sparkle-thin.svg`
- `site/public/assets/icons/reference4-v8/sparkle-thin.svg.metadata.json`
- `site/public/assets/icons/reference4-v8/squares-four-thin.svg`
- `site/public/assets/icons/reference4-v8/squares-four-thin.svg.metadata.json`
- `site/src/components/FreeCollectionSurface.astro`
- `site/src/components/Reference4MobileMenu.astro`
- `site/src/components/SiteFooter.astro`
- `site/src/components/UnusualListingSurface.astro`
- `site/src/components/UnusualUnreadRuntime.astro`
- `site/src/components/listings/MobileDateAccessory.astro`
- `site/src/components/listings/MobileListingRailRow.astro`
- `site/src/components/listings/MobileListingRailSurface.astro`
- `site/src/data/unusual-events.json`
- `site/src/layouts/EventLayout.astro`
- `site/src/lib/eventDateAvailability.ts`
- `site/src/lib/mobileListingRailMedia.mjs`
- `site/src/lib/types.ts`
- `site/src/lib/unusualEvents.ts`
- `site/src/lib/unusualManifest.mjs`
- `site/src/pages/date-[date].astro`
- `site/src/pages/neobychnoe/index.astro`
- `site/src/pages/podborki/[slug]/index.astro`
- `site/tests/event-date-availability.test.mjs`
- `site/tests/free-collection-surface.test.mjs`
- `site/tests/mobile-listing-rail-media.test.mjs`
- `site/tests/mobile-listing-rails.test.mjs`
- `site/tests/mobile-shell-toast.test.mjs`
- `site/tests/reference4-collections-menu.test.mjs`
- `site/tests/unusual-events.test.mjs`

## Integration risks / gates

1. R10 intentionally remains noindex and empty until the builder writes an approved manifest. The builder contract now supplies `rollout_baseline_at`; migration items remain non-notifying.
2. Integration must run a full build/check on a host with sufficient disk and run the generated-output browser gates.
3. Canonical feature docs and `CHANGELOG.md` belong to the docs lane and remain an integration completion gate.
4. `artifacts/codex/R15-L04-ASTRO/` is intentionally ignored and must not be committed.
