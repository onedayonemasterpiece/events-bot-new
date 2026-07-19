# Lane content_media results

- Status: **Complete**
- Requirements: **R02, R07, R08**
- Base SHA: `2fda48d8ba1fb8cda13878a2e9fb726c984eb0f3`
- Implementation head SHA: `8f93e2c8610ed0caaf09d86d7a1084a6cf7feee1`
- Branch: `agent/static-event-v12/content-media`
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/static-event-v12-content-media`

## Outcome

### R02 — evidence-aware medallions

- Replaced the description-wide substring resolver with a typed resolver that returns the selected identity plus explicit `venue_name`, `festival`, `source_url`, or curated-festival evidence.
- Added Unicode letter/number boundaries for curated aliases, so short `ММО` cannot match inside `программой`.
- Assigned explicit `venue_brand`, `festival_brand`, and `organizer` roles in the manifest and made the resolver emit at most one venue brand. Equal-strength venue identities fail closed.
- Added a narrow structured contradiction gate: multiple `/event/<id>` identities on the same first-party source host fail closed. Event `5295` therefore emits no venue/organizer identity medallion.
- Preserved true structured matches: an exact `venue_name=ММО` resolves the World Ocean Museum, while event `6796` resolves only KAUP from its structured venue.
- Runtime HTML exposes evidence/fail-closed data attributes for audit without exposing source prose.

Ignored read-only evidence artifact:

- `artifacts/codex/static-event-v12-content-media/identity-contamination-5295.json`
- Snapshot: `/home/dev/projects/events-bot-new/artifacts/db/static-site-prod-20260715.sqlite`, opened with SQLite `mode=ro` + `query_only=on`.
- Finding: Tretyakov first-party source identities `event/46315` (Mateo, 20:00) and `event/47686` (Цареубийца, 16:30) coexist on event `5295`.

### R07 — typed presentation fallback art

- Added a typed fallback resolver/component and wired both `EventHero` and `DesktopEventPage`.
- Concert/symphonic types use `concert-symphonic.webp`; lecture/meeting types use `lecture-meeting.webp`.
- Fallback art is a presentation-only DOM image. It is never appended to `image_assets`, galleries, OG/head metadata, JSON-LD, or share media.
- Empty desktop `<img src="">` is replaced by typed art or the existing generic non-image surface.
- Both optimized assets are deterministic 1280×1280 WebP, rendered with responsive `contain` on a dark backing surface.

Source provenance and hashes:

| Runtime asset | Source path (dirty root, read/copied only) | Source SHA-256 | Runtime SHA-256 |
| --- | --- | --- | --- |
| `site/public/assets/event-fallbacks/concert-symphonic.webp` | `docs/features/static-site-pages/symphonic concert.png` | `444f5bcf5ad60eca24241c64c1ad5e13de33260c2785bcd4addd570075539bc4` | `2b16039d5a6ec12939e604327d5694a3aa975048ff608bde84c1759dc29b52d2` |
| `site/public/assets/event-fallbacks/lecture-meeting.webp` | `docs/features/static-site-pages/lecture (2).png` | `f5c6656ce100da6967d151e63e2044a63a8c168b34acdc6e7439e8afd9a01f24` | `3b43cfe2056174c6113b39cb89e7bc0083f8889ea39ba9e2e2fa51c0c93aa4d7` |

Visual QA: both optimized WebP assets were opened with `view_image`; linework, centering, contrast, and source composition remained intact.

### R08 — reusable no-image inventory

- Added executable read-only tool `site/scripts/audit-no-image-inventory.py`.
- It categorizes actionable exported no-image events as `no_ledger`, `no_approved`, `approved_non_cdn`, or `projection_mismatch`, grouped by normalized event type.
- Report freshness includes DB mtime, max event update when available, preview generation time, chosen freshness reference, and age in hours.
- The inventory excludes already-past/inactive projections from the actionable total but reports their count.

Baseline artifact (ignored):

- `artifacts/codex/static-event-v12-content-media/no-image-inventory.json`
- As-of: `2026-07-18T12:00:00Z`
- Freshness reference: preview generated `2026-07-17T09:42:49.800821Z`; age `26.286h`.
- Actionable no-image total: **3** — `5663`, `6774`, `6890`.
- Reasons: **3 no_ledger**, **0 no_approved**, **0 approved_non_cdn**, **0 projection_mismatch**.
- Types: **2 concert**, **1 meeting** (`концерт: 2`, `встреча: 1`).
- One stale past projection (`6908`, 2026-07-17) was disclosed as `past_or_inactive_projection_count=1`, not mixed into the current baseline.

## Validation

Commands and results:

1. `npm --prefix site run test:content-media`
   - **6/6 passed**: Unicode alias boundary; 6796 KAUP/no-MMO; true MMO; 5295 fail-closed; one-venue ceiling; typed fallback mapping.
2. `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_static_no_image_inventory.py`
   - **2/2 passed**: all four reasons/type/freshness; byte-identical read-only DB behavior.
3. `PREVIEW_BUILD_ID=preview-content-media-lane npm --prefix site run build:preview`
   - **Passed**: `377 page(s)` in `2m38s`; output `site/dist/preview-content-media-lane/`.
4. `PREVIEW_BUILD_ID=preview-content-media-lane npm --prefix site run check:preview`
   - **Passed**: `303 events`, `strict_related=false`.
5. Focused generated-HTML assertions (`artifacts/codex/static-event-v12-content-media/focused-html-check.log`)
   - **16/16 passed**.
   - Events `6774`/`6890`: exactly two fallback references (desktop/mobile), none in head/OG/JSON-LD, gallery, or share media, and no empty image source.
   - Event `6796`: KAUP evidence present, MMO absent.
   - Event `5295`: conflict fail-closed marker present, Tretyakov venue medallion absent.
6. `python3 -m py_compile site/scripts/audit-no-image-inventory.py`
   - Passed.
7. `node --experimental-strip-types --check site/src/lib/eventMedallions.ts` and `...eventFallbackArt.ts`
   - Passed.
8. `git diff --check`
   - Passed.

Two discarded validation attempts are not acceptance evidence: an accidental pair of concurrent Astro builds collided in `dist/.prerender`, and the next foreground runner received SIGTERM from the command-session wrapper. After checking the official Astro static prerender/build concurrency contract and ensuring only one build process, the isolated background preview build above completed successfully and `check:preview` passed.

## Changed files

- `site/package.json`
- `site/public/assets/event-fallbacks/concert-symphonic.webp`
- `site/public/assets/event-fallbacks/lecture-meeting.webp`
- `site/scripts/audit-no-image-inventory.py`
- `site/scripts/content-media.behavior.test.mjs`
- `site/src/components/DesktopEventPage.astro`
- `site/src/components/EventFallbackArt.astro`
- `site/src/components/EventHero.astro`
- `site/src/components/EventTokenMedallions.astro`
- `site/src/data/organizerMedallions.json`
- `site/src/lib/eventFallbackArt.ts`
- `site/src/lib/eventMedallions.ts`
- `tests/test_static_no_image_inventory.py`
- `.codex/lanes/content_media/RESULTS.md`

## Risks / merge notes

- The contradiction gate is intentionally narrow and evidence-based: it detects conflicting structured ticket IDs on one host. Other semantic source contradictions still require an upstream typed identity-quality marker rather than renderer keyword heuristics.
- Resolver safety deliberately no longer treats arbitrary description/summary mentions as organizer identity. This can reduce a medallion when structured venue/festival/source evidence is absent; it avoids silently asserting a false identity.
- Canonical documentation and `CHANGELOG.md` were forbidden in this lane. The integrator must update the canonical static-page/medallion documentation and `[Unreleased]` before delivery.
- No transport, CTA, footer, build-core, canonical docs, `CHANGELOG.md`, root source asset, or production DB file was modified.
- Do not commit ignored `artifacts/` or generated `site/dist/` during integration.
