---
name: static-event-medallions
description: Research, source, render, merge, and verify KenigEvents static-site event medallions/badges. Use in events-bot-new when adding or changing organizer/source/festival/program medallions, Bach/organ/Bahosluzhenie-style medallions, Pushkin-card badges, vector/SVG medallions, or when asked to inspect git history/parallel branches for medallion work before static generation.
---

# Static Event Medallions

Use this skill for KenigEvents static event-page medallions in `events-bot-new`.

## Non-negotiable workflow

1. **Start with git archaeology.** Before creating or redrawing a medallion, search current branch, `origin/main`, recently active branches, and local worktrees for prior medallion assets/commits.
2. **Prefer existing accepted assets.** If a medallion already exists in another branch, merge/cherry-pick/copy it with provenance rather than recreating it.
3. **Source-first, SVG-first.** Use official/local source assets. Prefer SVG/vector runtime when source is SVG or the mark is simple/geometric and can be faithfully reconstructed. Use WebP-first raster only when no trustworthy vector path exists.
4. **Do not guess logos.** Unknown organizers can get neutral initials only after the normalized organizer/venue is known. Do not invent institutional marks.
5. **No paid image generation by default.** Do not use OpenAI image generation/editing unless the user explicitly consents in the current thread. Use official SVG, SVG Repo/local icon library, local vectorization, Inkscape/rsvg/sharp/Pillow, or deterministic SVG.
6. **Visual QA is required.** Render the final page/medallion, inspect it at desktop and mobile sizes, and update `site/scripts/check-preview.mjs` when the contract changes.
7. **Keep docs and changelog synced.** Update `docs/features/static-site-pages/event-token-medallions.md` and `CHANGELOG.md` for code/asset behavior changes.

## Git archaeology commands

Run targeted searches before editing:

```bash
git fetch origin --prune
git log --all --decorate --oneline --grep='medallion\|badge\|бах\|bach\|организ' -i --max-count=120
git branch -a --list '*medall*' '*badge*' '*static*'
git ls-tree -r --name-only HEAD | grep -Ei 'medallion|badge|organizer|festival|token'
git grep -n -i -E 'бахослуж|бах|bachos|bach|орган|organ|svg|vector' \
  $(git for-each-ref --format='%(refname)' refs/heads refs/remotes/origin) -- \
  site/src/data site/public site/src/assets docs/features/static-site-pages 2>/dev/null
```

Known medallion commits to inspect when present:

- `8404c3b2` — first organizer raster/WebP medallion avatars.
- `1d5a82cc` — organizer medallions served as SVG where possible.
- `fb2570dc` — partner/festival medallions and logo grid.
- `01a85a35` — KGD80/Act Opus/Znanie corrections.
- `aeb5f0d0` — organizer visual tuning.
- `1959dad5` — recenter brand medallion artwork.

Recently relevant branch names:

- `origin/agent/static-medallions-visual-tune-20260702`
- `origin/feature/static-medallion-svg-upgrade`
- `origin/recovery/static-site-smart-search-full-20260701`
- `origin/main`

Recovered 2026-07-03 festival medallion set (originally observed as uncommitted work in the dirty main checkout, not as a reachable committed branch) lives under `site/public/assets/festivals/`, `site/src/assets/festivals/source/`, and `site/src/data/festivalMedallions.json`. It includes `bahosluzhenie`, `simfoniya-vetra`, `kaliningrad-city-jazz`, `kaliningrad-street-food`, `grozd-festival`, `koroche`, `ostrova`, `more-vnutri`, `tolkin-fest`, `kaup`, and `kgd80-80-stories`. Treat these as source-first recovered assets; keep provenance in the source README and docs.

Record which branch/commit or dirty-worktree source supplied every imported asset.

## Current asset/data contract

- Runtime assets: `site/public/assets/organizers/`, `site/public/assets/festivals/`, and `site/public/assets/badges/`.
- Source/provenance originals: `site/src/assets/organizers/source/`, `site/src/assets/festivals/source/`, and `site/src/assets/badges/source/`.
- Manifests: `site/src/data/organizerMedallions.json` and `site/src/data/festivalMedallions.json`.
- Renderer: `site/src/components/EventTokenMedallions.astro`.
- Visual lab: `site/src/pages/lab/medallions/index.astro`.
- Canonical docs: `docs/features/static-site-pages/event-token-medallions.md`.

Manifest items should include at least: `slug`, `name`, `shortName`, `aliases`, `sourcePage/sourceUrl/sourceFile`, `background`, `ring`, `ariaLabel`, `renderNote`, `avatarUrl`, `fallbackPngUrl` when applicable, `sourcePath`, `retrievedAt`, and `assetFormat`.

## Sourcing and vector decision tree

1. Search official site/source first (`logo.svg`, page assets, press kit, social profile OG image, existing local references).
2. Search current repo and branches before the web.
3. If a generic symbol is needed (e.g. program/category icon, not an organizer logo), use the `svgrepo-svg-finder` workflow: local library first, then SVG Repo with license/provenance and visual inspection.
4. Use runtime SVG when:
   - official SVG exists;
   - a simple geometric mark can be faithfully reconstructed from a trusted raster;
   - a self-contained SVG medallion can be assembled from source shapes/text without pretending it is the original logo.
5. Use WebP-first raster when:
   - only raster source exists;
   - vectorization would distort a complex/logo-like mark;
   - license/provenance allows local runtime use but not derivative vector reconstruction.

## Rendering pattern

- Keep circle diameter visually aligned with organizer medallions.
- Use a filled background + ring; avoid transparent marks that disappear on cream/white.
- Center optically, not mathematically: check mobile 90px and desktop ~112px.
- For very vertical marks, use an explicit medallion viewBox/crop and document the optical shift.
- Avoid overlays over poster/OCR text unless separately QA-approved.

## Bach / organ / “Бахослужение” style medallions

Treat composer/program medallions as **event-program badges**, not organizer logos. If an official festival/series asset exists (for example `bahosluzhenie` in `festivalMedallions.json`), prefer that source-grounded festival medallion over a newly invented generic program SVG.

- Do not use Bach’s portrait/signature unless a public-domain/source and design fit are documented.
- Prefer a neutral deterministic vector: monogram `BACH`, organ pipes, music staff, or church/organ silhouette adapted from a permissive SVG icon.
- Detection must be source-grounded/LLM-first for broad semantics. Deterministic checks may only support narrow evidence such as exact event title/program mentioning `И.С. Бах`, `Bach`, `орган`, `Кафедральный собор`.
- The visible label should be short; use full explanation in `ariaLabel`/tooltip and docs.

## Verification checklist

Before final handoff:

```bash
npm --prefix site run build:preview
npm --prefix site run check:preview
```

Then verify public/local HTML contains the medallion block and expected asset references. Use Playwright or screenshots for visual QA when possible. If deploying a preview, verify HTTP 200 for `__preview/` and at least one event page that should show the new medallion.
