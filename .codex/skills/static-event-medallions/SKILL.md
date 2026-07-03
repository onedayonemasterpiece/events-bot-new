---
name: static-event-medallions
description: Use in events-bot-new when adding, auditing, sourcing, repairing, or prioritizing KenigEvents static-site event/venue/organizer/source/festival/program medallions; triggers include medallion/медальон badges, organizerMedallions.json, festivalMedallions.json, EventTokenMedallions.astro, Pushkin-card badges, venue-logo gap analysis, SVG/WebP medallion rendering, visual QA, or git-history archaeology for previous medallion work.
---

# Static Event Medallions

Use this skill only in `/home/dev/projects/events-bot-new` for static-site medallions/badges.

## Start

1. Open the canonical docs before changing behavior:
   - `docs/features/static-site-pages/event-token-medallions.md`
   - `docs/features/static-site-pages/astro-preview.md` when building/checking previews.
2. Check current files, not memory:
   - `site/src/data/organizerMedallions.json`
   - `site/src/data/festivalMedallions.json`
   - `site/src/components/EventTokenMedallions.astro`
   - `site/src/pages/lab/medallions/index.astro`
   - source READMEs under `site/src/assets/{organizers,festivals}/source/`; for badge/source medallions, create the matching provenance folder/README if it does not already exist.
3. If prioritizing gaps from production events, use the `fly-prod-db-access` skill and read-only SQLite probes against `/data/db.sqlite`.
4. Read `references/history-and-methods.md` when you need archaeology, known SHAs/branches, or method selection details.

## Non-negotiable rules

- **Git archaeology first.** Search current branch, `origin/main`, active feature/recovery branches, and local worktrees before redrawing any medallion.
- **Prefer accepted assets.** If an asset exists on a previous branch/commit, reuse/cherry-pick/copy it with provenance instead of recreating it.
- **Source-first, SVG-first.** Official/local logo SVG is preferred. Use WebP-first raster only when vector output would distort a complex mark or no trustworthy vector exists.
- **Do not guess logos.** Unknown organizers may get neutral initials only after the normalized organizer/venue is known; never invent institutional marks.
- **No paid OpenAI image generation/editing** without explicit consent in the current thread. Use official SVG/raster, social avatars, SVG Repo/local icons, deterministic SVG, Inkscape/rsvg/sharp/Pillow/OpenCV, or manual source-faithful cropping.
- **Do not use `contour_svg`/`countur_svg` as a shortcut for logos.** It is a separate photo-to-contour system and was not the medallion SVG-upgrade path. Historical medallion SVGs used direct official SVG path extraction/wrapping, hand-rebuilt simple SVG primitives, local raster-to-path vectorization for a geometric mark, or an SVG container embedding a trusted raster source. Use those patterns deliberately; never raw-vectorize a logo from edges and present it as official.
- **Visual QA is required** for new/changed artwork: inspect mobile (~90px circle) and desktop (~112px circle), check optical centering, contrast, and no horizontal token overflow.
- **Keep docs/provenance synced.** For code/asset behavior changes update `docs/features/static-site-pages/event-token-medallions.md`, relevant source README, and `CHANGELOG.md`.

## Git archaeology commands

Run targeted searches before editing:

```bash
git fetch origin --prune
git log --all --decorate --oneline --grep='medallion\|badge\|медаль\|бах\|bach\|организ' -i --max-count=160
git branch -a --list '*medall*' '*badge*' '*static*' '*event-issue*'
git log --all --oneline -- site/src/data/organizerMedallions.json site/src/data/festivalMedallions.json site/src/components/EventTokenMedallions.astro site/src/assets/organizers site/src/assets/festivals site/public/assets/organizers site/public/assets/festivals docs/features/static-site-pages/event-token-medallions.md
git grep -n -i -E 'medallion|медаль|organizerMedallions|festivalMedallions|бахослуж|bach|contour_svg|countur_svg|counter_svg' \
  $(git for-each-ref --format='%(refname)' refs/heads refs/remotes/origin) -- \
  site/src/data site/src/components site/src/assets site/public docs/features/static-site-pages .codex/skills 2>/dev/null
```

Record the supplying SHA/branch for every reused asset.

## Production venue gap analysis

When asked “where are medallions missing?”:

1. Query production SQLite read-only by `location_name`, using current date as an explicit absolute date.
2. Count at least: `total`, `future_or_current`, `recent_90`, `first_date`, `last_date`, top `city`, `festival`, `event_type`, `source_post_url`/`source_chat_id`, `source_vk_post_url`, and ticket domains.
3. Compare venue names against `aliases` in `organizerMedallions.json`; compare event `festival` values against `festivalMedallions.json` separately so venue gaps are not hidden by festival badges.
4. Prioritize venues with multiple future/current events and recurring history. Treat duplicate venue spellings like `Янтарь холл, Ленина 11, Светлогорск` as alias candidates for the same medallion.
5. For each candidate, classify source quality:
   - **Strong:** official site exposes SVG/clean PNG/logo and our events point to the same official domain/social account.
   - **Medium:** official Telegram/VK avatar or clean ticket/vendor image exists.
   - **Low:** only aggregator/repost/shortlink sources; use neutral initials or defer unless product value is high.

## Source and rendering decision tree

1. Search existing repo/branches first.
2. Search official site/press kit/page assets (`logo.svg`, `favicon`, header logo, OG image), then official Telegram/VK public avatar, then ticket/vendor pages.
3. If the mark is official SVG, wrap/crop it into a deterministic circle medallion; do not rewrite paths unless necessary for centering/contrast.
4. If the source is a simple geometric raster, recreate as self-contained SVG primitives only when source-faithful and visually verified.
5. If a geometric raster must become SVG and no official SVG exists, use local trace/vectorization only as a controlled source-faithful conversion; document that it is locally vectorized and keep PNG/WebP fallback.
6. If a source PNG must be carried inside SVG for layout/round framing, mark it honestly as `svg-embedded-source-png`, not as a true vector logo.
7. If the source is complex raster or hand-lettered, crop/recompose locally and export WebP primary + PNG fallback.
6. Pick background/ring from brand source or sampled social/avatar colors; document sampling evidence.
7. For program/category badges, use deterministic icons/monograms and keep them distinct from organizer logos.

## Manifest and file contract

Organizer/venue assets:

- runtime: `site/public/assets/organizers/`
- source originals: `site/src/assets/organizers/source/`
- manifest: `site/src/data/organizerMedallions.json`

Festival/program assets:

- runtime: `site/public/assets/festivals/`
- source originals: `site/src/assets/festivals/source/`
- manifest: `site/src/data/festivalMedallions.json`

Manifest items should include:

```text
slug, name, shortName, aliases, sourcePage, sourceUrl, sourceFile,
background, ring, logoCrop/fitBox when useful, ariaLabel, renderNote,
avatarUrl, fallbackPngUrl for raster/WebP, sourcePath, retrievedAt, category/assetFormat when useful
```

## Runtime rendering contract

- Event detail page is the primary medallion surface; list/search cards need separate product approval.
- Use `<picture>` for WebP primary + PNG fallback.
- Keep full `aria-label` semantics for icon-only medallions.
- Avoid duplicated festival/organizer identity tokens when a curated organizer token already carries the same brand.
- Keep festival/program tokens source-grounded; do not infer broad semantics with regex-only rules.

## Verification checklist

Before final handoff for code/asset changes:

```bash
npm --prefix site run build:preview
npm --prefix site run check:preview
```

Then inspect generated HTML or screenshots for:

- expected medallion block and asset URLs;
- expected `aria-label`s;
- no broken images;
- acceptable mobile/desktop optical centering;
- source README + manifest + docs + changelog consistency.
