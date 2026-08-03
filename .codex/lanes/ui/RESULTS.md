# Tile mosaic UI lane result

- **Base:** `f44f7fc66` (`origin/feature/static-launch-tile-mosaic-20260803`)
- **Head:** the committed lane HEAD returned with this report (`agent/static-launch-tile-mosaic-20260803/ui`)
- **Scope:** R01 / R02 / R04 UI implementation only.

## Files

- `site/src/components/launch/TileMosaicLaunch.astro`
- `site/src/pages/lab/launch/tile-mosaic/index.astro`
- `site/public/assets/launch/PWA-icon.png`
- `.codex/lanes/ui/RESULTS.md`

## Implemented

- One replaceable image beneath 72 physical-looking HTML/CSS tiles: 12×6 desktop and 6×12 mobile.
- Registered continuous tile variables and deterministic `sealed`, `dim`, `revealed`, `glint`, `sleeping` states.
- Sparse seeded 1–4 tile cycles (1.2–2.8s selection, 4.8s transitions), state caps/repeat avoidance, autonomous light and subtle pointer attraction.
- Reduced-motion static composition with pointer/random animation disabled.
- `imageSrc`/`focalX`/`focalY` Astro props, validated preview-base-safe `?mosaicImage=` query input, and validated `tile-mosaic:set-image` event input. Protocol-relative, credential-bearing, non-HTTPS cross-origin, and preview-prefix-escaping same-origin URLs are rejected.
- Responsive desktop/mobile layout and exact mobile content order; 320px guardrail; visible form label, honeypot, native email semantics, focus treatment, `aria-live`, `aria-busy`, and idle/submitting/success/error states.
- Subscription goes through shared `getResilientDataClient` using only `PUBLIC_PERSONALIZATION_SUPABASE_URL`, optional `PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL`, and `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY`; email is preserved for missing env, network/ambiguous result, and RPC errors.
- Static Russian SEO/GEO content, canonical, OG/Twitter, exact noindex directive, launch `<time>`, and WebSite/WebPage/Service JSON-LD.
- Served image is byte-identical to tracked `site/public/assets/pwa/focus-group-icon.png` / historical canonical (`sha256: 7015488739e0296f6c5b04935a16769804aa8bf128436450e8a60eef32ec07dd`). Integration owns adding the requested canonical docs path at the same hash.

## Commands and tests

- `python3 .../ui-ux-pro-max/scripts/search.py ...` — unavailable because the installed skill's `scripts` pointer resolves to absent `/home/dev/src/ui-ux-pro-max/scripts/search.py`. Applied the skill's embedded accessibility/touch/responsive/form guidance directly: ≥54px controls, visible label, focus ring, non-color-only messages, responsive no-overflow guards, reduced motion.
- `npm run build` — initially blocked by missing worktree dependencies/config fixture; after linking the existing project `node_modules` and restoring the tracked config fixture, **passed**: 467 pages, including `/lab/launch/tile-mosaic/`.
- `SITE_BASE_PATH=/preview-smoke npm run build` — **passed**: 467 pages.
- Generated HTML verification — **passed**: 72 tiles; `/preview-smoke/assets/launch/PWA-icon.png` and `/preview-smoke/favicon.svg`; no root `/assets/launch/PWA-icon.png` leak; served asset exists.
- Source contract assertions — **passed**: 72 tiles, five states, reduced motion, image contracts, resilient RPC/env contract, no generic/service key, SEO/JSON-LD/date tokens.
- `git diff --check` — **passed**.
- Playwright visual check — blocked before first browser run: installed Playwright had no Chromium; `npx playwright install chromium` then failed with `ENOSPC` on the host root filesystem. No browser screenshots are claimed by this lane.

## Risks / merge notes

1. Integration must add `subscribe_site_launch_v1` to `SELECTED_ONCE_RPCS` in `site/src/lib/backendOperationCatalog.ts`; the transport intentionally fail-closes an unclassified RPC. Do not classify it as replayable because `submission_count` increments.
2. The DB migration must return the documented PostgREST table shape (`[{accepted:true,status:"subscribed"}]`); malformed 2xx payloads are treated as errors.
3. Integration/release owner must run browser screenshots at 320/390 and desktop after freeing disk or using a host with Chromium, then test live success/duplicate/invalid/network/missing-env flows.
4. No changes were made to production `site/src/pages/index.astro`, DB, docs, or changelog in this lane.
