# Tile mosaic launch integration report

## Identity and boundary

- Base: `origin/feature/static-launch-tile-mosaic-20260803` at
  `f44f7fc66ce6f833b7412796de1fb36f53cacec0`.
- Integration branch: `integration/static-launch-tile-mosaic-20260803`.
- Delivery branch: `feature/static-launch-tile-mosaic-20260803`.
- Route: `/lab/launch/tile-mosaic/` only.
- `site/src/pages/index.astro`, the production root, navigation, sitemap and
  indexability were not changed.
- The unlisted preview bearer URL and Telegram destination receipt are kept out
  of Git and recorded only in the task artifact/final handoff.

## Lane reconciliation

| Lane | Requirement IDs | Worker head | Integrated commit | Status | Evidence |
|---|---|---|---|---|---|
| UI | R01, R02, R04 | `fc121a2c994f3e43fff5eb9d1c4993b49687676d` | `3832d061a` | merged | `.codex/lanes/ui/RESULTS.md` |
| DB | R03 | `c969f75872f79b94f4ff8fbd9b501906dae229a5` | `93b8e2c15` | merged | `.codex/lanes/db/RESULTS.md` |
| Docs | R05 | `d2b2d93e5659a3b99950a3257e5ef07c508bc7db` | `3fa94fdba` | merged | `.codex/lanes/docs/RESULTS.md` |
| Integration | R06, R07, R08 | n/a | `9e3b9b47b` plus this report | committed | catalog/unit gate, DB apply/RPC receipts, candidate/Telegram external receipts |

Every worker worktree was clean at handoff. Every worker commit was inspected
and cherry-picked; there are no rejected, abandoned or unmerged worker changes.

## Integration reconciliation

- Registered `subscribe_site_launch_v1` in the shared backend operation catalog
  as `selected-once`. It is deliberately not replayable because the RPC updates
  `submission_count`.
- Added the requested canonical source asset and verified that it and the served
  copy are byte-identical:
  `sha256:7015488739e0296f6c5b04935a16769804aa8bf128436450e8a60eef32ec07dd`.
- Added the canonical docs index/routes and the `[Unreleased]` changelog entry.
- Kept the form on the separate personalization Supabase contour and on the
  shared `ResilientDataClient`; no legacy Supabase key or server credential is
  present in browser code.

## Verification ledger

Selected static-site trigger tags:
`static-route`, `visual-layout`, `mobile-input`, `supabase-connectivity`.

- L0 Astro/source contract: worker full builds passed at 467 pages, including a
  `/preview-smoke` base-path build; final exact-SHA candidate build remains the
  integration publication gate.
- L0 transport contract: the focused resilient transport suite passed `16/16`,
  including the new selected-once RPC classification.
- L0 DB: PostgreSQL 17 disposable and transactional live-project checks passed;
  the additive migration was then applied to the personalization project with
  migration history, RLS/table ACL/function ACL/search-path checks.
- Live RPC: first and duplicate anonymous calls both returned the constant safe
  contract, one normalized row reached `submission_count=2`, invalid input
  returned `400`, and the synthetic smoke row was deleted.
- L1 pre-publication browser: Chromium covered 320, 360, 390, 430, 768, 1024,
  1366, 1440 and 1920 px without horizontal overflow; 72 tiles, image loading,
  mobile order, control geometry, reduced motion, sparse motion, image URL
  validation, invalid email, missing-env preservation and honeypot behavior
  passed. Final exact-SHA public screenshots/form checks are stored as task
  artifacts rather than committed outputs.
- L2 Android/iOS `mobile.keyboard_inputs` and
  `mobile.page_family_specimens` remain `planned` in the canonical registry and
  therefore do not block this isolated noindex review candidate. The requested
  human mobile review receives the exact HTTPS candidate URL in Telegram.

## Residual promotion gates

- The anonymous RPC is safe from table reads/email enumeration but is not a
  trustworthy per-IP rate limiter. Edge/WAF rate limiting and monitoring are
  required before a broadly promoted/indexable launch page.
- This candidate does not authorize merging to `main`, replacing the root,
  enabling indexing or production-root promotion.
