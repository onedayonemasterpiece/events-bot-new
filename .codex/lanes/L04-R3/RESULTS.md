# Lane L04 Results

## Scope and status

- Lane: `L04`
- Requirements: `R08`, `R09`
- Status: Done
- Base SHA: `68576d5b70f57164c00386b05cff126586c3f700`
- Implementation head SHA: `0b78ade003cf10e9b00de73bccd5f7be63d6f764`

## Delivered

- Visually inspected `docs/features/static-site-pages/references/klubs (2).png` and adapted its centered atmospheric intro plus dark, image-led desktop cards without copying unrelated page chrome.
- Audited existing club/event images against production event provenance. Added one documentary Game Vibes photograph from approved event `2897` and retained its source URL, Telegram post, event identity, checksum, dimensions, and audit note beside the asset.
- Rejected the available neural-club announcement illustrations as generated/non-documentary media. That club deliberately uses a deterministic CSS fallback rather than an untrustworthy photo.
- Made the current two-card desktop catalog a complete equal-width row. The column count remains data-driven up to three cards, while the established one-column light-card mobile presentation is preserved.
- Added catalog-scoped desktop keyboard behavior:
  - rendered geometry, rather than DOM adjacency, defines visual rows and order;
  - Left/Right follow flattened visual order;
  - Up/Down choose the nearest column in the adjacent row;
  - Home/End move to the first/last visible card;
  - Enter on a focused card activates only that card's primary action;
  - nested links retain native activation and Escape returns focus to their card;
  - shortcut hints remain hidden except on the currently focused card/action.
- Added runtime broken-image fallback handling and automated coverage for geometry, complete rows, scoped activation, focus-only hints, source provenance, image fallback, and mobile overflow.
- Selectively compared `integration/static-unified-prototype-corrections-20260723`; accepted its geometry-based ordering, focus-only hint pattern, and current breadcrumb-free club page, without porting unrelated shared keyboard changes.

## Source evidence

- Local asset: `site/src/assets/clubs/source/game-vibes-event-2897.webp`
- SHA-256: `2722133536e3eeb695a0185751d48aacf251a4ae1b544c7b90981c8386f63672`
- Production source event: `2897`, “Вавилон”, `2026-03-14`
- Source post: `https://t.me/signalkld/9929`
- Original approved asset URL and the full audit record are in `game-vibes-event-2897.metadata.json`.

## Validation

Commands run from the lane worktree:

- `PUBLIC_INTEREST_CLUBS_ENABLED=1 npm run build`
  - Passed: Astro generated 383 pages; the catalog and club detail routes built successfully.
- `node --test tests/interest-club-catalog.test.mjs tests/interest-club-catalog.browser.test.mjs`
  - Passed: 4/4.
- `node --test tests/keyboard-event-navigation-production.test.mjs tests/visual-keyboard-regressions.test.mjs`
  - Passed: 15/15.
- Playwright visual QA at 1440 px and 390 px:
  - desktop cards formed one complete equal-width row;
  - mobile cards used the established light stacked treatment;
  - horizontal overflow was `0` at both widths;
  - hidden keyboard instructions remained clipped to 1 px.
- `sha256sum site/src/assets/clubs/source/game-vibes-event-2897.webp`
  - Matched the checked-in provenance metadata.
- `git diff --check` and `git diff --cached --check`
  - Passed.

Python regression attempt:

- `python3 -m pytest ...` could not start because `pytest` is not installed in the base interpreter.
- `uv run --with pytest pytest tests/test_interest_clubs_static_export.py` reached repository test loading but stopped because root `conftest.py` requires unavailable `aiogram`.
- This is a test-environment dependency limitation; the Astro build and targeted Node/Playwright catalog coverage passed.

## Risks

- Only Game Vibes currently has a trustworthy documentary source image. The neural club intentionally remains on a provenance-safe deterministic fallback until a real source photograph is available.
- Browser coverage requires a fresh enabled catalog build (`PUBLIC_INTEREST_CLUBS_ENABLED=1 npm run build`) before execution.
- No shared keyboard component was changed; the new controller is intentionally isolated to `[data-club-catalog]`.

## Changed files

- `site/src/assets/clubs/source/game-vibes-event-2897.metadata.json`
- `site/src/assets/clubs/source/game-vibes-event-2897.webp`
- `site/src/components/ClubCatalogKeyboard.astro`
- `site/src/components/InterestClubCard.astro`
- `site/src/components/clubCatalogNavigation.mjs`
- `site/src/data/interest-club-covers.ts`
- `site/src/pages/kluby-po-interesam/index.astro`
- `site/tests/interest-club-catalog.browser.test.mjs`
- `site/tests/interest-club-catalog.test.mjs`
- `.codex/lanes/L04/RESULTS.md`
