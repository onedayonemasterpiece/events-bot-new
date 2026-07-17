# Phone CTA lane result

## Outcome

Restored the pre-`4faf3eac` branded primary-action hierarchy for the desktop
phone-only specimen (`6851`) instead of retaining v10's plain number plus a
detached utility icon.

- initial branded CTA: copy icon + `Показать телефон`;
- one click reveals `+7 911 868-89-55` inside the same CTA and copies normalized
  `+79118688955`;
- transient, non-layout toast and polite live region say `Номер скопирован`;
- Clipboard API rejection falls back to a hidden textarea / `execCommand`;
- subsequent clicks copy again; CTA and panel dimensions do not change;
- phone glyph and visible helper line stay absent;
- calendar remains icon-only and admission/calendar/share/like stay on one row.

The primary button reuses the existing accepted terracotta radius, shadow,
weight and 56px action styling recovered from `4faf3eac^`; this is not a new CTA
redesign.

## Focused validation

- `PREVIEW_BUILD_ID=preview-20260717t-static-personalization-v11-phone-lane node --test tests/event-detail-runtime-regressions.test.mjs`: **5/5 pass**.
- `npm run check:production-desktop`: **303 event pages pass**.
- `npm run check:preview`: **303 events pass**.
- Full preview compile: **373 pages**.
- Exact-source Playwright on real event `6851`, at `1366×768`, `1536×864`,
  `1920×1080`: **3/3 pass** for initial label/icon, reveal, normalized clipboard
  value, toast/live feedback, icon-only calendar, containment, no horizontal
  overflow, and stable CTA/panel geometry.
- Dev-only `/data/discovery/6851.json` is not materialized by Astro dev and
  returns the expected 404; the full static preview check has the generated
  endpoint and passed.
- `git diff --check`: pass.

The full compile completed before the last one-line `overflow: visible`
strengthening that lets the non-layout toast escape a later page-level rounded
button rule. The final exact source was compiled by Astro dev and browser-gated;
the integration lane should perform its normal final full build after merging
all three lanes.

## Evidence

- `artifacts/codex/static-event-v11-phone-cta/playwright-results.json`
- `artifacts/codex/static-event-v11-phone-cta/phone-before-1536.png`
- `artifacts/codex/static-event-v11-phone-cta/phone-after-1536.png`
- `artifacts/codex/static-event-v11-phone-cta/phone-cta-playwright.cjs`

## Files

- `site/src/components/DesktopEventActionPanel.astro`
- `site/tests/event-detail-runtime-regressions.test.mjs`
- `site/scripts/check-production-desktop-contract.mjs`
- `docs/features/static-site-pages/event-page-product-design.md`
- `docs/reports/incidents/INC-2026-07-16-static-event-media-action-regressions.md`
- `CHANGELOG.md`
