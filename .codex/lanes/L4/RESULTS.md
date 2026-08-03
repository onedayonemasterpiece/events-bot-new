# L4 — unified page runtime and VK question CTA

Status: **Done**

## Acceptance coverage

- **Done — one personalization mount:** `EventLayout` owns exactly one
  `PersonalizationRuntime`; standalone focus/closed-listing pages now mount the
  same runtime exactly once.
- **Done — generated route inventory:** 392 eligible HTML pages pass. Runtime
  contexts: 387 shared auth/transport, 2 specialized diagnostics, 3 explicit
  static-only. Explicit exclusions: 77 lab HTML, 292 JSON routes, 366 ICS
  routes, 1 service worker and 1 webmanifest. Failures: 0. Preview-token prefix
  stripping is tested.
- **Done — production/off characterization:** executable Chromium evidence
  records zero runtime network requests, zero localStorage key/value/byte
  delta, zero visible reorder/copy change and zero event-handler additions.
- **Done — EventCard gesture:** a standard non-interactive body click navigates
  once after 280 ms; mouse double-click and touch double-tap set like=true once
  and cancel navigation. Drag, rails and nested like/share/calendar/link/form/
  role controls are excluded; keyboard Enter navigates immediately and clears
  pending pointer state; dynamically cloned cards use delegation.
- **Done — exact CTA contract/resolver:** `question_cta?: { provider: "vk";
  url: string; source: "partner_post" | "managed_afisha_post" }`. Resolution is
  exact partner-owned live post, then published managed Afisha live URL, then
  null. Scheduled/stored/arbitrary/malformed/wrong-owner rows fail closed.
- **Done — rebuild trigger:** successful or recovered managed VK live
  publication coalesces a delayed `static_site_build:prod` request with trigger
  `vk_publication_live`.
- **Done — visual/assets:** CTA uses exact CC0 SVG Repo copies
  `svgrepo-390478` and `svgrepo-348786`, with metadata and provenance. Rendered
  desktop/mobile Chromium screenshots were inspected: icons are legible,
  hierarchy and button contrast are clear, mobile stacks without horizontal
  overflow, and approved copy is `Остались вопросы?` / `Задайте их в
  комментариях ВКонтакте`.

## Validation

- `npm run build` — PASS, 469 pages in 2m10s. Existing Vite warning remains for
  inconsistent JSON import attributes on `listingMediaOverrides.json`.
- L4 + P13N Node test bundle — PASS, 14/14.
- `node scripts/check-personalization-source-guard.mjs` — PASS.
- focused Python resolver / VK rebuild tests — PASS, 6/6.
- `node tests/event-card-double-like.playwright.mjs` — PASS.
- `node tests/event-question-cta.playwright.mjs` — PASS.
- `node tests/personalization-off.playwright.mjs` — PASS.
- personalization route inventory — PASS, 392 eligible, 0 missing, 0
  duplicate, 0 unclassified/policy failures.
- page runtime inventory — PASS, 392 eligible HTML, 737 explicit lab/non-HTML
  exclusions, 0 failures.
- `npm run check:preview` is not applicable to the plain `npm run build`
  artifact and correctly stopped with `No preview-* folder found in dist`; no
  preview-prefix deployment was performed.

Artifacts (ignored):

- `artifacts/codex/l4-personalization-route-inventory.json`
- `artifacts/codex/l4-page-runtime-inventory.json`
- `artifacts/codex/l4-question-cta-visual/desktop-cta.png`
- `artifacts/codex/l4-question-cta-visual/mobile-cta.png`
