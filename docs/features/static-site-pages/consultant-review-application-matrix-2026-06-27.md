# Consultant Review Application Matrix — Event Page MVP — 2026-06-27

> **Status:** traceability matrix for the full consultant review, so the review is not only archived but also explicitly applied.
> **Input review:** [consultant-event-page-mvp-review-2026-06-27.md](consultant-event-page-mvp-review-2026-06-27.md).
> **Canonical implementation target:** [event-page-merged-skeleton.md](event-page-merged-skeleton.md).
> **Scope:** first Astro SSG vertical slice for static event pages on `kenigevents.ru`.

## Executive answer

The consultant review was processed in three layers:

1. **Archived verbatim** in [consultant-event-page-mvp-review-2026-06-27.md](consultant-event-page-mvp-review-2026-06-27.md).
2. **Merged into the canonical skeleton** in [event-page-merged-skeleton.md](event-page-merged-skeleton.md).
3. **Tracked below** as accepted / modified / deferred / rejected decisions.

The important correction from the review is that the next step is **not another design review and not a one-off test HTML page**. It is a real **Astro SSG vertical slice** that produces `/sobytiya/<stable-slug>/index.html` for 5–10 production-like future events.

## Decision matrix

| Consultant point | Decision | How it is applied |
| --- | --- | --- |
| Variant A is stronger as canonical MVP/product contract. | **Accepted** | Skeleton states: “Variant A as canonical product/MVP contract”. Variant B is a donor for concrete UI/implementation details only. |
| Variant B is useful as UI/UX/SEO wireframe, but cannot be taken whole. | **Accepted** | Adopted B's desktop 8/4 grid, sticky mobile CTA, semantic facts, `.ics`/share details. Excluded B's FAQ/gallery/API/hidden LLM fragments and broad analytics. |
| Current work is design, not implementation. | **Accepted and made explicit** | README and skeleton now state there is no Astro build in `events-bot-new` yet; first implementation must create a real Astro SSG generator. |
| First implementation must generate real `/sobytiya/<slug>/index.html` pages. | **Accepted** | P0 gate requires Astro SSG generator and 5–10 future active event pages. |
| Mobile order should be decision-first: brand, compact breadcrumbs, hero, badges, H1, facts, CTA, actions, summary, description, facts/source, other dates, related, anti-bubble, footer. | **Accepted with slight consolidation** | Skeleton main-content/transaction sections encode this order. Breadcrumbs/header must not push facts below first viewport. |
| Gallery before description is over-engineering. | **Accepted** | Gallery/lightbox excluded from P0; allowed only if 2–3 verified local/proxied images exist. |
| FAQ/FAQPage should not be P0. | **Accepted** | FAQ accordion and FAQ schema are excluded from P0. |
| Desktop 8/4 grid from B is strong. | **Accepted** | Skeleton uses max-width shell, 12-column grid, main 8 / sidebar 4. |
| `H1` in sidebar is fatal for long Russian titles. | **Accepted** | Skeleton says H1 always in main content and P0 gate checks safe wrapping. |
| Sidebar should be only transactional. | **Accepted** | Sidebar contains date/time, venue/address/map, ticket status, CTA, calendar/share/copy, compact facts. |
| CTA matrix from A is stronger. | **Accepted** | Skeleton keeps CTA states: buy/register/call/source/status-only; primary CTA must be real `href`. |
| `.ics` must work without JS. | **Accepted** | Skeleton requires `.ics` calendar link without JS. |
| Share/copy are progressive enhancements, not core dependencies. | **Accepted** | Skeleton: Web Share API and copy are enhancements; fallback links/visible URL remain. |
| `Сохранить` should be removed from P0. | **Accepted** | Skeleton explicitly omits save action from MVP scope. |
| SEO contract from A is stronger, B gives useful concrete schema examples. | **Accepted with guardrails** | Skeleton uses Event by default, MusicEvent only when reliable, BreadcrumbList, title/meta/canonical/OG, JSON-LD matching visible facts. |
| Coordinates, duration, performers, organizer, FAQ facts must not be hallucinated. | **Accepted** | Skeleton forbids unbacked structured data and requires verified DB facts. |
| Hidden LLM fragments/comments should not be P0. | **Accepted / tightened** | Skeleton says no hidden LLM-specific fragments/comments in P0; visible `<dl>` facts are allowed. |
| `llms.txt` is post-MVP, not release blocker. | **Accepted** | Skeleton excludes `llms.txt` from P0 release gates. |
| Hero preload only if image is local/proxied/dimensioned. | **Accepted** | Skeleton requires safe stable media before preload; otherwise fallback. |
| Static related HTML first; no `/api/v1/related` in first slice. | **Accepted** | Skeleton forbids required related API/Supabase call in initial slice. |
| Future personalization only reranks/hides within static pool after consent. | **Accepted** | Skeleton states consented personalization may only rerank/hide static candidates and cannot affect above-the-fold. |
| No visible reorder/jump after related block is in viewport. | **Accepted** | Skeleton includes this as a recommendation rule. |
| “Другие жанры рядом” should be separate anti-bubble block. | **Accepted** | Skeleton includes separate static anti-bubble block, 1–2 cards. |
| Promo is safer omitted in first slice unless real campaign exists. | **Accepted / tightened** | Skeleton says promo omitted by default; if present, at most one labeled native/static card after organic context. |
| Promo must not be between H1/facts and primary CTA. | **Accepted** | Skeleton prohibits promo before primary action and organic context. |
| Promo must not be unlabeled organic recommendation. | **Accepted** | Skeleton forbids unlabeled promo in related. |
| Complex promo frequency cap is not P0. | **Accepted** | Skeleton defers promo frequency cap until telemetry/write-path exists. |
| Dark mode, full design-token system, gallery analytics, FAQ expand analytics are over-engineering. | **Accepted / deferred** | Not included in P0 skeleton. Minimal CSS variables are enough for first slice. |
| Map link is not P0 if address reliability is weak. | **Accepted with condition** | Skeleton: map link only when reliable address exists. |
| Ticket URL must be validated and safely encoded. | **Accepted** | P0 gate requires valid/safely encoded direct `href`. |
| External/hotlinked images are risky. | **Accepted** | P0 gate requires local/proxied/dimensioned image or graceful fallback. |
| Analytics should be compact or disabled; no raw telemetry firehose. | **Accepted** | Skeleton P0 gate: compact or disabled analytics, no raw telemetry firehose. |
| Rollback path is required. | **Accepted** | P0 gate requires rollback via previous static tree or disabling links. |

## Deferred items

These are not rejected forever; they are out of the first vertical slice:

- client-side `/api/v1/related` or Supabase recommendation fetch;
- remote personalization/write path;
- FAQ UI and `FAQPage` schema;
- gallery/lightbox and gallery analytics;
- hidden LLM-specific content fragments;
- `llms.txt` as launch blocker;
- save action without auth/consent state;
- dark mode;
- full design-token system beyond minimal CSS variables;
- complex promo frequency caps;
- full public events JSON API.

## Rejected for P0

These should not appear in the first production preview:

- H1 in desktop sidebar;
- CTA that depends on JS;
- unlabeled promo mixed into organic recommendations;
- hallucinated schema facts;
- empty/null badges or fact rows;
- recommendations that include current/past/expired events;
- personalization-dependent content above the fold;
- layout that is just stretched mobile cards on desktop.

## Remaining action

The next useful step is implementation, not another abstract review:

```text
Astro SSG scaffold
  → production-like event export fixture
  → /sobytiya/<stable-slug>/index.html for 5–10 events
  → CSS/layout + no-JS CTA/calendar/share fallback
  → JSON-LD/sitemap/robots/preview noindex
  → Playwright/static HTML checks
  → upload preview to kenigevents.ru bucket prefix
```
