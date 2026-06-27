# Event Page Merged Skeleton — «Полюбить Калининград Анонсы»

> **Status:** implementation target for the first static event-page vertical slice after Variant A/B comparison, Gemini comparison and external MVP review.
> **Implementation status in `events-bot-new`:** first **Astro SSG preview vertical slice is implemented** under `site/` and published at `https://kenigevents.ru/preview-20260627-event-pages-v6/__preview/`. Production rollout is still pending: the current build uses a compact committed fixture from real production rows, preview `noindex`, and preview canonical URLs.
> **Source reviews:** [Variant A product/design spec](event-page-product-design.md), [Variant B Opus UI/UX variant](opus-event-page-ui-ux-2026-06-27.md), [Gemini comparison review](gemini-event-page-comparison-2026-06-27.md), [consultant MVP review](consultant-event-page-mvp-review-2026-06-27.md).
> **Control event:** production event `5878` — «Песни СССР», 2026-07-11 21:30, Янтарь-холл, Светлогорск.

## 0. Implemented preview artifact

Current public preview, built and deployed 2026-06-27:

- index: <https://kenigevents.ru/preview-20260627-event-pages-v6/__preview/>
- today listing: <https://kenigevents.ru/preview-20260627-event-pages-v6/segodnya/>
- weekend listing: <https://kenigevents.ru/preview-20260627-event-pages-v6/vyhodnye/>
- control event: <https://kenigevents.ru/preview-20260627-event-pages-v6/sobytiya/pesni-sssr-svetlogorsk-5878/>
- control ICS: <https://kenigevents.ru/preview-20260627-event-pages-v6/sobytiya/pesni-sssr-svetlogorsk-5878/event.ics>
- sitemap: <https://kenigevents.ru/preview-20260627-event-pages-v6/sitemap.xml>
- robots: <https://kenigevents.ru/preview-20260627-event-pages-v6/robots.txt>

Runbook/code map: [Astro SSG preview](astro-preview.md).

## 1. Platform decision

The page must be implemented as **Astro SSG**:

- create real static routes such as `/sobytiya/<stable-slug>/index.html`;
- export event data from Fly SQLite into a build contract/fixture before Astro build;
- render SEO-critical event content in HTML, not through client fetch;
- deploy static output to the `kenigevents.ru` Yandex Object Storage bucket;
- keep preview builds under a `noindex` prefix;
- reuse `kgd80/site` patterns where applicable: `getStaticPaths()`, shared layout, event cards, asset gates, preview deploy rewrite, sitemap generation.

The current artifact is an Astro build, but still **preview-only**: production canary requires a regular Fly SQLite export/manifest, production canonical URLs, retention policy and visual/regression gates over a larger event sample.

## 2. Decision summary

The first vertical slice uses **Variant A as canonical product/MVP contract** and **selected Variant B implementation details**:

- static HTML first; no required client-side recommendation fetch for launch;
- no auth, no LLM, no embeddings, no recommendation API and no Supabase write path in initial render;
- `H1` stays in the main content column, never in the desktop sidebar;
- desktop uses an 8/4 grid, but sidebar is only transactional: date, place, status, CTA, actions, facts;
- mobile uses one-column decision flow plus sticky bottom CTA after the main CTA scrolls out;
- `search_digest` appears high on mobile before long description;
- diversity/anti-bubble is a ranking and composition constraint inside the continuation feed, not a separate user-facing block; labels such as «Попробовать другое», «Другие жанры рядом» or «Открыть новое» must not appear in the event-detail UI;
- promo is omitted by default; if present, it is one clearly labeled native/static card and never an unlabeled recommendation;
- omit MVP over-engineering: client-side related API, FAQ schema, gallery/lightbox, hidden LLM fragments, `llms.txt` as a release blocker, complex promo frequency cap, save action and dark mode.

## 3. Head & SEO/GEO

1. `<title>`: `{event.title} — {date}, {venue/city} | Полюбить Калининград Анонсы`.
2. Meta description from `search_digest` / `short_description`, not raw long description.
3. `canonical` URL: `https://kenigevents.ru/sobytiya/<stable-slug>/`.
4. Production pages are indexable; preview prefixes are `noindex`.
5. Open Graph and Twitter Cards use the local/proxied hero poster when available.
6. `schema.org/Event` by default; narrower type such as `MusicEvent` only when category is reliable.
7. `schema.org/BreadcrumbList` from best available city/category taxonomy.
8. JSON-LD must match visible facts; no hallucinated coordinates, duration, performer, organizer, FAQ or price facts.
9. GEO tags only where reliable; venue coordinates only if verified in DB.
10. Visible machine-readable facts use semantic HTML (`<dl>`). No hidden LLM-specific fragments/comments in P0.
11. Preload the hero image only when it is local/proxied, dimensioned and stable; avoid unsafe third-party hotlink preload.

## 4. Global header

12. Brand bar: «Полюбить Калининград Анонсы».
13. Compact MVP navigation: `Сегодня`, `Выходные`; in preview these are real static `/segodnya/` and `/vyhodnye/` pages, not loopback QA anchors.
14. Header and breadcrumbs must not push event facts below the first mobile viewport.

## 5. Layout

15. Mobile: single column, no horizontal scroll, touch targets at least 44px.
16. Desktop: max-width shell, 12-column grid: main content 8 columns, sidebar 4 columns.
17. Desktop sidebar is sticky only inside the event detail region and must not contain `H1`.
18. Tablet can collapse to 2-column or single-column at implementation breakpoint, but CTA visibility is preserved.

## 6. Main content column

19. Breadcrumbs: `Афиша > Светлогорск > Концерты` or best available taxonomy.
20. `H1` event title in the main column on all viewports.
21. Hero image with fixed aspect ratio and reserved dimensions to prevent CLS; mobile keeps the poster visible (`object-fit: contain`), desktop may use wider hero geometry; graceful fallback when media is absent/unsafe.
22. Badges only for known fields: event type, lifecycle/status, festival; do not render empty/null badges.
23. Top facts: date/time, venue, city/address and ticket status.
24. Mobile/high-priority summary: `search_digest` in 1–2 sentences before long description.
25. Long description rendered visibly from trusted sanitized markdown/HTML; it must not be hidden in a collapsed disclosure by default because it is useful for both people and SEO/GEO.
26. Fact block `<dl>` contains only non-empty verified fields: type, date, venue, city, age limit, duration, Pushkin card, organizer.
27. Source/provenance and temporary Telegraph dual-run link appear below facts/description.
28. “Другие даты” shows only same event occurrence group and only future/active occurrences.
29. `H2` “Смотрите дальше”: one mobile-first vertical discovery feed that combines high-similarity candidates with bounded diversity slots as an invisible ranking rule; current/past/expired and same-occurrence/other-date events are excluded by freshness gate.
30. Every discovery card has explicit feedback controls: large like button in the lower-right/right-thumb zone with current aggregate like count, toggle unlike, and a lower-priority “Не интересно” negative control. These labels are product actions, not hidden technical anti-bubble copy.
31. A local like increments the visible count immediately for the current visitor; production source of truth is a compact personalization aggregate snapshot from Supabase/Postgres, not Fly SQLite.
32. Card calendar action is icon-led and links directly to `.ics`; card share duplication is avoided.
33. Desktop renders the same continuation candidates as a normal grid; horizontal mobile rails are not the event-detail continuation pattern.
34. Promo is omitted by default. If a real campaign exists, show at most one clearly labeled `Партнёр`/`Реклама` native card after organic context, not between H1/facts and primary CTA and not as an unlabeled related item.

## 7. Sidebar / mobile transaction block

35. Date and time, with timezone implied by region or explicitly generated.
36. Venue, city, reliable address and map link only when address/coordinates are reliable; weak-address pages say “Точный адрес уточняйте у организатора”.
37. Primary CTA is a real `href`: `Купить билет`, `Зарегистрироваться`, `Позвонить`, `Открыть пост организатора`, or status-only for sold-out/cancelled.
38. For `ticket_status=sale` and valid `ticket_link`, show `Купить билет`.
39. Status indicator uses text plus color/icon: tickets in sale, free, sold out, cancelled/postponed.
40. `.ics` calendar link works without JS and should be served as `text/calendar` with inline content disposition.
41. Source-only CTAs use clear copy such as `Открыть пост организатора`, not ambiguous `Уточнить регистрацию`.
42. Share button is visible by default and attempts native Web Share API first; duplicate Telegram/VK/WhatsApp share pills are not shown on the event page. If native share is unavailable, the single share button copies the URL.

## 8. Mobile sticky CTA

43. Sticky bottom CTA appears only while the user is still in the decision area.
44. It duplicates the same `href` as the primary CTA.
45. It hides when the discovery feed enters the viewport, because at that point the user is choosing the next event and the old CTA becomes distracting.
46. It reserves bottom/safe-area padding and never covers content/footer/action targets.
47. If JS is unavailable, the page still has a visible primary CTA near the top.

## 9. Personalization and recommendations in P0

48. “Смотрите дальше” is static HTML; no required `/api/v1/related` or Supabase call in the first production slice.
49. Explicit `like_event` / `unlike_event` / `not_interested` actions are strong personalization signals and may update local ranking immediately after consent.
50. Later consented personalization may only rerank/hide within the static candidate pool.
51. No personalization-dependent content above the fold.
52. No visible reorder/jump after the related block is in viewport except direct response to an explicit user action such as like/not-interested.
53. Current event, past events and expired lifecycle statuses are excluded.
54. “Другие даты” is separated from recommendations.
    - Related/event cards must not use nested anchors: media/title/action links are separate, and every card has a visual slot plus `.ics` calendar action.

## 10. P0 acceptance gates

55. Astro SSG generator exists and creates `/sobytiya/<stable-slug>/index.html` from production-like event export.
56. At least 5–10 future active events are generated for preview, including paid/free/registration/unknown/other-dates cases.
57. Page is usable with JS disabled: event facts, description, CTA, calendar link and static recommendations remain available.
58. Mobile 375px: no horizontal scroll, CTA visible, touch targets at least 44px.
59. Desktop 1366px: real two-column layout, not stretched mobile feed.
60. `H1` wraps safely in main column for long Russian titles.
61. No `null`, empty badges, empty fact rows or broken image placeholders are rendered.
62. No layout shift from late image/recommendation/promo loading; hero dimensions are reserved.
63. CTA state follows the canonical Variant A matrix.
64. Ticket URL is valid, safely encoded and works as direct `href`.
65. `.ics` file exists, is linked without a forced `download` attribute, and is served as inline `text/calendar` for mobile calendar handoff.
66. `Event`/`MusicEvent` JSON-LD validates and matches visible facts.
67. Breadcrumb links and sitemap entries are generated and valid.
    - `/segodnya/` and `/vyhodnye/` are generated, linked from header and included in sitemap.
68. Preview pages are `noindex`; production pages are indexable.
69. Related freshness gate excludes current/past/expired events.
    - Related gate excludes `other_date_ids` / same occurrence group duplicates.
70. Promo is omitted or clearly labeled and separated.
71. Analytics are compact or disabled; no raw telemetry firehose in MVP.
72. Rollback path exists: previous static tree or disabling links to generated event pages.
