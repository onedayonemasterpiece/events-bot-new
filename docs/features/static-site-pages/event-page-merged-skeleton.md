# Event Page Merged Skeleton — «Полюбить Калининград Анонсы»

> **Status:** implementation target for the first static event-page vertical slice after Variant A/B comparison, Gemini comparison and external MVP review.
> **Implementation status in `events-bot-new`:** first **Astro SSG preview vertical slice is implemented** under `site/` and published at `https://kenigevents.ru/preview-20260627-event-pages-v18/__preview/`. Production rollout is still pending: the current build uses a compact committed fixture from real production rows, preview `noindex`, and preview canonical URLs.
> **Source reviews:** [Variant A product/design spec](event-page-product-design.md), [Variant B Opus UI/UX variant](opus-event-page-ui-ux-2026-06-27.md), [Gemini comparison review](gemini-event-page-comparison-2026-06-27.md), [consultant MVP review](consultant-event-page-mvp-review-2026-06-27.md).
> **Control event:** production event `5878` — «Песни СССР», 2026-07-11 21:30, Янтарь-холл, Светлогорск.

## 0. Implemented preview artifact

Current public preview, built and deployed 2026-06-27:

- index: <https://kenigevents.ru/preview-20260627-event-pages-v18/__preview/>
- today listing: <https://kenigevents.ru/preview-20260627-event-pages-v18/segodnya/>
- weekend listing: <https://kenigevents.ru/preview-20260627-event-pages-v18/vyhodnye/>
- control event: <https://kenigevents.ru/preview-20260627-event-pages-v18/sobytiya/pesni-sssr-svetlogorsk-5878/>
- control ICS: <https://kenigevents.ru/preview-20260627-event-pages-v18/sobytiya/pesni-sssr-svetlogorsk-5878/event.ics>
- control discovery JSON: <https://kenigevents.ru/preview-20260627-event-pages-v18/data/discovery/5878.json>
- sitemap: <https://kenigevents.ru/preview-20260627-event-pages-v18/sitemap.xml>
- robots: <https://kenigevents.ru/preview-20260627-event-pages-v18/robots.txt>

Runbook/code map: [Astro SSG preview](astro-preview.md). Counter freshness: [Event reaction counters](reaction-counters.md). Footer contract: the site footer contains visible/crawlable social links with icons for Telegram, VK and Max; Telegraph uses the same destinations as plain links without icons.

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

- static HTML first; no required live recommendation API for launch; a same-origin static JSON manifest may be used after first paint to top up the feed;
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
12. Visible Russian dates omit the year when both boundaries are inside the build current year. Keep years for non-current-year events and cross-year ranges.

## 4. Global header

13. Brand bar: «Полюбить Калининград Анонсы».
14. Compact MVP navigation: `Сегодня`, `Выходные`; in preview these are real static `/segodnya/` and `/vyhodnye/` pages, not loopback QA anchors.
15. Header and breadcrumbs must not push event facts below the first mobile viewport.

## 5. Layout

16. Mobile: single column, no horizontal scroll, touch targets at least 44px.
17. Desktop: max-width shell, 12-column grid: main content 8 columns, sidebar 4 columns.
18. Desktop sidebar is sticky only inside the event detail region and must not contain `H1`.
19. Tablet can collapse to 2-column or single-column at implementation breakpoint, but CTA visibility is preserved.

## 6. Main content column

20. Breadcrumbs: `Афиша > Светлогорск > Концерты` or best available taxonomy.
21. `H1` event title in the main column on all viewports.
22. Hero image policy is OCR-safe, not one-size-fits-all: verified `visual_only` media may reserve/fill a vertical `4:5` frame; `ocr_text` and `unknown` media must render in their natural image aspect ratio so text is readable and not distorted/cropped.
23. Poster crop policy: when OCR/text is present or unknown, the full image is visible with no crop, no fixed wrapper, no `object-fit: contain` over a duplicate background, no blur fill and no repeated image edges. Only verified text-free/no-meaningful-OCR visual media may use `object-fit: cover` inside a vertical `4:5` frame. This applies to hero, discovery cards and listing thumbnails.
24. Badges only for known fields: event type, lifecycle/status, festival; do not render empty/null badges.
25. Top facts: date/time, venue, city/address and ticket status.
26. Mobile/high-priority summary: `search_digest` in 1–2 sentences before long description.
27. Long description rendered visibly from trusted sanitized markdown/HTML; it must not be hidden in a collapsed disclosure by default because it is useful for both people and SEO/GEO.
28. Fact block `<dl>` contains only non-empty verified fields: type, date, venue, city, age limit, duration, Pushkin card, organizer.
29. Source/provenance and temporary Telegraph dual-run link appear below facts/description.
30. “Другие даты” shows only same event occurrence group and only future/active occurrences.
31. `H2` “Смотрите дальше”: one mobile-first vertical discovery feed that combines high-similarity candidates with bounded diversity slots as an invisible ranking rule; current/past/expired and same-occurrence/other-date events are excluded by freshness gate.
32. Each generated event page should preload up to 10 discovery candidates in static HTML (or all eligible candidates if the compact fixture has fewer). After JS activation, only a consented compatible local profile may filter/rerank: remove `hidden_event_ids` / `not_interested_event_ids` / strong `negative_interest_tags` from those preloaded cards, then perform exactly one automatic lightweight same-origin fetch from `/data/discovery/<event_id>.json` (`event_detail_related` manifest with `related_static[]`) to restore the visible pool. Further expansion must be explicit through `Показать ещё`, not infinite automatic loading.
33. Every discovery card has explicit feedback controls: large like button in the lower-right/right-thumb zone with current aggregate like count, toggle unlike, and a lower-priority “Не интересно” negative control. These labels are product actions, not hidden technical anti-bubble copy.
34. Visible likes must be honest: `likes_count = source_likes_count + service_likes_count`. `source_likes_count` is aggregated from already collected TG/VK/source-post metrics tied to the event; `service_likes_count` is first-party KenigEvents likes from the personalization store. A local like increments the visible count immediately for the current visitor and later becomes service aggregate after ingest. The source/service split is a technical/audit field and must not be rendered in public HTML/UI; users and crawlers see only the total count.
35. Calendar action links directly to `.ics`, but appears on the event detail/primary transaction area only for short/one-day events (`end_date` empty or equal to `start_date`). Feed/preview cards must not show a calendar icon because the bottom row must fit reliably on mobile.
36. The card bottom action row contains exactly `Не интересно`, share and like. The share action always shows a VK-like outlined repost/share arrow and `Поделиться` label; share and like counts are visible only when positive. After a successful like the share action may be highlighted, but it must not float over content or overlap the like button. Native Web Share is tried first, copy fallback increments local share evidence.
37. Hero, card and listing media use the same OCR-safe media mode from `image_text_mode`: `visual_only` / no-meaningful-OCR posters use `cover` in a strict vertical `4:5` frame; `ocr_text` and `unknown` posters use natural image aspect ratio without duplicate/backdrop underlays. Blur fill, repeated image edges, black letterbox-as-design and OCR crop are forbidden.
38. Cards are full-clickable for users via JS (`data-card-href`) while keeping real crawlable HTML anchors on media/title for SEO/GEO. The old explicit `Открыть` card button is omitted as redundant UI noise. Single tap/click navigates immediately. Double-tap like is not part of the accepted interaction because it conflicts with reliable full-card navigation; likes use the explicit button.
39. `Не интересно` turns the current card into a grey explanatory plate and keeps it in place for orientation; subsequent page loads/personalized surfaces may remove or demote similar events.
40. Explicit-feedback rerank must preserve scroll orientation: the acted-on card and all cards above it do not move during the current interaction; only cards below that anchor may be re-ordered.
41. Desktop renders the same continuation candidates as a normal grid; horizontal mobile rails are not the event-detail continuation pattern.
42. Promo is omitted by default. If a real campaign exists, show at most one clearly labeled `Партнёр`/`Реклама` native card after organic context, not between H1/facts and primary CTA and not as an unlabeled related item.

## 7. Sidebar / mobile transaction block

43. Date and time, with timezone implied by region or explicitly generated.
44. Venue, city, reliable address and map link only when address/coordinates are reliable; weak-address pages say “Точный адрес уточняйте у организатора”.
45. Primary CTA is a real `href`: `Купить билет`, `Зарегистрироваться`, `Позвонить`, `Открыть пост организатора`, or status-only for sold-out/cancelled.
46. For `ticket_status=sale` and valid `ticket_link`, show `Купить билет`.
47. Status indicator uses text plus color/icon: tickets in sale, free, sold out, cancelled/postponed.
48. `.ics` calendar link works without JS, is shown only on the detail/primary transaction area for short/one-day events, and should be served as `text/calendar` with inline content disposition. It is intentionally omitted from feed/preview cards.
49. Source-only CTAs use clear copy such as `Открыть пост организатора`, not ambiguous `Уточнить регистрацию`.
50. Share button is visible by default and attempts native Web Share API first; duplicate Telegram/VK/WhatsApp share pills are not shown on the event page. If native share is unavailable, the single share button copies the URL.

## 8. Mobile sticky CTA

51. Sticky bottom CTA appears only while the user is still in the decision area.
52. It duplicates the same `href` as the primary CTA.
53. It hides when the discovery feed enters the viewport, because at that point the user is choosing the next event and the old CTA becomes distracting.
54. It reserves bottom/safe-area padding and never covers content/footer/action targets.
55. If JS is unavailable, the page still has a visible primary CTA near the top.

## 9. Personalization and recommendations in P0

56. “Смотрите дальше” starts as static HTML; no required `/api/v1/related` or Supabase call in the first production slice. A generated same-origin JSON manifest is acceptable as a cacheable static top-up after first paint.
57. Explicit `like_event` / `unlike_event` / `not_interested` actions are strong personalization signals and may update local ranking immediately after consent.
58. MVP personalization on this surface is local and manifest-based: `rankEventDetailRelated` uses the static score as the dominant signal and the consented local profile as a modifier. Live RPC/personal ranker remains a later gated slice.
59. No personalization-dependent content above the fold.
60. No visible reorder/jump after the related block is in viewport except direct response to an explicit user action such as like/not-interested.
61. Current event, past events and expired lifecycle statuses are excluded.
62. “Другие даты” is separated from recommendations.
    - Related/event cards must not use nested anchors: media/title/action links are separate, every card has a visual slot, and `.ics` calendar actions stay on the detail page instead of the card row.

## 10. P0 acceptance gates

63. Astro SSG generator exists and creates `/sobytiya/<stable-slug>/index.html` from production-like event export.
64. At least 5–10 future active events are generated for preview, including paid/free/registration/unknown/other-dates cases; production event pages target 10 preloaded discovery candidates where the future active catalog has enough eligible events.
65. Page is usable with JS disabled: event facts, description, CTA, calendar link and static recommendations remain available.
66. Mobile 375px: no horizontal scroll, CTA visible, touch targets at least 44px.
67. Desktop 1366px: real two-column layout, not stretched mobile feed.
68. `H1` wraps safely in main column for long Russian titles.
69. No `null`, empty badges, empty fact rows or broken image placeholders are rendered.
70. No layout shift from late image/recommendation/promo loading; hero dimensions are reserved.
71. CTA state follows the canonical Variant A matrix.
72. Ticket URL is valid, safely encoded and works as direct `href`.
73. `.ics` file exists, is linked without a forced `download` attribute, and is served as inline `text/calendar` for mobile calendar handoff.
74. `Event`/`MusicEvent` JSON-LD validates and matches visible facts.
75. Breadcrumb links and sitemap entries are generated and valid.
    - `/segodnya/` and `/vyhodnye/` are generated, linked from header and included in sitemap.
76. Preview pages are `noindex`; production pages are indexable.
77. Related freshness gate excludes current/past/expired events.
    - Related gate excludes `other_date_ids` / same occurrence group duplicates.
78. Same-origin `data/discovery/<event_id>.json` exists for each event and declares `preload_target=10`, `page_size=10`.
79. Promo is omitted or clearly labeled and separated.
80. Analytics are compact or disabled; no raw telemetry firehose in MVP.
81. Rollback path exists: previous static tree or disabling links to generated event pages.
