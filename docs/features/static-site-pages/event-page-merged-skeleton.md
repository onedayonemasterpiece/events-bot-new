# Event Page Merged Skeleton — «Полюбить Калининград Анонсы»

> **Status:** implementation target for the first static event-page vertical slice after Variant A/B comparison and user-provided Gemini review.
> **Source reviews:** [Variant A product/design spec](event-page-product-design.md), [Variant B Opus UI/UX variant](opus-event-page-ui-ux-2026-06-27.md), [Gemini comparison review](gemini-event-page-comparison-2026-06-27.md).
> **Control event:** production event `5878` — «Песни СССР», 2026-07-11 21:30, Янтарь-холл, Светлогорск.

## 1. Decision summary

The first vertical slice uses **Variant A constraints** and **selected Variant B UI/SEO ideas**:

- keep the page **static HTML first**; no required client-side recommendation fetch for launch;
- keep **H1 in the main content column**, never in the desktop sidebar;
- use Opus/B style **desktop 8/4 grid**, but sidebar is only transactional: date, place, status, CTA, actions, facts;
- use **mobile sticky bottom CTA** after the main CTA scrolls out;
- keep `search_digest` high on mobile, before long description;
- show anti-bubble exploration as a separate static block **«Другие жанры рядом»**, not mixed into “Похожие”;
- promo is a separate native block marked **«Партнёр»**, never an unlabeled recommendation;
- omit MVP over-engineering: client-side related API, FAQ schema, complex client promo frequency cap, photo gallery unless enough verified media exists.

## 2. Head & SEO/GEO

1. `<title>`: `{event.title} — {date}, {venue/city} | Полюбить Калининград Анонсы`.
2. `canonical` URL: `https://kenigevents.ru/sobytiya/<stable-slug>/`.
3. `robots`: indexable for future/active production pages; preview prefixes remain `noindex`.
4. Meta description from `search_digest` / `short_description`, not raw long description.
5. Open Graph and Twitter Cards with event title, short description and hero poster URL.
6. `schema.org/Event` or narrower type such as `MusicEvent` when category is reliable.
7. `schema.org/BreadcrumbList`.
8. GEO tags where reliable: region/city and coordinates only if actual venue coordinates exist; do not hallucinate coordinates.
9. LLM-friendly machine-readable fact block in visible HTML (`<dl>`) plus optional short HTML comment if it is generated from the same canonical fields.
10. Preload the hero poster only when it is local/proxied and dimensioned; otherwise avoid unsafe third-party hotlink preloads.

## 3. Global header

11. Brand bar: «Полюбить Калининград Анонсы».
12. Compact navigation for MVP: `Сегодня`, `Выходные`; later category/date/search links.
13. Header must not push event facts below the first mobile viewport.

## 4. Layout

14. Mobile: single column, no horizontal scroll, touch targets at least 44px.
15. Desktop: max-width content shell, 12-column grid: main content 8 columns, sidebar 4 columns.
16. Desktop sidebar is sticky only inside the event detail section; it must not contain `H1`.
17. Tablet can collapse to 2-column or single-column at the implementation breakpoint, but must preserve CTA visibility.

## 5. Main content column

18. Breadcrumbs: `Афиша > Светлогорск > Концерты` or best available taxonomy.
19. `H1` event title in the main column on all viewports.
20. Hero image with fixed aspect ratio (`16:9` or `16:10`) and reserved dimensions to prevent CLS.
21. Badges: event type, lifecycle/status, festival if known; do not render empty/null badges.
22. Mobile-only/high-priority summary: `search_digest` in 1–2 sentences before the long description.
23. Long description rendered from trusted markdown/html sanitization.
24. “Другие даты” for linked occurrences of the same event group, if present and future/active.
25. Source/provenance: original source/organizer and temporary Telegraph compatibility link during dual-run.
26. Promo slot #1: static/native, clearly marked «Партнёр», visually separated.
27. `H2` “Похожие события”: 3–4 static cards by same/near venue, genre, time and lifecycle freshness.
28. `H2` “Другие жанры рядом”: 1–2 static exploration cards for anti-filter-bubble discovery.

## 6. Sidebar / mobile transaction block

29. Date and time, with timezone implied by region or explicitly generated.
30. Venue, city, address and map link when address is reliable.
31. Primary CTA as a real `href`: `Купить билет`, `Зарегистрироваться`, `Позвонить`, `Уточнить у организатора`, or status-only for sold-out/cancelled.
32. Status indicator: tickets in sale, free, sold out, cancelled/postponed; use text plus color/icon, not color alone.
33. Action bar: `.ics` calendar link, share, copy link.
34. Fact block as `<dl>`: type, duration, age limit, Pushkin card, organizer — only fields that exist.
35. On desktop this block is sticky sidebar; on mobile it is inline near the top plus sticky CTA duplicate.

## 7. Mobile sticky CTA

36. Sticky bottom CTA appears when the primary CTA leaves the viewport.
37. It must reserve safe-area padding and must not cover footer/action content.
38. It duplicates the same `href` as the primary CTA, so the core action works without JS.
39. JS-only enhancement: copy-link toast and Web Share API; fallback is a normal copy/share link or visible URL action.

## 8. MVP exclusions

40. No required client-side `/api/v1/related` fetch in the first production slice.
41. No FAQ accordion or `FAQPage` schema until structured Q&A is generated and reviewed.
42. No multi-photo gallery unless generator has at least 2–3 quality, local/proxied, dimensioned images.
43. No unlabeled promo mixed into organic recommendations.
44. No personalization-dependent content above the fold.

## 9. P0 acceptance gates

45. Page is usable with JS disabled: event facts, description, CTA, calendar link and static recommendations remain available.
46. H1 stays in the main column on desktop and wraps safely for long Russian titles.
47. No `null`, empty badges, empty fact rows or broken image placeholders are rendered.
48. No layout shift from late image/recommendation/promo loading; hero dimensions are reserved.
49. CTA state follows the canonical state matrix from Variant A.
50. Freshness gate removes past/expired related events from production recommendations.
51. `Event`/`MusicEvent` JSON-LD validates with canonical URL, date, status and place.
52. Promo block is labeled and visually distinct.
53. Analytics are aggregate and compact; no raw telemetry firehose in MVP.
