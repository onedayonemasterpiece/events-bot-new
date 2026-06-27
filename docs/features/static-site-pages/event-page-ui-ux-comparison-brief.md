# Event Page UI/UX Comparison Brief — Variant A vs Variant B

> **Status:** consultant comparison brief; no implementation decision yet.
> **Brand:** «Полюбить Калининград Анонсы».
> **Target page:** `https://kenigevents.ru/sobytiya/<stable-slug>/`.
> **Control event:** production event `5878` — «Песни СССР», 2026-07-11 21:30, Янтарь-холл, Светлогорск.

## Documents under comparison

1. **Variant A — internal product/design spec:** [event-page-product-design.md](event-page-product-design.md)
2. **Variant B — independent Opus UI/UX variant:** [opus-event-page-ui-ux-2026-06-27.md](opus-event-page-ui-ux-2026-06-27.md)
3. **Gemini comparison review supplied by the user:** [gemini-event-page-comparison-2026-06-27.md](gemini-event-page-comparison-2026-06-27.md)
4. **Merged implementation skeleton:** [event-page-merged-skeleton.md](event-page-merged-skeleton.md)

Current merged decision: implement Variant A's static/MVP constraints, adopt Variant B's desktop 8/4 grid, sticky mobile CTA, dedicated “Другие жанры рядом” anti-bubble block, separate labeled promo slot, and concrete SEO/GEO/schema/fact-block mechanics. Keep `H1` in the main content column, not in the desktop sidebar.

Related architecture context:

- Static-site feature index: [README.md](README.md)
- Event detail related-events contract: [../unsigned-personalization/event-detail-related.md](../unsigned-personalization/event-detail-related.md)
- Personalization production gate: [../unsigned-personalization/production-integration.md](../unsigned-personalization/production-integration.md)
- Opus architecture gate review: [../unsigned-personalization/opus-review-2026-06-27.md](../unsigned-personalization/opus-review-2026-06-27.md)

## Concrete production fixture used for Variant B

Primary event:

- `event_id`: `5878`
- title: «Песни СССР»
- type: concert / концерт
- festival: «Симфония ветра»
- date/time: 2026-07-11 21:30
- venue: Янтарь-холл, Ленина 11, Светлогорск
- status: active, ticket_status `sale`, ticket_trust_level `high`
- ticket link: `https://янтарьхолл.рф/afisha/pesni-sssr%2011%2007/`
- current Telegraph page: `https://telegra.ph/Koncertnaya-programma-Pesni-SSSR-06-10`
- summary: музыкальная программа со знаковыми произведениями советских композиторов и поэтов, вокалистами и инструментальным ансамблем.

Related-event candidates from current manifest/production probe:

- `3000` — «Моя Мишель», 2026-08-08, Янтарь-холл, Светлогорск, music.
- `4040` — «ЛЮБОВНИЧКИ 2 ПРОДОЛЖЕНИЕ», 2026-06-30, Янтарь-холл, theatre/diversity.
- `5495` — «Вадим Самойлов: Агата Кристи с оркестром», 2026-06-27, same venue/music, must pass freshness gate after event date.
- `3730` — «Симфоническая пятница», date range 2026-06-26—2026-07-13, Калининград, exhibition+music/diversity.
- `6326` — «Экскурсия “В потоке времени”», 2026-06-26, museum/excursion, must be removed after date expires.

## Initial comparison matrix

| Area | Variant A — internal spec | Variant B — Opus independent variant | Open decision |
| --- | --- | --- | --- |
| Product framing | “Fast local event decision page”; strong mapping from Telegraph baseline and project architecture. | “No Dead End”; explicitly models 3 entry paths and four first-screen questions. | Combine A’s lifecycle/architecture rigor with B’s first-screen decision checklist. |
| Mobile layout | Mobile decision block + sticky bottom CTA; simpler page anatomy. | More detailed mobile sequence with hero, action bar, gallery, fact/source/promo/related/FAQ. | Decide whether gallery is above or below short explanation on mobile. |
| Desktop layout | Desktop-native, related modules and action rail; clear separation from mobile feed behavior. | Concrete 12-column grid with sticky sidebar and no desktop bottom sticky CTA. | B’s sticky sidebar is implementation-friendly and likely should be adopted. |
| CTA model | Broad state matrix: ticket/register/free/phone-only/source-only/sold-out/cancelled. | Concrete graceful degradation: `.ics`, Web Share API fallback, ticket trust level, source domain caption. | A’s state matrix should govern; B’s interaction details should enrich implementation. |
| Related/personalization | Tied to static fallback + consented rerank + anti-bubble/promo/analytics architecture. | Freshness gate, “Другие жанры рядом”, two partner slots. | Need one final related module taxonomy: similar / nearby / exploration / partner. |
| SEO/GEO | Covers canonical, sitemap, JSON-LD, answer blocks, llms-friendly facts, lifecycle policy. | Gives concrete `MusicEvent`, `FAQPage`, `BreadcrumbList`, OG/Twitter and machine-readable fragments. | Use B’s concrete snippets under A’s lifecycle/deletion policy. |
| Analytics | Compact aggregate counters and page/CTA/share/related event contract. | 14 event names; warns against telemetry firehose. | Need reduce to a stable v1 analytics dictionary before implementation. |
| Visual design | Warm editorial local guide: amber/teal, off-white, calm card UI. | “Baltic minimalism”: neutral stone, blue CTA, explicit design tokens and states. | Need choose final palette; A feels more brand-local, B has more complete state tokens. |
| Promo | Requires marked promo without breaking trust. | Native partner slots, labeled «Партнёр», placement after source and in sidebar. | Define hard rules: no fake related ranking, no unlabeled promo, frequency caps. |
| Implementation readiness | Strong acceptance gates for first vertical slice. | Includes HTML skeleton and P0 gates for developers. | B’s skeleton can become implementation fixture after final merge. |

## Copy-paste prompt for external consultants

Use this prompt when sending the two variants to a human/external consultant:

```text
You are reviewing UI/UX, SEO and GEO design for a static event page of a local events service.

Brand/site: «Полюбить Калининград Анонсы», kenigevents.ru.
Target page: https://kenigevents.ru/sobytiya/<stable-slug>/.
MVP: no login; static HTML page, later consented anonymous personalization via same-origin JSON/RPC. LLM must not be in the page hot path. Mobile should feel like a convenient event feed/card; desktop should be desktop-native, not a stretched mobile feed. The page must support ticket/register CTA, add to calendar, mobile share, copy link, source/organizer, related events, anti-filter-bubble exploration, promo slots, analytics, SEO/GEO.

Control production event:
- event_id 5878, «Песни СССР»
- 2026-07-11 21:30
- Янтарь-холл, Ленина 11, Светлогорск
- concert, festival «Симфония ветра»
- ticket_status=sale, ticket_trust_level=high
- ticket link: https://янтарьхолл.рф/afisha/pesni-sssr%2011%2007/
- Telegraph baseline: https://telegra.ph/Koncertnaya-programma-Pesni-SSSR-06-10
- summary: советская музыкальная программа с вокалистами и инструментальным ансамблем.

Please compare two design variants:
A) Internal product/design spec: docs/features/static-site-pages/event-page-product-design.md
B) Independent Opus variant: docs/features/static-site-pages/opus-event-page-ui-ux-2026-06-27.md

Be critical and concrete. Please answer:
1. Which variant is stronger for first vertical slice and why?
2. Which exact blocks/order should be kept for mobile?
3. Which exact blocks/grid should be kept for desktop?
4. What CTA/share/calendar behavior is missing or risky?
5. What SEO/GEO/schema/fact-block pieces are required for launch?
6. Where could personalization create filter bubble or privacy/trust issues?
7. How should promo campaigns be inserted without damaging trust?
8. What should be removed from either variant as over-engineering?
9. What are P0 blockers before implementation?
10. Provide a final merged recommended page skeleton in 20–40 bullets.
```

## Gemini Pro review status

Gemini Pro critical comparison must use only `gemini-3-pro-preview` or `gemini-3.1-pro-preview`. Lower Gemini/Flash/Gemma outputs are not accepted as consultant review for this gate.

Result on 2026-06-27: **blocked by Gemini API quota**. Both allowed models were tried across 4 configured keys and returned HTTP 429 `RESOURCE_EXHAUSTED`; lower-tier fallback was intentionally not used.

The attempt evidence for this specific UI/UX comparison is recorded in artifacts, not committed:

- `artifacts/codex/static-event-page-ui-ux-2026-06-27/gemini_pro_ui_ux_comparison_attempts_2026-06-27.md`
- `artifacts/codex/static-event-page-ui-ux-2026-06-27/gemini_pro_ui_ux_comparison_attempts_2026-06-27.json`

If Gemini Pro is quota-blocked, the decision remains pending; do not substitute Flash/Lite output as a completed Gemini consultant review.
