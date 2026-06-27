# Consultant Event Page MVP Review — 2026-06-27

> **Status:** external/user-supplied critical review of the event-page vertical slice after Variant A, Variant B and the first merged skeleton.
> **Primary decision impact:** confirms that no implementation exists yet in `events-bot-new`, tightens the first slice to Astro SSG/static HTML, removes hidden LLM fragments from P0, moves/limits promo, and reinforces that Variant A is canonical while Variant B is a UI/implementation donor.
> **Canonical implementation target after applying this review:** [event-page-merged-skeleton.md](event-page-merged-skeleton.md).

Краткий вердикт

Для первого vertical slice сильнее Variant A как канонический продуктовый контракт: он лучше держит MVP-границы, static-first, no-JS fallback, честную CTA-матрицу, anti-bubble, promo-disclosure, compact analytics и порядок реализации. Variant B сильнее как UI/UX/SEO wireframe для конкретного события 5878 «Песни СССР», потому что даёт 12-column desktop grid, sticky sidebar, HTML skeleton, .ics, share contract, fact block и конкретные SEO/schema примеры. Но B нельзя брать целиком: он тащит over-engineering и рискованные элементы — FAQ schema, gallery/lightbox, /api/v1/related, LLM-hidden fragments, потенциально галлюцинированные GEO/performer/duration facts и слишком широкий analytics surface. A прямо помечен как design для первого static vertical slice, а не implementation; B тоже помечен как независимый вариант, не implementation contract.

Итоговая формула правильная: A constraints + selected B implementation details. Существующий merged skeleton уже идёт в эту сторону: static HTML first, no required client-side recommendation fetch, H1 в main column, desktop 8/4 grid, mobile sticky CTA, high mobile search_digest, отдельный anti-bubble-блок и исключение FAQ/API/gallery как MVP over-engineering.

1. Какой вариант сильнее для первого vertical slice

A сильнее как основа первого slice. Он отвечает на главный вопрос MVP: как выпустить несколько реальных страниц /sobytiya/<slug>/, которые полезны без JS, имеют правильный CTA, static “Похожие события”, SEO/GEO/JSON-LD, preview noindex и production indexable режим. A также правильно говорит: не начинать с personalization backend, а сначала сделать static event page vertical slice на 5–10 будущих событиях, затем подключать related manifest/client island, потом write-path spike.

B сильнее как дизайн-референс для верстальщика. В нём есть конкретный mobile wireframe, desktop 12-column grid, sticky sidebar, CTA/share/ICS details, semantic HTML skeleton, P0 gates и риски для event 5878 «Песни СССР». Это полезно для реализации, потому что A местами остаётся “продуктовым контрактом”, а B показывает, как это может лечь в HTML/CSS.

Мой выбор: canonical spec = A; implementation skeleton = merged A+B; B использовать как component/wireframe donor, но не как источник MVP scope.

2. Порядок блоков mobile

A даёт более правильный продуктовый порядок: brand bar → poster → title/facts → CTA → calendar/share/copy → summary → description → other dates → related → more nearby/footer. Он удерживает mobile цель “decide quickly with one thumb” и отдельно запрещает visible rerank jump после поздней персонализации.

B добавляет полезные детали: breadcrumbs, hero with badges, date-time-location bar, map link, action bar, fact block, source block, related, anti-bubble, sticky CTA. Но B ставит gallery до description и добавляет FAQ; для первого slice это лишнее и может ухудшить time-to-decision.

Мой mobile order для MVP:

Brand bar: «Полюбить Калининград Анонсы».
Очень компактные breadcrumbs, только если не выталкивают event facts ниже первого viewport.
Hero image/poster с reserved dimensions; если image плохой или remote/hotlink — лучше аккуратный fallback.
Badges: тип, status, festival only if known.
H1: «Песни СССР».
Date/time: сб, 11 июля 2026, 21:30.
Venue/city/address: Янтарь-холл, Ленина 11, Светлогорск.
Ticket/status row: Билеты в продаже.
Primary CTA full width: Купить билет, real <a href>.
Secondary action row: .ics, share, copy link, optional map.
Short search_digest / 1–2 sentence summary.
Long description, sanitized; no required JS collapse in P0.
Fact block <dl>: тип, дата, площадка, город, возраст, длительность, Пушкинская карта, организатор — only known fields.
Source/provenance: source, organizer, last updated, temporary Telegraph link if dual-run.
Other dates, if same occurrence group exists.
Static “Похожие события”, visible without JS.
“Другие жанры рядом” as anti-bubble exploration.
Promo only if real campaign exists and clearly labeled; otherwise omit.
Footer.

Критическая правка к existing merged skeleton: promo лучше не ставить перед “Похожие события” на mobile в первом slice. Либо убрать полностью, либо показывать как labeled card после 2 organic related cards. Иначе страница может выглядеть как “сначала источник, потом реклама”, что снижает доверие.

3. Desktop grid/sidebar

Здесь B сильнее. Его 12-column model с main column 8 / sidebar 4 — хороший desktop-native вариант: не растянутый mobile, не Telegraph article, а нормальная страница афиши. A тоже описывает two-column layout и sticky decision rail, но B конкретнее по сетке и sticky behavior.

Правильный desktop merge:

Main shell max width около 1120–1200px.
Grid: 8 columns main + 4 columns sidebar.
H1 всегда в main column, не в sidebar.
Hero/media в main column.
Summary и long description в main column.
Sidebar только transactional: date/time, venue/address/map, ticket status, primary CTA, calendar/share/copy, compact facts.
Sidebar sticky только внутри event detail region; не должен конфликтовать с footer.
“Похожие события” лучше full-width grid ниже описания или main-width grid с 3 cards.
“Другие жанры рядом” отдельным H2-блоком ниже related.
Promo на desktop допустим в sidebar ниже CTA/facts или отдельной labeled card in related area, но не как первый actionable element.

Существующий merged skeleton это в целом фиксирует: desktop grid 8/4, sidebar only transactional, H1 in main, fact/action block in sidebar/mobile transaction block.

4. CTA / share / calendar

A сильнее как CTA-contract. Он покрывает paid ticket, registration, free with link, free without link, phone-only, source-only/unknown, sold out, cancelled/postponed. Особенно важно правило: free without link не должен становиться disabled/no-op button; это info badge, а не фейковая кнопка.

B полезен для implementation details: real <a> for ticket, .ics server-generated file, Web Share progressive enhancement, fallback share links, mobile sticky CTA after hero scrolls out.

Для MVP:

Primary CTA всегда real href, кроме status-only состояний.
Для event_id 5878, ticket_status=sale: primary CTA = Купить билет.
.ics должен работать без JS как обычная ссылка.
Share без JS может быть обычным Telegram/VK/mail link или visible copy URL; Web Share API — enhancement.
Copy link — JS enhancement; без JS не должен ломать action row.
Sticky mobile CTA — progressive enhancement; он дублирует тот же href, а не создаёт отдельную логику.
“Сохранить” удалить из P0: без авторизации/consent это спорный UX и лишний state.
5. SEO / GEO / schema / fact blocks

A сильнее как SEO contract: unique title, meta description from digest, canonical, OG/Twitter, JSON-LD matching visible facts, breadcrumbs, sitemap, preview noindex, and crawler contract: personalization cannot materially change page for bots or remove SEO-critical links.

B сильнее по конкретике: показывает MusicEvent, BreadcrumbList, visible fact block, address, offer, organizer, performer, duration. Но B опасен тем, что конкретные поля вроде coordinates, duration, performer, Pushkin card, organizer and FAQ answers должны попадать в JSON-LD только если они есть в canonical source-of-truth. Иначе сайт начнёт генерировать убедительную, но недоказанную SEO-разметку. B прямо включает координаты и rich facts для контрольного события; merged skeleton правильно ужесточает это правилом “GEO tags only where reliable; do not hallucinate coordinates”.

Для первого slice:

Use Event by default; MusicEvent only when category is reliable.
JSON-LD должен совпадать с видимым HTML.
No FAQ schema until Q&A generated from verified venue/event facts and reviewed.
Visible <dl> fact block — да.
Hidden LLM comment/meta blocks — нет в P0, максимум visible machine-readable facts.
llms.txt — post-MVP; не блокирует vertical slice.
Coordinates only if venue coordinates are already verified in DB.
Hero image preload only if local/proxied and dimensioned; otherwise no unsafe preload.
6. Риски персонализации и filter bubble

A заметно сильнее. Он сохраняет static fallback, разрешает только consented local rerank внутри existing candidate pool, запрещает online LLM/vector calls, запрещает replacing block with unrelated categories, запрещает visible reorder after engagement, отделяет other dates от related и вводит anti-bubble/diversity rules.

B даёт полезную идею отдельного блока “Другие жанры рядом”, но его /api/v1/related fetch нужно убрать из первого production slice. Даже если fetch non-blocking, он создаёт новую поверхность: API freshness, fallback logic, abuse, cache, latency, schema contract, and potential divergence between crawler/user content. Existing merged skeleton правильно говорит: no required client-side /api/v1/related fetch in the first production slice and no personalization-dependent content above the fold.

Правила для MVP:

“Похожие события” — static HTML fallback.
Local personalization после consent может только reorder/hide внутри static candidate pool.
No remote personalization API in first slice.
No personalization above the fold.
No visible jump after block is in viewport/read.
Current event excluded.
Other dates separated.
“Другие жанры рядом” — static anti-bubble exploration, not mixed into “Похожие”.
Explicit hide is hard veto.
Promo cannot override hide or audience exclusion.
7. Промо-кампании без потери доверия

A сильнее как trust policy: promo is not banner ad, it is event card with disclosure; at most 1 promo in first 6 related cards; never promote cancelled/past/wrong-city events; never override audience_exclusion_tags; explicit hide suppresses repeated promo; promo measured separately.

B полезен визуально: static HTML, no third-party JS in hot path, always visible label “Партнёр/Реклама”, no empty slot if no advertiser. Но B разрешает up to 2 promo slots, включая second optional slot; для первого slice это лишнее.

Моя рекомендация:

В первом static vertical slice промо лучше выключить, если нет реального коммерческого кейса.
Если промо нужно тестировать: один slot максимум.
Label always visible: Партнёр / Реклама, legal decision отдельно.
Никогда не ставить promo между H1/facts и primary CTA.
На mobile — лучше после 2 organic related cards, а не перед related.
На desktop — можно sidebar below facts/source или related-grid card, но visually separated.
Не смешивать unlabeled promo with organic recommendations.
Не делать complex promo frequency cap до telemetry/write-path spike.
8. Что удалить как over-engineering

Удалить или вынести post-MVP:

Required client-side /api/v1/related.
FAQ accordion.
FAQPage schema.
Multi-photo gallery/lightbox.
Hidden LLM-specific fragments/comments.
/llms.txt and public events JSON API as part of this slice.
Second promo slot.
Promo frequency cap before telemetry infra exists.
Full gallery analytics.
FAQ expand analytics.
Complex ticket_trust_level taxonomy; заменить простым source caption.
“Save” action.
Dark mode.
Full design-token system beyond minimal CSS variables.
Sticky CTA with blur/backdrop polish before basic mobile no-overlap is proven.
Map link as P0 if address reliability is weak; keep only when address exists.
Any structured data field not backed by canonical facts.
Any AI/LLM-oriented hidden content not visible to normal users.

Existing merged skeleton already excludes several of these: no required related API, no FAQ/FAQPage, no multi-photo gallery unless enough verified local/proxied media exists, no unlabeled promo and no personalization-dependent above-the-fold content.

9. P0-блокеры

Сейчас главный P0-блокер не дизайн, а отсутствие real vertical slice implementation. A прямо говорит, что документ — product/UI design and not implemented yet.

P0 before preview publication:

Реальный generator/build создаёт /sobytiya/<stable-slug>/index.html.
Есть 5–10 future active events из production data, включая paid/free/registration/unknown/other-dates cases.
Для 5878 «Песни СССР» CTA state maps to Купить билет.
Ticket URL валидируется и безопасно encoded; B отдельно отмечает риск спецсимволов/пробелов в ticket link.
Hero image local/proxied/dimensioned or graceful fallback; B отмечает риск протухающих external images.
Page works with JS disabled: facts, description, CTA, calendar, static related.
Mobile 375px: no horizontal scroll, CTA visible, touch targets ≥44px.
Desktop 1366px: real two-column layout, not stretched mobile feed.
.ics file exists and downloads.
Share/copy fallbacks do not break without JS.
JSON-LD validates and matches visible facts.
BreadcrumbList exists and links are not broken.
Sitemap includes generated pages.
Preview prefixes are noindex; production pages are indexable.
Related freshness gate excludes past/expired/current event.
Other dates separated from related.
No empty/null badges/fact rows.
No layout shift from late image/related/promo loading.
Promo omitted or clearly labeled.
Analytics are compact or disabled; no raw telemetry firehose.
No remote personalization/write path required for first preview.
Rollback path exists: previous static tree or disabling links.
10. Итоговый merged skeleton, 34 пункта
Page target: https://kenigevents.ru/sobytiya/<stable-slug>/.
Static HTML first; full event page usable without JS.
No auth required.
No LLM, embeddings, ML, recommendation API, or Supabase write path in first static page render.
<title>: {event.title} — {date}, {venue/city} | Полюбить Калининград Анонсы.
Meta description from search_digest, not raw long text.
Canonical URL with stable trailing slash.
Production pages indexable; preview prefixes noindex.
OG/Twitter tags with local/proxied hero image if available.
JSON-LD Event; MusicEvent only when category is reliable.
JSON-LD must match visible facts.
BreadcrumbList from best available taxonomy/city.
No hallucinated coordinates, duration, performer, organizer, or FAQ facts.
Header: compact brand bar + minimal nav Сегодня, Выходные.
Header/breadcrumbs must not push event facts below first mobile viewport.
Mobile: single column, no horizontal scroll, touch targets ≥44px.
Desktop: max-width shell, 8-column main + 4-column transactional sidebar.
H1 always in main content, never in sidebar.
Hero image fixed aspect ratio with reserved dimensions.
Badges only for known fields: type, lifecycle/status, festival.
Top facts: date/time, venue, city/address, ticket status.
Primary CTA real href: Купить билет, Зарегистрироваться, Позвонить, Уточнить у организатора, or status-only.
For ticket_status=sale and valid ticket_link, show Купить билет.
.ics calendar link works without JS.
Share uses Web Share API only as enhancement; fallback links remain.
Copy link is JS enhancement; no core dependency.
Mobile sticky CTA appears only after primary CTA leaves viewport and duplicates same href.
Sticky CTA reserves bottom/safe-area padding and never covers content.
search_digest appears high on mobile before long description.
Long description is sanitized and rendered as static HTML.
Fact block <dl> contains only non-empty verified fields.
Source/provenance and temporary Telegraph dual-run link appear below facts/description.
“Другие даты” shows same event occurrence group only.
“Похожие события” is static fallback HTML; 3–6 cards, current/past/expired excluded.
“Другие жанры рядом” is separate static anti-bubble block, 1–2 cards.
Personalization, when later enabled, may only rerank/hide within static pool after consent.
No visible reorder/jump after related block is in view.
Promo omitted by default; if present, one clearly labeled native/static card.
No FAQ/FAQPage in MVP.
No gallery/lightbox unless at least 2–3 verified local/proxied images exist.

Bottom line: merged skeleton is the right implementation target, but I would make promo even more conservative and explicitly move B’s FAQ/API/gallery/LLM fragments out of the first slice. The next useful step is not another review; it is generating 5–10 real static
