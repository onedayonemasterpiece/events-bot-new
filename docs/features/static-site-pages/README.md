# Static Site Event Pages

> **Status:** Astro SSG preview implemented; production rollout pending  
> **Scope for MVP:** только публичные страницы **событий** на `kenigevents.ru`  
> **Out of scope for MVP:** экскурсии как отдельный домен, авторизация, перенос core event DB в Supabase, полная миграция всех Telegraph surfaces.

## Implementation status

В `events-bot-new` теперь есть первый **Astro SSG preview vertical slice** в `site/`: он строит статические страницы событий, `event.ics`, `sitemap.xml`, `robots.txt` и опубликован под noindex-prefix в bucket `kenigevents.ru`. Это ещё не production rollout: fixture пока компактный, canonical preview-safe, а корневые production URL `/sobytiya/<slug>/` не включены.

Текущий preview реализует production-oriented форму по паттерну соседнего `kgd80/site`: committed production-like fixture → `getStaticPaths()` → `/segodnya/`, `/vyhodnye/`, `/sobytiya/<stable-slug>/index.html` → `event.ics` → `data/discovery/<event_id>.json` → sitemap/robots/JSON-LD → preview `noindex` → publish to Yandex Object Storage bucket `kenigevents.ru`. Следующий шаг — заменить fixture на регулярный export из Fly SQLite/static page manifest и включить production canonical после canary-gate.
Для медиа export обязан передавать `image_text_mode` из существующего OCR/media-контура: `ocr_text` и `unknown` рендерятся в натуральном соотношении без crop/backdrop, `visual_only` допускает cover-кроп в вертикальной 4:5 карточке. Astro build сам OCR не запускает.

## Текущий публичный preview

- Preview index: <https://kenigevents.ru/preview-20260627-event-pages-v16/__preview/>
- Today listing: <https://kenigevents.ru/preview-20260627-event-pages-v16/segodnya/>
- Weekend listing: <https://kenigevents.ru/preview-20260627-event-pages-v16/vyhodnye/>
- Control event `5878`: <https://kenigevents.ru/preview-20260627-event-pages-v16/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Control ICS: <https://kenigevents.ru/preview-20260627-event-pages-v16/sobytiya/pesni-sssr-svetlogorsk-5878/event.ics>
- Control discovery JSON: <https://kenigevents.ru/preview-20260627-event-pages-v16/data/discovery/5878.json>
- Sitemap: <https://kenigevents.ru/preview-20260627-event-pages-v16/sitemap.xml>
- Robots: <https://kenigevents.ru/preview-20260627-event-pages-v16/robots.txt>
- Website endpoint fallback: <http://kenigevents.ru.website.yandexcloud.net/preview-20260627-event-pages-v16/__preview/>

Preview `v16` keeps the consultant P0 hardening and explicit discovery feedback: visible description, one vertical neutral `Смотрите дальше` feed, no user-facing “try another genre” block, large right-thumb like buttons with counts and unlike, “Не интересно” negative feedback, single native-share-first button, favicon, prefetch for static links, and sticky CTA hiding when the user reaches the feed. Media rule after the v15 fix: selected preview images with **no meaningful OCR** (`visual_only`) use a strict vertical `4:5` cover frame; this is now enforced for `С чего начинается Родина`, `Симфоническая пятница`, `День валяния в сене`, `Точка и линия` and `Пионеры советской археологической науки`. `ocr_text` and `unknown` images in hero/card/listing render as the actual image in its natural aspect ratio, with no crop, no fixed frame, no `contain` over a duplicate underlay, no blur/backdrop fill and no repeated image edges. Each event page statically preloads up to 10 continuation candidates in HTML; after JS starts, the page uses only a consented compatible local profile (`ke_personalization_profile`, UUID `anon_id/session_id`, `event-detail-related-v1` + `event-taxonomy-v1`) to filter/rerank. The client removes already hidden / `not_interested` / strong negative-interest matches from the preloaded cards, performs one same-origin JSON hydration from `/data/discovery/<event_id>.json`, where the payload is an `event_detail_related` manifest with `related_static[]`, and top-ups relevant candidates; subsequent expansion is only by `Показать ещё`. The legacy display-only JSON and `ke_event_feedback_state_v1` state are no longer the active contract. Local strong actions write a compact browser log with `served_list_id` / `served_list_hash` context for future Supabase telemetry mapping. Same-year visible dates omit the year; the year is kept only for non-current-year dates and cross-year ranges. Feed-card calendar buttons were removed because the three-action row did not fit reliably on mobile; calendar remains on the event detail page / primary action area. Cards are full-clickable for users while keeping real HTML links on media/title for SEO/GEO; double-tap like is disabled because it conflicted with navigation. Likes still animate from the explicit like button. `Не интересно` turns the acted-on card into a grey explanatory plate until the next page/reload. Visible like/share counters are hidden when the total is zero. Visible like counts are honest totals: `likes_count = source_likes_count + service_likes_count`, where source likes come from production TG/VK post metrics and service likes are first-party KenigEvents likes; public HTML/UI shows only this total, not the technical source/service split.

Build/runbook: `docs/features/static-site-pages/astro-preview.md`. Reaction counter architecture: `docs/features/static-site-pages/reaction-counters.md`.

## Цель продукта

Перейти от ограниченных `telegra.ph`-страниц к собственным статическим страницам событий на `kenigevents.ru`, чтобы:

- дать пользователю быстрый мобильный landing события с нормальной навигацией и CTA;
- улучшить SEO/GEO и видимость в AI/search за счёт собственного HTML, `canonical`, sitemap и JSON-LD;
- снизить время пользователя на поиск интересного события по сравнению с конкурентами;
- сохранить стабильность: страница должна открываться и индексироваться даже если динамическая персонализация или Supabase временно недоступны.

Главная продуктовая метрика персонализации: **меньше времени до нахождения релевантного события**, а не максимальный CTR любой ценой.

## Принятые решения на MVP

- Домен: `kenigevents.ru`.
- Публикуем только **будущие активные события**.
- Авторизации в MVP нет.
- Anonymous personalization допустима с простым consent/banner: пользователь подтверждает “ОК”, после чего включается аналитика/персонализация.
- Telegraph остаётся временным compatibility/fallback layer примерно на **1 месяц** после включения статических страниц.
- Доступы к Yandex Cloud/Object Storage будут выданы отдельным шагом; до этого проектируем контракт и пайплайн без привязки к конкретным credentials.

## Связанные документы

- Astro SSG preview runbook and public URLs: `docs/features/static-site-pages/astro-preview.md`.
- Исторический backlog/research: `docs/backlog/features/static-event-pages/README.md`.
- Anonymous personalization: `docs/features/unsigned-personalization/README.md`.
- MVP-0 related recommendations surface: `docs/features/unsigned-personalization/event-detail-related.md`.
- Product/UI spec for the first event page vertical slice: `docs/features/static-site-pages/event-page-product-design.md`.
- Independent Opus UI/UX variant for the event page: `docs/features/static-site-pages/opus-event-page-ui-ux-2026-06-27.md`.
- Consultant comparison brief for Variant A vs Variant B: `docs/features/static-site-pages/event-page-ui-ux-comparison-brief.md`.
- Gemini comparison review supplied by the user: `docs/features/static-site-pages/gemini-event-page-comparison-2026-06-27.md`.
- External MVP review after the first merged skeleton: `docs/features/static-site-pages/consultant-event-page-mvp-review-2026-06-27.md`.
- Traceability matrix showing how the consultant review was applied: `docs/features/static-site-pages/consultant-review-application-matrix-2026-06-27.md`.
- Merged implementation skeleton for the first page build: `docs/features/static-site-pages/event-page-merged-skeleton.md`.
- Interface reference board for event detail and continuation blocks: `docs/features/static-site-pages/interface-references.md`.
- Bot/automation contract for personalization-safe static pages: `docs/features/unsigned-personalization/bots-and-automation.md`.
- Production integration plan for personalization, promo, Smart Update rebuilds, analytics and CTA: `docs/features/unsigned-personalization/production-integration.md`.
- Исследовательская заметка по рекомендациям/LLM: `docs/features/unsigned-personalization/alanytics.md`.
- Dual DB routing skill: `.codex/skills/events-bot-dual-db/SKILL.md`.

## Локальный опыт kdg80

Локальный проект-референс находится рядом на сервере: `/home/dev/projects/kdg80`.
Его нужно использовать как фактический опыт реализации static-first event/program site, но код и документы новой фичи пишутся в `/home/dev/projects/events-bot-new`.

Ключевые файлы kdg80:

- `site/package.json` — Astro `^6.0.5`, Node `>=22.12.0`, build `prepare_public_assets -> astro build -> verify_public_assets`;
- `site/src/pages/sobytiya/[slug].astro` — leaf event pages через `getStaticPaths()`;
- `site/src/layouts/Layout.astro` — общий SEO/GEO/OG/JSON-LD layout;
- `site/src/components/EventCard.astro` — переиспользуемая карточка события для программы и leaf page;
- `site/src/lib/festival.ts` — parser/data contract, stable slug overrides, date/status helpers, ICS links;
- `site/src/components/RegistrationClient.astro` — пример client-side island поверх static HTML;
- `deploy-to-yc.sh` — S3-compatible deploy в Yandex Object Storage;
- `deploy-preview-to-yc.sh` — secret preview prefix с rewrite root-absolute URLs, canonical/OG и `noindex`.

Что уже хорошо сработало и нужно переиспользовать как паттерн:

- **Astro SSG + flat HTML.** Все event pages строятся как статические nested routes (`/sobytiya/<slug>/index.html`).
- **Landing-first hybrid.** Основной UX может жить на главной/программе, а leaf event pages дают SEO/share/JSON-LD слой.
- **Один event-card component.** Карточка события переиспользуется в программе, thematic routes и leaf page, чтобы не плодить расходящиеся шаблоны.
- **Stable slug survival.** В `festival.ts` есть `FIXED_EVENT_SLUGS`, чтобы URL переживал правку заголовка.
- **Preview без авторизации.** Secret prefix + отсутствие ссылок с корня + `noindex` оказались практичнее, чем закрывать preview auth-слоем.
- **Preview rewrite обязателен.** Простая загрузка сырого `site/dist` в подпапку ломает CSS/assets/canonical; нужен rewrite root-absolute URLs под preview prefix.
- **Same-origin dynamic state files.** Для read-heavy динамики kdg80 использует/планирует state manifest вроде `/tickets/registration/states.json`, а API остаётся fallback, чтобы не дергать backend на каждую карточку.
- **Asset gate.** `prepare_public_assets.mjs` и `verify_public_assets.mjs` нормализуют WebP и запрещают случайные PNG/JPG в public/dist.
- **Client-side dynamics не блокирует SEO.** Регистрация/статусы/спецCTA подгружаются после HTML; страница остаётся полезной без JS/API.

Что нельзя переносить без доработки:

- У kdg80 текущий root `sitemap.xml` содержит только главную, хотя `site/dist` генерирует много event pages. Для `kenigevents.ru` sitemap должен строиться автоматически по всем canonical event/listing URLs.
- У kdg80 часть данных и overrides зашита в TypeScript/Markdown master-файлы. Для `kenigevents.ru` source of truth — Fly SQLite, поэтому нужен формальный export contract, а не ручной master-файл.
- Яндекс.Метрика в kdg80 подключена сразу в layout без consent-gate. Для нового MVP с anonymous personalization нужен consent/banner policy до персонализационной telemetry.
- kdg80 — фестивальный сайт; `kenigevents.ru` — постоянно обновляемая афиша. Нужны lifecycle/retention/rebuild правила, которых в разовом фестивальном сайте меньше.

Вывод для `kenigevents.ru`: берем **Astro SSG + landing/listing + event leaf pages + preview-prefix deploy + manifest-first динамику**, но добавляем автоматический event export, sitemap builder, deletion/retention policy и Supabase personalization boundary.

## Архитектурный принцип

Страница события должна быть **static-first**:

```text
Fly SQLite /data/db.sqlite
  → export/build contract для будущих active events
  → Astro static renderer
  → HTML + assets + sitemap + robots
  → Yandex Object Storage/CDN
  → kenigevents.ru

Supabase/Postgres personalization DB
  → anonymous visitors/sessions
  → analytics events
  → profile snapshots
  → recommendation cache/RPC
```

Критичный SEO/GEO контент должен быть в готовом HTML. Client-side JS разрешён только для улучшения опыта после первого рендера: consent, analytics, localStorage profile, personal feed. Search/preview/AI crawlers and suspicious automation receive the genuine static fallback and must not influence personalization telemetry/training (`docs/features/unsigned-personalization/bots-and-automation.md`).

## Граница двух БД

### Fly SQLite — источник истины для событий

Хранить здесь:

- canonical `Event` и связанные факты/источники;
- `telegraph_url`, публикации TG/VK/Telegraph;
- joboutbox/scheduler state;
- будущую metadata статических страниц: slug, canonical URL, content hash, last built/published time, status.

### Supabase/Postgres — только персонализация и telemetry

Хранить здесь:

- anonymous visitor/session ids;
- interaction events: page view, impression, dwell, ticket click, hide;
- short/mid/long profile snapshots;
- recommendation cache/debug snapshots;
- E2E personas для проверки чужих персонализаций.

Запрещено молча переносить core events в Supabase или telemetry в Fly SQLite.

## URL и identity

Модель для MVP:

- canonical event URL: `https://kenigevents.ru/sobytiya/<stable-slug>/`;
- slug должен переживать правку заголовка;
- если slug меняется, нужен redirect из старого URL;
- одна страница соответствует конкретному event occurrence/date;
- `linked_event_ids` используются для блока “Другие даты”, но не склеивают разные даты в один canonical URL.

Нужны отдельные правила для:

- отмены события;
- переноса;
- смены площадки;
- sold out;
- merge/split события;
- удаления события из будущей выдачи.

## Контентный contract event page

Минимальный первый экран:

- название;
- дата, время, timezone;
- город, площадка, адрес;
- статус: active/cancelled/postponed/sold out;
- цена/free/диапазон цены;
- CTA: билет/регистрация/источник;
- обложка/постер;
- короткое описание или `search_digest`;
- provenance: источник/последнее обновление.

Ниже:

- полное описание;
- фото/видео, если доступны;
- “Другие даты”;
- “Похожие события” — статический fallback и первый MVP-0 personalization surface (`event_detail_related`);
- персональная лента/главная после consent и client-side hydration — later, не стартовый MVP-0.

## Discovery UX: mobile feed vs desktop-native layout

В требованиях слово **«лента» означает именно мобильный паттерн**: на телефоне пользователь ожидает вертикальный, быстрый, thumb-friendly feed карточек. Это не означает, что desktop должен быть растянутой мобильной бесконечной лентой.

Требуемое поведение по viewport:

- **Mobile (`<768px`)** — основная discovery surface это вертикальная лента:
  - карточка почти на всю ширину, крупная обложка, дата/время/место/CTA видны без точного попадания;
  - подгрузка чанками или infinite-like feed допустима, но с честным fallback и без блокировки первого экрана;
  - фильтры/темы/даты должны быть доступны как chips или bottom-sheet, а не как desktop sidebar;
  - персонализация сильнее учитывает scroll depth, impressions, quick-skip, dwell, tap/card click, hide/not interested, ticket/share/copy.
- **Desktop (`>=1024px`)** — ожидаемый desktop UX:
  - grid/list с нормальной плотностью, видимыми фильтрами, поиском, датами, категориями и/или правой колонкой;
  - персонализация проявляется как порядок внутри grid/list, блоки «Рекомендуем вам», «Похоже на просмотренное», «Сегодня/выходные для вас», а не только как бесконечная лента;
  - hover/focus можно писать как слабый сигнал интерфейса, но нельзя делать критичные действия hover-only и нельзя переобучаться на случайные hover events;
  - desktop должен поддерживать открытие карточек в новой вкладке, back с сохранением scroll/filter state и понятные breadcrumbs.
- **Tablet (`768–1023px`)** — адаптивный промежуточный режим: чаще 2-column grid + mobile-like chips/bottom filters; точный layout фиксируется на UI prototype этапе.

Единый профиль интересов может быть общим, но telemetry и ранжирование обязаны различать surface:

```text
viewport_class = mobile | tablet | desktop
layout_mode    = feed | grid | list | module
surface        = home_feed | event_detail_related | category_page | date_page | search_results
position       = index/card slot within current surface
algorithm_id   = static_fallback | local_rerank_v1 | rpc_personal_v1 | experiment_key
```

Reference board for page/continuation mechanics: `docs/features/static-site-pages/interface-references.md`. It is a comparison checklist, not proof of usability; mobile/desktop layouts still need a real prototype review.

Acceptance criteria для первой реализации:

- на 375px нет горизонтального scroll, touch targets не меньше 44px, primary CTA не прячется под fixed UI;
- на desktop 1366/1440px нет ощущения «мобильной карточки на всю ширину», фильтры и контекст видимы без лишнего открытия;
- все ключевые экраны deep-linkable, back сохраняет scroll/filter state;
- метрики персонализации считаются отдельно по `viewport_class/layout_mode`, иначе нельзя понять, ускорили ли мы поиск на телефоне и не ухудшили ли desktop.

## SEO/GEO contract

Для каждой event page:

- `<title>`;
- meta description;
- canonical URL;
- Open Graph/Twitter preview;
- JSON-LD `schema.org/Event`;
- breadcrumbs/internal links;
- `lastmod` в sitemap;
- корректный HTTP status.

Минимум JSON-LD:

- `@type: Event`;
- `name`;
- `description`;
- `startDate`;
- `endDate`, если известен;
- `eventStatus`;
- `eventAttendanceMode`;
- `location`/`Place`;
- `image`;
- `offers` или явная ссылка на tickets/signup;
- `organizer`/source, если известен.

Google Event structured data требует добавлять required properties, валидировать Rich Results Test, деплоить несколько страниц и проверять через URL Inspection; будущие изменения рекомендуется доносить через sitemap. См. официальную документацию: <https://developers.google.com/search/docs/appearance/structured-data/event>.

## Политика прошедших/удалённых событий

Текущий scope — публиковать будущие события. Для удаления нужна отдельная проверенная политика, потому что поисковые системы по-разному и не мгновенно снимают URL из выдачи.

Предварительная безопасная политика:

1. **До события:** страница индексируемая, в sitemap.
2. **После события:** не удалять мгновенно. Минимум 7–30 дней оставить страницу доступной как “событие прошло” с `EventCompleted`, убрать из активных лент, оставить canonical и внутреннюю перелинковку ограниченно.
3. **После retention:**
   - если страница имеет поисковую ценность/историю/медиа — оставить архивной, но убрать из активных event sitemap или переместить в archive sitemap;
   - если страница тонкая/ошибочная/дубликат — вернуть `410 Gone` или `404`, удалить из sitemap, при необходимости отправить removal в Yandex/Google Webmaster/Search Console.
4. **Нельзя:** просто удалить HTML из bucket без осознанного статуса и sitemap update.

Основания:

- Google Removals tool даёт быстрые временные удаления, но для постоянного удаления нужно удалить/обновить контент, закрыть доступ или поставить `noindex`; Google отдельно предупреждает не использовать `robots.txt` как способ блокировки страницы от выдачи: <https://developers.google.com/search/docs/crawling-indexing/remove-information>.
- Для `noindex` Google требует, чтобы страница была доступна crawler’у и не была заблокирована `robots.txt`: <https://developers.google.com/search/docs/crawling-indexing/block-indexing>.
- Yandex указывает, что 404/403/410 удаляются из поиска после обнаружения роботом, а ускорение возможно через Yandex Webmaster; для `noindex` также нельзя блокировать страницу в `robots.txt`, иначе робот не увидит инструкции: <https://yandex.com/support/webmaster/en/yandex-indexing/removing-from-index>.

Открытый вопрос: точный retention для прошедших событий `kenigevents.ru` — 30/60/90 дней или архив навсегда для качественных страниц. До финального решения дефолт проектирования: **оставлять прошедшую страницу доступной минимум 30 дней**, убрать её из активных лент сразу после окончания события, а sitemap-размещение менять по правилам archive/retention.

## MVP-0: event page related block

Первый проверочный шаг персонализации — страница конкретного события, а не главная лента:

```text
/sobytiya/<slug>/
  -> static HTML event page
  -> static “Похожие события” block
  -> optional local rerank after consent
```

Требования к static site renderer:

- при build/export для каждого future active event подготовить `related_static` candidates;
- HTML должен показывать fallback related block без JS/Supabase;
- “Другие даты” рендерятся отдельным блоком и не смешиваются с “Похожие события”;
- client island может после consent переупорядочить уже полезный block, но не должен ломать CTA/SEO;
- desktop рендерит related как 3-column visual grid/module, mobile — горизонтальный snap-scroll poster rail с `.ics`/open/share actions; не текстовый список и не бесконечный TikTok-feed.

Детальный contract: `docs/features/unsigned-personalization/event-detail-related.md`.

## Personalization MVP на статической странице

Персонализация — enhancement поверх static-first сайта и должна учитывать различие mobile/desktop discovery surfaces. На mobile оптимизируем вертикальный feed; на desktop — персональный порядок, секции и фильтруемую grid/list выдачу.

Первый релиз:

1. Статический fallback блок “Похожие события” строится при генерации HTML.
2. До consent — минимум функциональности без персонального tracking.
3. После consent:
   - localStorage хранит lightweight anonymous profile;
   - same-origin endpoint/Supabase пишет compact telemetry; server snapshots используются для analytics/post-MVP ranker, не как обязательный browser read в MVP;
   - client-side island может удалить явно неинтересные события и локально переупорядочить `event_detail_related`;
4. Если Supabase/API не отвечает быстро, страница остаётся в fallback режиме.

Performance rule: персонализация не должна блокировать first contentful paint, indexing или CTA.

Reference implementation для будущего Astro island:

- `static_site/personalization/personalization.js` — browser-only local-first rerank/telemetry controller;
- `static_site/personalization/demo.html` — static demo page;
- `tests/playwright/static_personalization_contract.spec.ts` — Playwright contract.

Текущий reference scope — только MVP-0 `event_detail_related`: static fallback,
consent/localStorage rerank, mobile `vertical_related`, desktop `grid_related`,
compact served-list telemetry и fallback при недоступном telemetry endpoint.

Подробности: `docs/features/unsigned-personalization/README.md`.

## Build/publish lifecycle

Нужны отдельные job types поверх существующего `telegraph_build`:

- `static_event_export` — собрать canonical event payload;
- `static_event_build` — сгенерировать HTML/assets;
- `static_site_publish` — залить в Yandex Object Storage/CDN target;
- `static_sitemap_build` — обновить sitemap/robots;
- `static_redirects_build` — обновить redirect/deleted URL policy.

Build должен быть ближе к kdg80 `site/package.json`:

```text
prepare static export/assets
→ astro build
→ verify static assets/routes/sitemap
→ deploy preview or production tree
```

Минимальные проверки сборки:

- все будущие active events имеют HTML page;
- все canonical event pages есть в sitemap;
- no preview canonical/OG URLs попали в production build;
- no production canonical URLs попали в preview build;
- все image URLs доступны или имеют fallback;
- JSON-LD валиден как JSON и согласован с видимым HTML;
- нет случайных тяжелых/неподготовленных PNG/JPG в public output, если для них нет явного исключения.

Rebuild triggers:

- create/update event;
- change date/time/location/title/ticket/status/photo/description;
- merge/split/linked dates;
- cancellation/postponement/sold out;
- manual force rebuild.

## Telegraph coexistence

Период dual-run: ориентир **1 месяц**.

На период coexistence:

- Telegraph остаётся fallback/compatibility для уже опубликованных постов и админских flows;
- новые публичные links должны постепенно переключаться на `kenigevents.ru` canonical URL;
- отчёты должны показывать оба URL, пока migration flag не выключит Telegraph;
- старые Telegram/VK посты не переписываются массово без отдельной задачи.

Exit criteria для отключения Telegraph как основного event page target:

- event pages успешно генерируются и публикуются для будущих событий;
- sitemap/robots работают;
- Rich Results/URL Inspection smoke на нескольких страницах пройден;
- Telegram/VK preview не хуже текущего;
- fallback/error handling проверен;
- 1 месяц dual-run не выявил критических регрессий.

## Yandex Cloud/Object Storage notes

Canonical publish target is now fixed:

- **production bucket:** `kenigevents.ru`;
- **public domain:** `https://kenigevents.ru/`;
- **Yandex website endpoint:** `http://kenigevents.ru.website.yandexcloud.net/`;
- **production deploy:** static-tree upload to the root of `s3://kenigevents.ru/`;
- **preview deploy:** static-tree upload to a unique prefix under `s3://kenigevents.ru/preview-<timestamp>-<random>/`;
- **Fly/site secret names:** `KENIGEVENTS_SITE_YC_ACCESS_KEY_ID`, `KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY`, `KENIGEVENTS_SITE_YC_BUCKET=kenigevents.ru`, `KENIGEVENTS_SITE_YC_ENDPOINT=https://storage.yandexcloud.net`, `KENIGEVENTS_SITE_YC_REGION=ru-central1`, `KENIGEVENTS_SITE_PUBLIC_BASE_URL=https://kenigevents.ru`;
- credentials stay in local `.env` / Fly secrets only and must not be committed.

Do not use the generic media poster bucket default (`kenigevents`) as the static-site target. The personalized static site publishes HTML, JS, CSS, manifests, sitemap and robots to `kenigevents.ru`; poster/media uploads may keep using their existing storage settings.

Из kdg80 нужно перенести два режима deploy:

1. **Production static-tree deploy** в корень домена/bucket.
2. **Secret preview deploy** в `preview-<timestamp>-<random>/`:
   - собирает Astro;
   - переписывает root-absolute URLs под prefix;
   - добавляет/заменяет `robots` на `noindex, nofollow, noarchive`;
   - canonical/OG указывает на preview URL или нейтрализуется;
   - same-origin динамические пути, которые должны жить в корне домена, явно не переписываются;
   - upload в новый prefix должен работать без bucket-wide list/delete permissions.

## Open questions

- Retention прошедших событий: сколько дней держим indexable archive?
- Нужен ли отдельный archive sitemap?
- Нужна ли отдельная англоязычная/латинская alias-модель, или достаточно `/sobytiya/<slug>/`?
- Как именно bucket/CDN будет отдавать 410 для удалённых URLs, если object storage не умеет это нативно без CDN/edge rules?
- Какой минимальный набор listing pages нужен в MVP: главная, город, дата, выходные, категория?
- Какие desktop modules нужны в MVP: персональный grid на главной, правый rail, отдельные блоки по датам/категориям или только сортировка в общем списке?
