# Static Site Event Pages

> **Status:** Astro SSG preview implemented; production rollout pending  
> **Scope for MVP:** только публичные страницы **событий** на `kenigevents.ru`  
> **Core fallback:** страницы событий работают без авторизации; optional Yandex/email identity, smart search and personalization are separate enhancements. Core event DB never moves to Supabase.

## Implementation status

В `events-bot-new` теперь есть первый **Astro SSG preview vertical slice** в `site/`: он строит статические страницы событий, `event.ics`, `sitemap.xml`, `robots.txt` и опубликован под noindex-prefix в bucket `kenigevents.ru`. Это ещё не production rollout: fixture пока компактный, canonical preview-safe, а корневые production URL `/sobytiya/<slug>/` не включены.

Текущий preview реализует production-oriented форму по паттерну соседнего `kgd80/site`: production SQLite export/static manifest → `getStaticPaths()` → `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/vystavki/`, `/populyarnoe/`, `/poisk/`, `/sobytiya/<stable-slug>/index.html` → `event.ics` → `data/discovery/<event_id>.json` → sitemap/robots/JSON-LD → preview `noindex` → publish to Yandex Object Storage bucket `kenigevents.ru`. Служебные QA/product страницы `/lab/medallions/` и `/partnerstvo/` живут в том же preview-префиксе. Следующий release step — включить и доказать автоматический Smart Update → Kaggle → checked artifact → atomic production promotion/rollback path.
`/populyarnoe/` сейчас является только source-engagement preview: exporter отдаёт TG/VK counters, `service_likes_count` остаётся нулём, а Astro ранжирует локальной формулой. Публичный релиз требует, чтобы страница без собственного скоринга потребляла SHA-bound ordered projection общего [source + site engagement contract](../post-metrics/consolidated-event-engagement.md#shared-popular-event-projection); наличие общего `EventListItem` решает только отображение, но не консолидацию популярности.
Для медиа export обязан передавать `image_text_mode` из существующего OCR/media-контура. В hero `ocr_text`/`unknown` идут в `poster-stage` (full poster on solid slab), `visual_only` — в `photo-cover`; в discovery cards `ocr_text`/`unknown` рендерятся в натуральном соотношении без crop/backdrop, `visual_only` допускает cover-кроп в вертикальной 4:5 карточке; date-listing thumbnails (`/segodnya/`, `/zavtra/`, `/vyhodnye/`) являются компактным навигационным preview и используют crop в левой колонке, при этом detail page остаётся источником полного постера. Astro build сам OCR не запускает.



### Brand mark / root announcement landing

The live static announcement site root (`https://static.kenigevents.ru/`, bucket root `/`) uses the warm two-color brand mark from `site/public/favicon.svg`: graphite `#2d3035`, terracotta `#af481f`, cream backing `#fff6ea`. The drawing follows the T-000053 transparent SVG redraw shape (left monogram, `К`, heart tile, right tile and signal bars) rather than the earlier rejected calendar/church silhouette. The compact transparent variant lives at `site/public/brand-mark.svg`; both assets are hand-authored SVG paths with no embedded raster images, and `site/scripts/check-preview.mjs` guards the expected ids/colors. On 2026-06-29 the root `index.html`, `favicon.svg`, and `brand-mark.svg` were manually published to the static-site bucket root as a product-visible logo correction, then the root landing was refined to remove decorative shadows/borders, use the graphite mark, align the hero/actions consistently, and replace implementation-heavy copy with a short public-facing value statement. The preview/event-page prefixes were not modified by that root-only upload.

### Информационные партнёры

Сервисная institutional-страница `/partners/` добавлена как статическая preview-страница для статуса «информационный партнёр». Она не меняет event-detail модель: стартовый список партнёров хранится отдельно в `site/src/data/info-partners.ts`, рендерится в `site/src/pages/partners/index.astro`, попадает в `sitemap.xml` и доступен из footer-навигации как `Инфопартнёры`. Страница должна оставаться компактной плоской logo-first bento-доской с заголовком `Полюбить Калининград Анонсы выступает информационным партнёром организаций`: на mobile — 4 плотные колонки с full-occupancy bento placement, где `80 историй` занимает 2×2 hero-ячейку, на tablet/desktop — 8-колоночные aspect-aware spans для широких и вертикальных логотипов; высота grid-рядов и logo/caption frames фиксированы, а подпись не управляет размером ячейки; подпись рендерится только когда нужна для контекста, поэтому `Знание` и `Акт Опус` идут без подписи. Текущий contract запрещает публичные категории вроде `Пригородные маршруты`/`Лекции и музыка`, неверный label `КППК / РЖД`, тяжёлые card borders/backgrounds/shadows и декоративные highlight-плашки. Внешний переход — вся плитка партнёра с `rel="nofollow noopener noreferrer"`; отдельный CTA `Сайт партнёра` не рендерится. Каноника фичи: `docs/features/info-partners/README.md`.


### Kaggle CPU build handoff

Static-site generation is now prepared as a Kaggle CPU job reusing the existing CherryFlash/TelegramMonitor infrastructure: per-run private input dataset, `KaggleClient.push_kernel(...)`, optional `kaggle_run.json` status dataset, `kaggle_status_client` heartbeat/progress from inside the kernel, and a `static_site:builder` resource lease. The first verified 50-event production-snapshot Kaggle build is `preview-20260628-event-pages-prod50-kaggle-v44` (artifact only, not bucket-published): it produced a tar.gz static site archive, `static_site_build_result.json`, and passed `check:preview`. Production Smart Update handoff is a coalesced `static_site_build` outbox job 15 minutes after the last event update, gated by `ENABLE_STATIC_SITE_KAGGLE_BUILDER=1`; object-storage/CDN publication is now available for preview/focus-group builds after the media mirror gate; production promotion still requires a separate release gate. Operational publisher protocol: `docs/operations/kaggle-static-site-builder.md`; build notes: `docs/features/static-site-pages/astro-preview.md#kaggle-cpu-builder--smart-update-handoff`.

## Текущий публичный preview

Latest v44 CDN/Kaggle fixes: public preview `preview-20260628-event-pages-v44-cdn-kaggle` was built by Kaggle CPU from the 2026-06-28 production snapshot (80 real events), event images now render through `https://static.kenigevents.ru/p/...`, stable calendar CTAs use `https://static.kenigevents.ru/ics/<event_id>.ics`, and deploy copied 80 `.ics` files to the CDN bucket. The v43 UI/gallery fixes remain: wrapped mobile tag geometry, adjacent gallery preload/decode, paid real price links with `rel="nofollow"`, and a diverse same-day `/segodnya/` slice.


- Preview index: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/__preview/>
- Today listing: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/segodnya/>
- Tomorrow listing: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/zavtra/>
- Weekend listing: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/vyhodnye/>
- Control event `5878`: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/sobytiya/pesni-sssr-svetlogorsk-5878/>
- Gallery regression event `698`: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/sobytiya/drevnie-voiny-yantarnogo-kraya-kaliningrad-698/>
- Same-day listing-card/control event `6438`: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/sobytiya/vodnye-bitvy-s-animatorami-kaliningrad-6438/>
- Stable control ICS: <https://static.kenigevents.ru/ics/5878.ics>
- Control discovery JSON: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/data/discovery/5878.json>
- Hero composition lab: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/lab/hero/>
- Hero viewport review: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/lab/hero/review/>
- Sitemap: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/sitemap.xml>
- Robots: <https://kenigevents.ru/preview-20260628-event-pages-v44-cdn-kaggle/robots.txt>
- Website endpoint fallback: <http://kenigevents.ru.website.yandexcloud.net/preview-20260628-event-pages-v44-cdn-kaggle/__preview/>

Current v44 preview scale/evidence (Kaggle CPU build from the 2026-06-28 production snapshot):

- preview fixture: `80` real active events, including `49` events starting on 2026-06-28 across `14` event types;
- generated output: `95` static pages, `261` files, `28 MiB`;
- Kaggle build result: `event_count=80`, `started_at=2026-06-28T17:38:08Z`, `finished_at=2026-06-28T17:38:31Z`; public bucket upload from this workstation plus stable ICS copy completed after artifact download;
- full active-snapshot estimate measured by a local full export/build: `386` active future/intersecting events → `403` pages, `1185` files, `128 MiB`, `0:20.16` wall time (`maxrss≈522 MiB`). Upload/publish time is expected to scale mostly with file count/bytes rather than Astro render time.
- media CDN verification: `957` active legacy `/p/...` keys from `kenigevents` are present in `kenigevents.ru`; sample CDN image and `https://static.kenigevents.ru/ics/5878.ics` return `200`; rendered v44 HTML contains CDN `/p/...` and stable `/ics/...` links, not raw legacy image URLs.
- media CDN verification: `957` active legacy `/p/...` keys from `kenigevents` are present in `kenigevents.ru`, sample CDN image and `https://static.kenigevents.ru/ics/5878.ics` return `200`; rendered v44 HTML contains CDN `/p/...` and stable `/ics/...` links, not raw legacy image URLs.

Preview `v39` keeps the consultant P0 hardening, explicit discovery feedback and feed-card comparison, and adds the current UI/data refinements: the mobile brand tag removes the rejected icon and keeps only a subtle periodic title sway; the fullscreen gallery uses a slower/farther `38%→64%` pan and advances before the pan fully stops; `Фото события` and the event title are forced onto separate caption lines; `Пушкинская карта` is rendered as a green check property, not a text value; admission/free states use compact property labels instead of bare `Бесплатно`; and the desktop event nav no longer exposes `Sitemap`. It also keeps date-listing/navigation refinements: `/segodnya/` is grouped into `Утро / День / Вечер / Ночь`, `/zavtra/` exists as a separate tomorrow page with the same sections, date-listing cards use a compact mobile plaque with a cropped left photo column and a straight separator to the text column, and list cards no longer expose mass external ticket/source links; users go through the internal event page first. The mobile discovery drawer handle no longer shows the icon; it uses only a subtle title sway in the site palette. Preview `v39` also hardens the event hero into a stronger mobile-first surface: the hero **image itself** is guarded by preview checks at bbox-style full-width contract (`x=0,width=viewport`) with no layout side gutters; the normal mobile header is replaced over the hero by a TASS-like terracotta drawer handle (`Полюбить Калининград / Анонсы`). Tapping it opens a no-JS `<details>` discovery drawer implemented as one monolithic sliding object: the full-width navigation rail and handle move together, overlap by a few pixels, have no transitional gap, no chevron/up-down icon, no rounded dropdown panel and no pill-buttons inside the rail. Visual-only hero variants use stable `svh` sizing plus constant-scale vertical parallax so mobile browser chrome changes should not cause a post-scroll scale jump. The current event page itself exposes first-party like/unlike with the honest aggregate like count, and share counters are present but stay empty when the total is zero. Feed cards use `split-actions` as the baseline, not an A/B on normal event pages: `Поделиться` is clustered near the right-thumb like action below the card, while `Не интересно` is demoted to a quieter utility action. The fullscreen hero viewer now uses a visible on-image photo CTA (`Фото N` when multiple images exist), lazy-hydrates gallery images from `data-gallery-src` only after opening/navigating, uses full-viewport-height `cover` + one-way right-to-left auto-pan for `visual_only` photos to avoid black side fields, auto-advances to the next photo after the pan, pauses forward auto-advance after a manual backward swipe, keeps OCR/text images in the base `contain` mode, keeps the service tag visible in the gallery, and places the event title in a readable bottom stripe. JSON-LD offer `validFrom` is emitted as ISO 8601 with timezone and JSON-LD `image[]` includes the event gallery assets so lazy images remain connected to the event for SEO/GEO. The historical comparison is in `docs/features/static-site-pages/event-card-ui-ab-2026-06-27.md`; hero decisions are in `docs/features/static-site-pages/event-hero-lab-2026-06-27.md`. The inherited v32 interaction/SEO hardening remains: in fullscreen gallery the service tag is a real top-flush navigation link, not a floating label; visual-photo pan starts at `38%` and moves to `64%` over `17.9s`, which gives a slower/farther right-to-left image motion; manual backward motion uses `64% → 38%`; the event title in the fullscreen viewer uses inline/subline stripes via `box-decoration-break`, not a full-width bottom slab. Event cards now place the title before time/status meta because the feed scan task starts from “what is this?”, then date/conditions; service controls (`Не интересно`, share, like/undo plate) are marked `data-nosnippet` and remain buttons, not crawlable links. Phone-only CTAs use a selected SVG Repo handset-with-ringing-arcs phone icon (`533283/phone-call`) adapted into the shared inline icon system.

Other v39 contracts remain: visible description, one vertical neutral `Смотрите дальше` feed, no user-facing “try another genre” block, large right-thumb like buttons with counts and unlike, “Не интересно” negative feedback, native-share-first button, graphite/terracotta monogram SVG favicon, prefetch for static links, and sticky CTA hiding while the hero is visible and again when the user reaches the feed. The after-hero drawer handle remains visible when closed so navigation is never lost while scrolling; the monolithic root transform keeps the panel off-screen and only the handle protrudes. Media rule after the v15 fix: selected preview images with **no meaningful OCR** (`visual_only`) use a strict vertical `4:5` cover frame in discovery cards; `ocr_text` and `unknown` images in discovery cards render as the actual image in natural aspect ratio; compact date-listing thumbnails may crop in the left column as navigation previews; event-page hero uses deterministic media policy (`poster-stage` solid slab with full-poster `contain` for OCR/unknown, `photo-cover` for `visual_only`) plus a separate composition layer (`poster-billboard`, `poster-attached-card`, `photo-cinematic-sheet`, `photo-parallax-sheet`, `compact-ticketing`). There is no crop for OCR, no duplicate underlay, no blur/backdrop fill and no repeated image edges. Each event page statically preloads up to 10 continuation candidates in HTML; after JS starts, the page uses only a consented compatible local profile (`ke_personalization_profile`, UUID `anon_id/session_id`, `event-detail-related-v1` + `event-taxonomy-v1`) to filter/rerank. The client removes already hidden / `not_interested` / strong negative-interest matches from the preloaded cards, performs one same-origin JSON hydration from `/data/discovery/<event_id>.json`, where the payload is an `event_detail_related` manifest with `related_static[]`, and top-ups relevant candidates; subsequent expansion is only by `Показать ещё`. Local strong actions currently write a compact browser log with `served_list_id` / `served_list_hash` context for future Supabase telemetry mapping. Important status: the v39 static preview does **not** persist first-party likes/profile snapshots to Supabase yet; the preview contract keeps like/profile writes local-only and forbids treating this as Supabase persistence. Source counters are already synced to Supabase by the production metric pipeline, but browser feedback remains same-browser/local until the dedicated gated write path (same-origin endpoint or append-only Supabase RPC with RLS/grants) is implemented. Cards are full-clickable for users while keeping real HTML links on media/title for SEO/GEO; double-tap like is disabled because it conflicted with navigation. `Не интересно` turns the acted-on card into a grey explanatory plate with an explicit `Отменить` action; tapping the plate itself must not navigate to detail. Visible like/share counters are hidden when the total is zero. Visible like counts are honest totals: `likes_count = source_likes_count + service_likes_count`, where source likes come from production TG/VK post metrics and service likes are first-party KenigEvents likes; public HTML/UI shows only this total, not the technical source/service split. The hero no longer duplicates facts as a second info block; it keeps only a compact meta line, while the single `Коротко` block owns icon facts (`Где` combines venue + address, `Вход`, optional `Пушкинская карта`/festival), no longer links to Telegraph, and no longer exposes source count/views in public HTML. The registered-user sources/mentions notice belongs to the parent details section as a bottom strip, not to the `Коротко` fact block. Footer is now a compact navigation block: top links (`Сегодня`, `Завтра`, `Выходные`, `Все анонсы`), crawlable editorial social links with icons + short labels, and contact email `info@kenigevents.ru`.

Preview `v40` closes the share experiment: temporary `Поделиться эксперимент`, `Поделиться эксперимент 2` and rich-clipboard controls are not production UI. The single visible `Поделиться` button is now the production baseline: it attempts Web Share with image file + plain text + URL, then falls back to generated 1080×1350 image + text + URL, and finally to text/URL copy when the browser or target app cannot accept files. Rich hidden hyperlinks inside share text are documented as impossible to guarantee from a mobile browser.

Preview fixture note: production row `5370` («Точка и линия») currently has a false-free state (`is_free=1`, `ticket_status=бесплатно по регистрации`) because a free curator round-table source was merged into the long-running paid exhibition. The v40 fixture intentionally overrides the preview event to `ticket / paid / is_free=false` and attaches five real exhibition photos for gallery testing; production source-of-truth repair remains a separate event-quality task under the existing false-free incident family.

Build/runbook: `docs/features/static-site-pages/astro-preview.md`. Reaction counter architecture: `docs/features/static-site-pages/reaction-counters.md`. Event-page medallions / quick-read tokens: `docs/features/static-site-pages/event-token-medallions.md`.

## Share / Open Graph

The share experiment is closed. Production-like UI exposes only one `Поделиться` action. It attempts, in order:

1. `navigator.share({ files, text, url })` with an actual image file, short caption text and the event URL;
2. the same file/text/url flow with a generated 1080×1350 PNG fallback if the source image cannot be shared as a file;
3. native text+URL share or clipboard copy when file share is unavailable.

Important limit: Web Share API does not expose a rich-text/HTML payload field and cannot force Telegram/VK/Max to render a hidden hyperlink under arbitrary caption text. The reliable production mechanism remains the event URL with strong Open Graph metadata (`og:title`, `og:description`, `og:url`, `og:image`, `og:image:secure_url`, `og:image:type`, optional width/height). Telegram/VK/Max can render a rich preview from the URL, but browser share can only offer file/text/url and the target app decides the final message format.

### Share-image generator technical debt

Production must replace the same-page canvas fallback with a stable server/offline share-image generator:

- generate same-origin or CORS-enabled images, preferably CDN-served after CDN enablement;
- keep at least three formats: `1200×630` for Open Graph, `1080×1350` vertical `4:5` and `1080×1080` square `1:1` for Web Share/media-post;
- reserve a visible bottom stripe in `4:5` and `1:1` variants for brand, event date/place and CTA, matching the successful transferred-post visual pattern;
- include brand, event title, date/time, venue/city, admission/CTA and canonical URL;
- obey the same OCR/media rules as event pages: do not crop OCR text, avoid face cuts once Smart Update returns face/focal metadata;
- emit deterministic filenames/cache keys tied to event id + updated timestamp, and QA Telegram/VK/Max behavior on Android/iOS.

Admin backlog: add a one-click “prepare detailed event post” function that creates a richer ready-to-paste post package for Max/VK/Telegram (share image + caption + URL) for an operator, without changing public page SEO.

## Event-page medallions

Event detail pages render large quick-read **medallions** after the hero/title area on `/sobytiya/<slug>/`. These are medallions of the concrete event, not a card-list badge row. The current slice renders curated organizer avatars plus safe facts such as `Пушкинская карта`, free/price, family/charity/festival hints; speaker/celebrity avatar medallions are a P1 extension gated by source-grounded identity and cached avatars. Listing/search cards keep only the metadata formatting change: show a short weekday and render event type as plain text without `#`. Canonical contract: `docs/features/static-site-pages/event-token-medallions.md`.

## Listing personalization on static lists

P0 list personalization is a local **filter**, not a second SEO page and not a backend dependency. `/segodnya/`, `/zavtra/` and `/vyhodnye/` keep the full static list in HTML for users without JS and for crawlers. After JS starts, a `Все / Для меня` segmented switch appears only if the compatible local profile can actually hide at least one event (`Скрыто N > 0`). On mobile it is a fixed bottom switch with footer-overlap guard; when nothing is hidden it is not shown. If the user has not explicitly selected a mode and there are local `Не интересно` marks, the list defaults to `Для меня`; choosing `Все` is an explicit override. `Для меня` reads only the consented local profile (`ke_personalization_profile`) and hides exact events / linked date variants that the user already marked as `Не интересно`; it does not create `?personal=1`, does not affect canonical/sitemap/JSON-LD, and all controls are `data-nosnippet` buttons, not crawlable links.

## Browser notifications / Web Push planning

Browser notification permission must not be requested on first page load. It is a P1/P2 opt-in CTA after the user has demonstrated interest (for example liked/saved events, opened tomorrow/weekend pages, or explicitly tapped a “Получать подборки” prompt). Required mechanics: Service Worker + Push API subscription, explicit unsubscribe, quiet hours, frequency caps, and an analytics event for permission shown/granted/denied/clicked.

Product triggers for the promo/personalization roadmap:

- “Подборка событий на завтра готова” after the tomorrow list is rebuilt;
- Friday lunchtime “Посмотрите события на выходных”;
- optional event reminders only for events the user liked/saved/added to calendar;
- campaign-driven Web Push activities must respect the same disclosure/cap rules as other promo surfaces.

## Listing personal feed / backend preparation

Authenticated smart search is now designed/implemented as an optional static-site enhancement: when `PUBLIC_PERSONALIZATION_SUPABASE_URL` and `PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY` are present, listing/index pages render `AuthorizedEventSearch.astro`. It uses Supabase Auth with custom Yandex provider `custom:yandex`, calls Edge Function `event-search`, and renders results as the same split-action event cards. Canonical design: `docs/features/unsigned-personalization/authorized-event-search.md`.

Listing pages now include a hidden dynamic `Личная лента` slot at the end. Static HTML does not pre-render personal cards. If a backend/RPC feed is configured, the browser fetches up to 30 compact card projections once, caches the starter list in `localStorage` as `ke_listing_personal_feed_cache_v1` for 30 minutes, and reuses it across `/segodnya/`, `/zavtra/` and `/vyhodnye/`. If backend/RPC is absent, the section stays hidden and the static listing remains unchanged. Canonical design: `docs/features/static-site-pages/listing-personal-feed.md`.

## CDN asset delivery preparation

CDN `https://static.kenigevents.ru` is live for the static-site bucket. Focus-group previews now use it for both Astro code assets and event media: `PUBLIC_ASTRO_ASSET_BASE_URL=https://static.kenigevents.ru/{buildId}`, `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru`, `PUBLIC_ICS_BASE_URL=https://static.kenigevents.ru/ics`. Canonical pages stay on `kenigevents.ru`; images (`/p/...`) and stable calendar files (`/ics/<event_id>.ics`) are served from the CDN bucket `kenigevents.ru`. Legacy media from bucket `kenigevents` is copied idempotently by `scripts/migrate_static_media_to_cdn_bucket.py`; new server/Kaggle media writes target `YC_STORAGE_BUCKET=kenigevents.ru`. Canonical design/testing policy: `docs/features/static-site-pages/cdn-asset-delivery.md`.

## Admission/property labels

Event facts should read as properties, not prose duplicates. Current UI policy:

- `Пушкинская карта` is an admission/payment property, so it is rendered as a compact green `✓ Пушкинская карта` chip inside the `Вход` fact, not as a separate prose row and not as `возможна`/`доступна` value copy.
- `Вход` uses a compact primary chip plus subtype text. Free events must keep the word `Бесплатно`: `Бесплатно · вход свободный`, `Бесплатно · регистрация`, `Бесплатно · по записи`; then `За донат`, `Запись по телефону`, `Цена уточняется`/`По билетам` when price is missing, or the real price/range when present. If a paid event has both `price_label` and a ticket URL, the price chip itself may be the purchase link; because it is transactional/external, it uses `rel="noopener noreferrer nofollow"` and `data-nosnippet`, while the internal event page remains the SEO/GEO target.
- Real production scan on 2026-06-28 found 385 future active rows with free/admission clues: `free_word=59`, `registration=34`, `booking/запись=25`, `comments=4`, `limited=18`, `donation=2`. This confirms that “free” is not a single state: it can mean open entry, registration, booking, comment signup, limited seats, donation, or mixed paid/free ranges.

## Media/gallery and Smart Update image metadata requirements

For events with multiple images, MVP must **not** auto-rotate the first-paint hero by default. The event page chooses one deterministic best hero image for first paint. A tap/click on the hero opens an explicit immersive fullscreen gallery; only inside this opened viewer `visual_only` photos may run a controlled auto-forward pan/advance sequence. Current v43 state machine: forward pan is `38% → 64%` object-position over `17.9s` (right-to-left visual motion), auto-advances by an `8.88s` timer so there is no long dead wait with `gallery-pan-forward` as a fallback, preloads and `decode()`s the next adjacent image slides before the timer fires so the transition does not start over a black empty slide, manual backward swipe/click sets `autoAdvance=false` and runs only the local reverse pan, and `prefers-reduced-motion` remains a hard guard. The final gallery slide can be an explicit CTA card (`Купить билет`, `В календарь`, `Смотреть похожее`), visibly labeled as a CTA, not disguised as another image. Pull-to-expand/top overscroll hero behavior is P1/P2 lab-only because it can conflict with browser pull-to-refresh and mobile address-bar gestures.

Smart Update / image preparation must enrich image assets in batch/offline, not in the static page runtime. When OCR/media analysis runs, it must return enough metadata for safe crop decisions:

- `image_assets[]` with `src`, dimensions, `alt`, `image_text_mode`, `image_kind`;
- `ocr_boxes[]`, `image_text_density`, dominant text regions and `recommended_hero_fit`;
- `face_boxes[]`, `face_count`, `face_confidence`, `saliency_boxes[]`;
- `focal_point` / `recommended_object_position` in normalized 0..1 coordinates or CSS object-position form;
- `safe_crop` / crop guards: do not crop OCR text, and do not cut detected faces, especially top face bounds.

Runtime policy: `ocr_text`/`unknown` stay `contain`/natural-ratio no-crop; `visual_only` may use cover only with face/focal metadata when available. If safe face-aware cover is impossible, renderer should fall back to contain/natural presentation rather than cutting heads or important text.

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
- Публичный static fallback не требует авторизации; optional authorized search/identity surfaces do not change crawlable HTML availability.
- Anonymous local personalization допустима только после consent; remote telemetry/profile materialization remains a separate gated capability.
- Telegraph остаётся временным compatibility/fallback layer примерно на **1 месяц** после включения статических страниц.
- Доступы к Yandex Cloud/Object Storage будут выданы отдельным шагом; до этого проектируем контракт и пайплайн без привязки к конкретным credentials.

## Связанные документы

- Release umbrella and F1–F17 map: `docs/features/static-personal-announcements/README.md`.
- Release UI contract: `docs/features/static-site-pages/release-ui-contract.md`.
- Responsive navigation decision/research: `docs/features/static-site-pages/responsive-navigation.md`.
- Event sharing/generated images: `docs/features/static-site-pages/event-sharing.md`.
- Image framing/focal metadata: `docs/features/static-site-pages/image-framing.md`.
- Event gallery duplicate release audit: `docs/operations/event-image-duplicate-audit.md`.
- Event transport: `docs/features/event-transport/README.md`; optional gallery card: `docs/features/event-transport/gallery-how-to-get-there-card.md`.
- Event comment feedback: `docs/features/event-comment-feedback/README.md`.
- Admin issue reporting: `docs/features/event-issue-reporting/README.md`.
- Astro SSG preview runbook and public URLs: `docs/features/static-site-pages/astro-preview.md`.
- CDN asset delivery: `docs/features/static-site-pages/cdn-asset-delivery.md`.
- Final post-UI-freeze SEO/GEO optimization and three-agent audit: `docs/features/static-site-pages/seo-geo-release-optimization.md`.
- Listing personal feed: `docs/features/static-site-pages/listing-personal-feed.md`.
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
- Event-card UI A/B comparison and product hypothesis: `docs/features/static-site-pages/event-card-ui-ab-2026-06-27.md`.
- Event hero composition lab / mobile-first decision: `docs/features/static-site-pages/event-hero-lab-2026-06-27.md`.
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

## Related-block rebuild freshness

`static_related_v1` was the first build artifact over the active event catalogue. The release implementation is the strict `related_v1` pgvector + LLM-verified graph described in [Semantic vector retrieval](../unsigned-personalization/semantic-vector-retrieval.md#release-automation-contract). If a new event appears, it can become a good recommendation for already published event pages, so rebuilding only the new page is not sufficient.

Release operational rule:

- every effectful Smart Update create/update schedules one coalesced full-catalog static build for 15 minutes after the last effect;
- the build publishes the new/changed event page and recomputes the active/future related graph for older anchors before atomic promotion;
- an update during a running build guarantees exactly one follow-up build from a newer snapshot;
- the independent vector lane runs after Smart Update and on periodic full-catalog reconciliation; the static build verifies matching `related_v1` hashes before retrieval;
- a periodic drift reconciler and scheduled lifecycle rebuild recover lost triggers and expire started/ended candidates even when no Smart Update occurs;
- changed candidate windows require current LLM-verifier evidence; unchanged fingerprint+policy cache hits are valid, while raw vectors are never silently labelled `Похожие`;
- emergency/manual rebuild is allowed after major imports, large festivals, or quality fixes;
- future optimization may add reverse-impact rebuilds by same city/category/date bucket, but only after manifest-hash evidence shows full-catalog rebuild/reconciliation is too slow.

This automatic publication loop is **not production-complete yet**: the vector projection lane is configured, but the static Kaggle builder/strict related flags and atomic CDN promotion remain release gates.

## Date listing SEO/product contract

Date listing pages (`/segodnya/`, `/zavtra/`, `/vyhodnye/`) should be indexable in production: they answer real date-intent queries and provide internal links to canonical event pages. The SEO risk is not the listings themselves, but mass outbound links from every card to ticket/social/source services. Therefore listing cards should not show direct external ticket/source CTAs. They keep internal event links and optional same-origin `.ics`; the external ticket/registration/source action lives on the event detail page, where context, JSON-LD and `rel="noopener noreferrer nofollow"` can be controlled. Do **not** solve outbound-link concerns by `noindex` for production listing pages. Preview builds remain `noindex,nofollow` only because they are preview prefixes.

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
- mobile рендерит related как вертикальную continuation/feed-секцию `Смотрите дальше`, не горизонтальный rail; desktop остаётся desktop-native grid/module, а не растянутой мобильной лентой;
- cards внутри static fallback сохраняют crawlable media/title links, но служебные feedback controls остаются button-only и `data-nosnippet`;
- `static_related_v1` уже реализован в `site/src/lib/events.ts`: seed `preview-related.json` + deterministic scoring by category/tag overlap/city/date/venue/price/status + hard exclusions for current/other-date/past/inactive. Это достаточный MVP baseline for preview, но не финальная product-quality рекомендация без expanded catalog/golden top-10 review.

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
- **Fly/site secret names:** `KENIGEVENTS_SITE_YC_ACCESS_KEY_ID`, `KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY`, `KENIGEVENTS_SITE_YC_BUCKET=kenigevents.ru`, `KENIGEVENTS_SITE_YC_ENDPOINT=https://storage.yandexcloud.net`, `KENIGEVENTS_SITE_YC_REGION=ru-central1`, `KENIGEVENTS_SITE_PUBLIC_BASE_URL=https://kenigevents.ru`, `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru`, `PUBLIC_ICS_BASE_URL=https://static.kenigevents.ru/ics`, `YC_STORAGE_BUCKET=kenigevents.ru`, `YC_STORAGE_PUBLIC_BASE_URL=https://static.kenigevents.ru`;
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
