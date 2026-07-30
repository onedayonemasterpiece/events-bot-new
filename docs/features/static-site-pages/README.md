# Static Site Event Pages

> **Status:** accepted v11 baseline plus v12 fidelity corrections are the primary preproduction event-page family, integrated with the checked Smart Update/Kaggle secret-candidate pipeline; root promotion remains blocked/disabled
> **Scope for MVP:** только публичные страницы **событий** на `kenigevents.ru`  
> **Core fallback:** страницы событий работают без авторизации; optional Yandex/email identity, smart search and personalization are separate enhancements. Core event DB never moves to Supabase.
> **Current release plan:** [production profile, atomic promotion and 10-day Telegraph cutover](release-plan.md).

## Focus-group product prototype (2026-07-27)

The isolated focus-group branch adds page/product mechanics for a later UI
integration: a root testing stub, invite/auth-choice flow, bounded local preview
marker, secret hub, participant feedback specimens, lifecycle end states and an
explainable local `/dlya-menya/` editor. Canonical product decisions and the
strict prototype/production boundary live in
[`docs/backlog/features/static-site-focus-group/README.md`](../../backlog/features/static-site-focus-group/README.md).

This does not promote a production root, provision a cohort, implement Auth or
Supabase membership, send email, persist feedback, run a prize programme or
deploy a focus candidate. Production and secret-candidate builders still own
their root transformations; localStorage remains an explicit UX hint rather
than authorization.

Cross-surface naming, identity, lifecycle and acceptance for `Другие даты`,
strict related, broad continuation and personal feed are canonical in
[`docs/features/linked-events/README.md`](../linked-events/README.md). This
document owns page implementation details, not a parallel relation taxonomy.

Mobile Search, query collections and other Astro discovery routes use the
shared [`mobile shell`](mobile-shell.md): one header/drawer contract, one
route-owned bottom-nav selection and one header-attached toast region. Runtime
Search and materialized-query cards consume the same mobile large-card media
resolver: visual-only images fill a horizontal `5:4` frame; OCR/documents keep
their intrinsic ratio, including browser decode reconciliation when the Search
snapshot lacks dimensions. The desktop compact related-row optimizer remains a
separate contract and never supplies Search placement geometry. Search progress is backend-streamed and monotonic rather
than simulated by client timers. The exact runtime/search contract lives in
[`authorized-event-search.md`](../unsigned-personalization/authorized-event-search.md).
The isolated [amber-artifact placement research](amber-artifact-easter-egg.md)
compares a true rail-tail easter egg with an after-medallion challenger without
changing the accepted calendar renderer.

The [`festival calendar timeline`](festival-timeline.md) is now part of the
checked production generator and extends the same shell with a Fly
SQLite-backed July–December 2026 festival directory. It owns the
festival-specific source/status and flexible row-packing contract; the category
view and production-root publication remain out of scope.

## R15 home composition

The R15 home candidate is one ordered composition, not a second personalization
product:

1. a concise hero that explains the service in human language;
2. quick navigation to materialized discovery routes;
3. a cold-start event feed capped at 30 cards;
4. progressive client reranking only when the versioned same-origin local
   profile contains compatible preference signals.

Static HTML always contains the useful cold-start feed. Hydration may filter or
rerank within the bounded contract but cannot blank first paint, duplicate
events, introduce a page-view embedding/LLM call, or create a different
indexable home. Backend/auth/storage failure keeps the static ordering. The
production and secret-candidate packagers preserve this dedicated `/` output;
they must never overwrite it with the `/segodnya/` artifact. The home does not
authorize production-root cutover; it follows the same immutable
candidate and owner-acceptance gate as the
[`unusual-events` R15 release](../unusual-events/README.md).

### Unified occurrence projection

Интеграция из contract/donor `feature/related-events-compact-unified-20260721`
перенесена выборочно: event detail, большие/list
cards и hydrated discovery используют один pure
`site/src/lib/eventOccurrences.ts` и dumb components
`EventOccurrenceLabel.astro` / `EventOccurrenceNav.astro`. Identity берётся
только из взаимных explicit `other_date_ids`; совпадение title/type/venue не
создаёт family. Exact compact labels: `2, 9 ноября 19:00` и
`4 ноября 17:00, 19:00`; rail переносит тот же DTO в две строки с полным
`aria-label`. Date lists применяют `per-date`, ranked/entity lists —
`per-family`, event detail — always-visible selector. Search snapshot хранит
member ids, compact label и полный accessibility label; сервер сворачивает family
до logical pagination, после LLM rerank и в fallback, поэтому sibling не
всплывает на следующей странице. Полный contract и handoff:
[`REL-045`–`REL-050`](../linked-events/requirements.md) и
[`handoff.md`](../linked-events/handoff.md). Это не wholesale merge старых labs
и не меняет production-root status страницы.

## Implementation status

The accepted event-detail templates are the primary preproduction surface. A
coalesced Smart Update rebuild publishes an immutable noindex
`/_review/<token>/` candidate and advances a durable internal pointer only after
the full candidate gate succeeds. Historical dated previews are evidence, not
current review links; production root stays stable until atomic promotion, URL
lifecycle and rollback gates pass.

The durable single-flight lock follows the remote Kaggle ledger rather than
the age of the local claim alone. A terminal remote run remains available to
the exact same job's recovery pass, but a later coalesced Smart Update may
supersede that orphaned claim immediately and records the supersession in build
history. A live or non-terminal fresh run still blocks duplicate generation.

`static-event-detail-v11` is the mandatory baseline contract for automatic and
on-demand builds; v12/v13 are fidelity, idempotency and interaction corrections
on that same accepted family, not competing lab templates. Its accepted baseline is source SHA
`3b17e536e4dffa9c9fcebab6e641a7cd4ba99b6a`; the current build SHA may advance,
but every event HTML and release manifest must retain that contract identity.
The gate covers the complete accepted desktop scenario matrix, not three hand-picked pages:
horizontal photo, horizontal photo plus OCR companion, bounded OCR/document card cover,
portrait series, low-resolution and quality-fallback combinations. The same
`buildDesktopEventPresentation()` router is used for preview, checked
production artifacts and secret candidates. A build that falls back to the
legacy `EventHero + event-grid` desktop DOM fails closed.

The blocking Chromium release gate first derives a bounded specimen set from
the generated immutable HTML (preferring the reported `6408` gallery journey
while it remains in the catalog), then performs the same real-browser document
navigation and geometry assertions. It never scans dozens of live pages merely
to discover a specimen, so a large catalog cannot turn the mandatory gate into
an unbounded Kaggle run. Every Playwright action/navigation has an explicit
deadline (`8s`/`12s`), the gate waits for the concrete continuation/card
readiness contract rather than Playwright's discouraged `networkidle` state,
and each root/candidate subprocess is killed after five minutes. The local
HTTP fixture also force-closes residual image sockets after Chromium exits, so
the required visual gate fails closed instead of occupying the Kaggle
single-flight lease indefinitely. Continuation readiness is exercised by
crossing the actual end-of-related `IntersectionObserver` boundary with a
wheel-like scroll; the gate never jumps directly to `scrollHeight`, which can
skip the trigger in one frame and produce a false timeout.
Because immutable CDN assets are create-only and do not exist until after this
gate passes, Playwright maps only the candidate's `/_astro/**` requests back to
the checked generated tree. Event media remains on the real CDN. This ordering
ensures the prepublication gate executes the exact generated JS/CSS without
requiring or mutating a public build prefix first.
Document crop acceptance is card-local: `coverCrop` and computed `object-fit`
must prove the concrete card safe. Compact rows have no image bands: all cards
use `cover`, ordinary documents keep their natural ratio without crop, and
only very tall documents may crop at most `20%`. The shared global optimizer
enumerates feasible groupings, may reorder cards and minimizes normalized total
page height while keeping media and total card heights equal within each row.
Static
`Смотрите дальше` and hydrated `Ещё события` consume this one decision through
`relatedCardLayout` and the canonical `EventCard`; surface CSS may not
reinterpret it. The gate uses captured `6408` production canaries, computed
style, independent unused-frame/crop ratios, equal-height assertions and decoded
row pixels so a declared treatment cannot certify itself. Non-OCR hero/gallery
media also uses `cover`; OCR/unknown hero media remains full-poster `contain`.
The keyboard compatibility migration rewrites continuation links only for
legacy prefixed candidates. Root-form cards retain the canonical component's
relative `href` in both `data-card-href` and media/title anchors, so keyboard
enhancement cannot create a second URL representation or behaviour.
The Kaggle production-candidate install uses Playwright's CI form
`install --with-deps --only-shell chromium`: downloading Chromium alone is not
enough on the Kaggle CPU image, which does not ship Linux libraries such as
`libatk-1.0.so.0`. Browser-launch failure also tears down the local release
fixture immediately, so an environment defect is reported directly rather
than being misclassified as a five-minute journey timeout.

The reviewed desktop keyboard navigator V7 is documented in
[keyboard-event-navigation-prototype.md](keyboard-event-navigation-prototype.md).
Its exact command router is shared by the historical lab and the primary event
template. It is enabled only in checked noindex secret candidates, only at the
desktop breakpoint, and only after meaningful keyboard intent; root event pages
and listing/mobile routes remain untouched while the wider browser,
accessibility and rollout gates are open.

В `events-bot-new` есть Astro SSG preview и отдельный checked production profile в `site/`: он строит
статические страницы событий, `event.ics`, `sitemap.xml`, `robots.txt` из
production SQLite export и публикуется под noindex-prefix в bucket
`kenigevents.ru`. Production root-form artifact уже проверяет indexable canonical
output и полный eligible catalog, но это ещё не event-page production rollout:
Object Storage website не умеет reader-atomic переключать целое дерево через
`current.json`, поэтому root promotion отсутствует и остаётся `NO-GO`.

Принятый промежуточный release mode — один immutable noindex candidate под
`/_review/<256-bit-token>/`. Он собирается из того же snapshot/repo SHA, что и
checked root-form artifact, загружается create-only и никогда не меняет `/`,
`current.json` или стабильные `/ics/*`. Это неперечисляемая bearer-ссылка только
после отключения anonymous bucket listing; она не является авторизацией и может
быть переслана получателем.

### Каноническая preproduction-ссылка

Ссылки вида `preview-2026…` ниже сохранены только как датированное acceptance
evidence. Код, бот, operator scripts и новые review-документы не должны выбирать
из них «последнюю». Единственный текущий review target — последний полностью
проверенный и опубликованный immutable candidate в durable
`static_site_build_state.current_secret_candidate_receipt_json`. Он содержит
build/run/repo/snapshot/result/manifest/token hashes и bearer URL; failed,
unchanged/no-op и artifact-only build не могут заменить его. Публичного
`current` redirect/object нет и не будет на preproduction-этапе.

Получить target через общий read-only resolver:

```bash
.venv/bin/python scripts/request_static_site_build.py \
  --db /data/db.sqlite --show-current-review
```

Resolver fail-closed возвращает `current_review_unavailable`, если receipt
неполон или не доказывает noindex/prefix/root-isolation публикацию. Bearer URL
нельзя коммитить, добавлять в sitemap или отправлять в публичные каналы.

### Historical acceptance evidence: 2026-07-15 replacement candidate

`preview-20260715t-production-desktop-contract-v2` is the replacement noindex
review surface. Unlike the rejected v1 build, every generated desktop event
route mounts the **same** `DesktopEventPage.astro` implementation used by the
accepted laboratory scenarios; there is no legacy `EventHero + event-grid`
desktop imitation. A geometry- and semantic-state-aware router selects only
the accepted Continuous Editorial or Split family. Portrait and
resolution-constrained media fail to Split; only a classified
`event_identity_poster` can create an OCR companion. The unchanged mobile v4
DOM is retained as a separate breakpoint surface. Rail/bus transport is added
inside the accepted long reading flow rather than replacing its composition.

The source snapshot contains `282` public future or ongoing events. The
mandatory gate checks all `282` generated routes and pins four real specimens:
Pianissimo `5294` (low-resolution Split), Blogger bus `6815` (portrait Split),
«Гараж» `5658` (Continuous Editorial) and «Эпидемия» `4671` (Editorial with a
classified poster companion).

- index: <https://kenigevents.ru/preview-20260715t-production-desktop-contract-v2/__preview/>;
- [Pianissimo](https://kenigevents.ru/preview-20260715t-production-desktop-contract-v2/sobytiya/kontsert-festival-pianissimo-maksim-miloslavskiy-kaliningrad-5294/);
- [Блогерский автобус](https://kenigevents.ru/preview-20260715t-production-desktop-contract-v2/sobytiya/blogerskiy-avtobus-splav-na-baydarkah-kaliningrad-6815/);
- [«Гараж»](https://kenigevents.ru/preview-20260715t-production-desktop-contract-v2/sobytiya/spektakl-garazh-kaliningrad-5658/);
- [«Эпидемия. Огненная рукопись»](https://kenigevents.ru/preview-20260715t-production-desktop-contract-v2/sobytiya/epidemiya-ognennaya-rukopis-kaliningrad-4671/).

`preview-20260715t-production-transport-mobile-real-events-v1` is rejected by
`INC-2026-07-15-static-desktop-template-regression`: it rendered the legacy
production desktop DOM with approximation CSS, routed a `180×320` image into a
full-width hero, and was never reviewed by Gemini against the mass-generated
URLs. It must not be cited as desktop acceptance evidence.

The offline related graph is `event_pgvector_related_chain_v2_two_doc`: all
`282` anchors have `40` current candidates, no dangling ids, and the refresh
reused `564` unchanged embeddings with `0` provider calls. Browser page views
still consume only same-origin static discovery JSON. The replacement local
acceptance covers the whole `282/282` catalog with no page errors, plus a
`4 events × 3 viewports` (`1536×864`, `1920×1080`, `1440×650`) matrix with
visible H1/CTA, action-panel child geometry and zero horizontal overflow.
Phone variants must prove a one-line number and every calendar/share/like
control inside the graphite panel; text presence alone is not acceptance.
Interaction checks cover exact
gallery indices, classified-poster opening, idle autorotation, CTA safe
release, immutable thumbnail derivatives and both transport types. Public HTTP
is `200` for the preview index, four pinned events and both transport examples;
the public `4 × 3` matrix is `12/12` and the exact interaction suite has no
failures. Gemini 3.1 Pro's direct browser attempt was correctly recorded as
`BLOCKED` after its isolated Chromium crashed. It was not mislabeled as a pass;
the follow-up screenshot-based review inspected exact public Playwright
captures and evidence JSON and returned `ACCEPT`.

The current production policy keeps the synchronous Smart Update build on
pgvector rather than enabling the old per-anchor Gemma verifier. That verifier
is not one extra request per build: with two candidate passes it is normally
two provider calls per anchor (about `496` successful calls for a 248-page
catalog, plus retries), and a partial run can starve the public `Похожие` list.
Gemini 3.1 Pro review therefore selected **routine pgvector + targeted offline
LLM audit**, not LLM in the publication critical path. The UI keeps two honest
surfaces: semantically close events under `Смотрите дальше`, then a visibly
separate finite `Ещё события`/personalized anti-bubble block with category and
venue diversity limits.

Pgvector publication is revision-bound and topology-gated. Vector sync writes
one atomic durable receipt with deterministic `search_v3_hash` and
`related_v1_hash`; Smart Update passes the validated `related_v1_hash` into the
Kaggle export and the related cache must match it exactly. Selective graph
repair guarantees reciprocal eligible exact-normalized-title pairs, restores
only high-confidence cosine reverse links (`>=0.88`), and attaches a zero-
incoming event to a neighbour only as broader discovery when the edge is not
semantically strong. Full-catalog generation fails before publication when
the zero-incoming rate is `>=5%`, an eligible exact-title pair is disconnected,
or an anchor has fewer than four candidates. Equal titles create a duplicate/
occurrence **suspect and mutual discovery edge**, never an automatic merge;
canonical identity remains an LLM-first Smart Update decision with source,
date and venue evidence.

Every automatic root-form proof and immutable secret candidate also executes
the blocking browser release gate against its generated files before the
archive is trusted. The gate checks final recommendation `object-fit` against
the shared card layout on both discovery surfaces, performs a real two-page
gallery-recommendation navigation before testing destination hero arrows, and
tests visible-footer `P`/`S` from `BODY` and retained off-screen event focus. It does
not intercept navigation or pre-focus the share buttons, which were the two
false-positive patterns in the rejected test.

This is a prefix-only review release. It does not promote or delete the
production root and does not modify stable `/p/` media or `/ics/` calendars.
Automatic root promotion remains the release-protocol gate.

Mobile event-detail UI был отработан в контролируемом preview lab: четыре исходных
варианта образуют матрицу `current/open prose × current/grouped actions`,
`accepted-v2` сохраняет первый исправленный проход, `accepted-v3` — первый
Android feedback, а `accepted-v4` фиксирует поправку владельца: сохраняет
принятую weekday/date/time hierarchy, возвращает OCR-parallax без zoom и
layout gap, упрощает selected-like до терракотовой заливки и белого solid
heart и вводит явный вертикальный ритм между информационными поверхностями.
V4 перенесён в общий integration preview; бирка, discovery cards и sticky CTA
намеренно зафиксированы. Канонический scope
и acceptance gate:
[`event-mobile-ui-lab-2026-07-15.md`](event-mobile-ui-lab-2026-07-15.md).

В `events-bot-new` есть **Astro SSG production-integration preview** в `site/`:
он строит статические страницы полного текущего публичного каталога,
`event.ics`, transport ICS, `sitemap.xml`, `robots.txt` и публикуется под
noindex-prefix в bucket `kenigevents.ru`. Это ещё не production rollout:
canonical остаётся preview-safe, а корневые production URL
`/sobytiya/<slug>/` не продвигаются автоматически.

Текущий preview реализует production-oriented форму по паттерну соседнего `kgd80/site`: production SQLite export/static manifest → `getStaticPaths()` → `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/vystavki/`, `/populyarnoe/`, `/poisk/`, `/partnerstvo/`, `/sobytiya/<stable-slug>/index.html` → `event.ics` → `data/discovery/<event_id>.json` → sitemap/robots/JSON-LD → preview `noindex` → publish to Yandex Object Storage bucket `kenigevents.ru`. Служебные QA/product страницы `/lab/*` остаются только в preview или в явно перечисленных noindex regression specimens. `/partnerstvo/` является обычной сервисной страницей: в ней нет тестового расписания или fixture-данных, поэтому production/candidate генераторы обязаны сохранять её и проверять через sitemap/output gate. Следующий release step — включить и доказать автоматический Smart Update → Kaggle → checked artifact → atomic production promotion/rollback path.

### Единый review-прототип типов страниц

`/__preview/` — каноническое оглавление ручной приёмки внутри конкретного
immutable `preview-*` prefix. Одна ссылка адаптируется под desktop/mobile и
связывает реальные страницы: Today/Tomorrow/Weekend, Exhibitions, раздельные
desktop V28 и mobile Popular representations, Search, materialized Search
collections, preview-only cold-start `/dlya-menya/`, clubs, `Партнёры`,
partnership и event detail. Отдельные `/lab/*` ссылки вынесены в QA appendix и
не подменяют продуктовую навигацию.

Сборка берёт только актуальную public projection из read-only production
SQLite snapshot. Пустой clubs dataset показывается честным empty state, а не
синтетическими карточками. `/dlya-menya/` существует в этом integration
prototype только для целостного mobile journey: без достаточных локальных
сигналов он явно называет выдачу cold-start fallback и использует `per-family`;
это не новый SEO landing и не заявление о готовой production personalization.
В hub и handoff обязательно фиксируется, что Search принят визуально, но его
реальный auth/backend journey ещё не принят. Даже без публичных Supabase env
`/poisk/` рендерит честно отключённый трёхстрочный prompt и образцы skeleton
карточек: пользователь видит форму будущего продукта, но disabled CTA и status
не выдают макет за работающий поиск.

Generated-output gate `npm --prefix site run check:unified-prototype` проверяет
наличие и взаимную перелинковку типов страниц, preview canonical/noindex,
prefix-local targets, общий mobile dock, отдельные Popular representations,
short label `Партнёры`, реальный reciprocal occurrence selector, bus/rail
specimens и compact-related `cover`/OCR ≤20% crop. Он дополняет, но не заменяет
`check:preview`, occurrence tests, incident regressions и responsive browser
gate. Публикация допускается только в новый prefix; production root, public
pointer и stable `/ics/*` не изменяются.

Отдельный interactive product prototype страницы выставок доступен на
`/lab/exhibitions-personal/`. Он сохраняет production `/vystavki/` без изменений
и проверяет dark timeline/photo-deck подход, new-inbox, `Для меня / Все`,
like/`Не интересно`/undo, объяснимое recent/popular/ending ранжирование,
progressive disclosure старого хвоста и keyboard navigation. Канонический
контракт и ограничения: [exhibitions-personal-prototype.md](exhibitions-personal-prototype.md).
Для медиа export обязан передавать не только `image_text_mode`, но и LLM-first
`media_role`, semantic status/confidence, dimensions, focal metadata и
content-addressed 256/512 WebP derivatives. Только строгая роль
`event_identity_poster` создаёт крупный poster companion; карточка услуг,
расписание или иной документ не становится афишей из-за наличия OCR. Astro build
сам OCR/VLM не запускает. Канонические роли, fail-closed contract и backfill:
[event-media](../event-media/README.md).
Event gallery media имеет отдельный fail-closed CDN contract: exporter читает
только approved `EventPoster.supabase_url` на `static.kenigevents.ru`, допускает
только host-canonicalization raw URL текущего `kenigevents.ru` bucket и никогда
не откатывается к `catbox_url`, source CDN, Supabase или legacy bucket. Silent
rows исключаются тем же static predicate; продолжающиеся события остаются
eligible по `end_date`.

Footer service share использует отдельный от event-share контракт: на mobile —
одна system-share action, на desktop — независимые `image/png` и plain-text +
canonical URL intents. Каноника: [service-sharing.md](service-sharing.md),
[desktop clipboard research](service-sharing-desktop-clipboard-research.md),
[manual matrix](service-sharing-desktop-clipboard-manual-matrix.md) и
[preview runbook](../../operations/service-sharing-preview.md).



### Announcement lockup and favicon

The production header uses one `AnnouncementsLockup.astro`: `240×88` on
desktop and `128×96` on mobile. Desktop navigation stays to the right; active
state exists only for the exact listing context, while event details have no
selected menu item. The accepted favicon is a transparent `#98401f` tag with a
wide white «о» (`glyph` centre `y=36`, outer geometry `y=24…48`). The empty tag
and historical ПК/favicon variants are rejected. Canonical geometry and usage:
[`design-system/brand-lockups.md`](design-system/brand-lockups.md) and
[`design-system/favicon.md`](design-system/favicon.md).

### Партнёры

Сервисная institutional-страница `/partners/` добавлена как статическая preview-страница для партнёров. Пользовательское имя каталога во всех navigation/footer surfaces и его H1 — строго `Партнёры`; отдельный collaboration CTA называется `Стать партнёром`, чтобы не подменять каталог описанием типа сотрудничества. Она не меняет event-detail модель: стартовый список партнёров хранится отдельно в `site/src/data/info-partners.ts`, рендерится в `site/src/pages/partners/index.astro` и попадает в `sitemap.xml`. Страница должна оставаться компактной плоской logo-first bento-доской: на mobile — 4 плотные колонки с full-occupancy bento placement, где `80 историй` занимает 2×2 hero-ячейку, на tablet/desktop — 8-колоночные aspect-aware spans для широких и вертикальных логотипов; высота grid-рядов и logo/caption frames фиксированы, а подпись не управляет размером ячейки. Текущий contract запрещает публичные категории вроде `Пригородные маршруты`/`Лекции и музыка`, неверный label `КППК / РЖД`, тяжёлые card borders/backgrounds/shadows и декоративные highlight-плашки. Внешний переход — вся плитка партнёра с `rel="nofollow noopener noreferrer"`; отдельный CTA `Сайт партнёра` не рендерится. Каноника фичи: `docs/features/info-partners/README.md`.


### Kaggle CPU build handoff

Static-site generation reuses the Kaggle CPU/status-ledger infrastructure: one
private input dataset, `static_site:builder` lease and heartbeat per run. Smart
Update records one bounded request payload and debounces until 15 minutes after
the latest effect. A new effect during a running build produces exactly one
merged follow-up. Fly creates an online-backup SQLite snapshot, runs
`quick_check`, records SHA/size/watermark, and only that immutable file reaches
Kaggle. The same outbox is used by `scripts/request_static_site_build.py`.

Kaggle returns exactly three hash-checked artifacts plus bounded result JSON:
an indexable root-form proof, a prefix-contained noindex candidate, and a
browser-evidence archive containing the generated-tree JSON receipts and
settled-pixel recommendation screenshots for both trees. Publication
is a trusted Fly/operator-side create-only step guarded by
`ENABLE_STATIC_SITE_SECRET_PUBLISH`; Kaggle receives no bucket credentials.
Both generation flags default off until the controlled canary is accepted.
Operational protocol: `docs/operations/kaggle-static-site-builder.md`.

Interest-club pages are an additional versioned consumer of this checked build,
not a second publisher.  Only the accepted club projection may create
`/kluby-po-interesam/` index/detail routes; non-public review states must never
enter the manifest.  Club projection changes coalesce into the same static
build lifecycle, preserve last-good output, and require relation/manifest
parity before promotion.  Canonical identity and staged production gates:
[Interest clubs](../interest-clubs/README.md) and its
[release plan](../interest-clubs/release-plan.md).

## Historical public preview evidence

The current desktop `Популярное` contract is documented in
[`listing-surfaces-v28-desktop-popular.md`](listing-surfaces-v28-desktop-popular.md):
five short evidence shelves use a stricter build-time availability cutoff and
page-wide family deduplication, while an optional sixth 4+1 shelf applies the
existing local profile only for warm consented visitors. Mobile V26 rendering
is deliberately unchanged.

The current mobile `Популярное` sticky group-context pass is documented in
[`listing-surfaces-v26-mobile-sticky-groups.md`](listing-surfaces-v26-mobile-sticky-groups.md):
it keeps the full semantic category heading, then compacts that same heading
into a right-aligned sticky label while its group is being scanned. The V25
context and hierarchy baseline remains documented in
[`listing-surfaces-v25-mobile-context.md`](listing-surfaces-v25-mobile-context.md):
it preserves the same viewed event across both density controls, replaces the
desktop-like phone header stack with the existing mobile navigation contract,
makes ranked categories visually explicit and narrows compact evidence rails
after measuring the rejected all-under-photo alternative. The V24 density and
pinch baseline remains documented in
[`listing-surfaces-v24-mobile-pinch.md`](listing-surfaces-v24-mobile-pinch.md).
The V23
component-reuse baseline remains documented in
[`listing-surfaces-v23-mobile-adaptive.md`](listing-surfaces-v23-mobile-adaptive.md):
the large phone mode directly reuses canonical `EventCard split-actions`, the
adaptive compact mode keeps intrinsic listing-card geometry and immutable rank
order, and the accepted V22 desktop plus equal two-column phone specimen remains
available separately for regression/comparison.

Latest main-reachable checked public preview evidence as of 2026-07-17 is
`preview-20260717-interest-clubs-prod-canary`: 303 current/future events, checked
Astro output and the gated club projection. Exact SHA/counts/manifest/HTTP evidence
is recorded in `docs/features/interest-clubs/release-plan.md#production-evidence-2026-07-17`.
It is still a noindex preview and does not close the event-page production gates.

Historical v44 CDN/Kaggle baseline: public preview `preview-20260628-event-pages-v44-cdn-kaggle` was built by Kaggle CPU from the 2026-06-28 production snapshot (80 real events), event images now render through `https://static.kenigevents.ru/p/...`, stable calendar CTAs use `https://static.kenigevents.ru/ics/<event_id>.ics`, and deploy copied 80 `.ics` files to the CDN bucket. The v43 UI/gallery fixes remain: wrapped mobile tag geometry, adjacent gallery preload/decode, paid real price links with `rel="nofollow"`, and a diverse same-day `/segodnya/` slice.


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

Historical v44 preview scale/evidence (Kaggle CPU build from the 2026-06-28 production snapshot):

- preview fixture: `80` real active events, including `49` events starting on 2026-06-28 across `14` event types;
- generated output: `95` static pages, `261` files, `28 MiB`;
- Kaggle build result: `event_count=80`, `started_at=2026-06-28T17:38:08Z`, `finished_at=2026-06-28T17:38:31Z`; public bucket upload from this workstation plus stable ICS copy completed after artifact download;
- full active-snapshot estimate measured by a local full export/build: `386` active future/intersecting events → `403` pages, `1185` files, `128 MiB`, `0:20.16` wall time (`maxrss≈522 MiB`). Upload/publish time is expected to scale mostly with file count/bytes rather than Astro render time.
- media CDN verification: `957` active legacy `/p/...` keys from `kenigevents` are present in `kenigevents.ru`; sample CDN image and `https://static.kenigevents.ru/ics/5878.ics` return `200`; rendered v44 HTML contains CDN `/p/...` and stable `/ics/...` links, not raw legacy image URLs.
- media CDN verification: `957` active legacy `/p/...` keys from `kenigevents` are present in `kenigevents.ru`, sample CDN image and `https://static.kenigevents.ru/ics/5878.ics` return `200`; rendered v44 HTML contains CDN `/p/...` and stable `/ics/...` links, not raw legacy image URLs.

Preview `v39` keeps the consultant P0 hardening, explicit discovery feedback and feed-card comparison, and adds the current UI/data refinements: the mobile brand tag removes the rejected icon and keeps only a subtle periodic title sway; the fullscreen gallery uses a slower/farther `38%→64%` pan and advances before the pan fully stops; `Фото события` and the event title are forced onto separate caption lines; `Пушкинская карта` is rendered as a green check property, not a text value; admission/free states use compact property labels instead of bare `Бесплатно`; and the desktop event nav no longer exposes `Sitemap`. It also keeps date-listing/navigation refinements: `/segodnya/` is grouped into `Утро / День / Вечер / Ночь`, `/zavtra/` exists as a separate tomorrow page with the same sections, date-listing cards use a compact mobile plaque with a cropped left photo column and a straight separator to the text column, and list cards no longer expose mass external ticket/source links; users go through the internal event page first. The mobile discovery drawer handle no longer shows the icon; it uses only a subtle title sway in the site palette. Preview `v39` also hardens the event hero into a stronger mobile-first surface: the hero **image itself** is guarded by preview checks at bbox-style full-width contract (`x=0,width=viewport`) with no layout side gutters; the normal mobile header is replaced over the hero by a TASS-like terracotta drawer handle (`Полюбить Калининград / Анонсы`). Tapping it opens a no-JS `<details>` discovery drawer implemented as one monolithic sliding object: the full-width navigation rail and handle move together, overlap by a few pixels, have no transitional gap, no chevron/up-down icon, no rounded dropdown panel and no pill-buttons inside the rail. Visual-only hero variants use stable `svh` sizing plus constant-scale vertical parallax so mobile browser chrome changes should not cause a post-scroll scale jump. The current event page itself exposes first-party like/unlike with the honest aggregate like count, and share counters are present but stay empty when the total is zero. Feed cards use `split-actions` as the baseline, not an A/B on normal event pages: `Поделиться` is clustered near the right-thumb like action below the card, while `Не интересно` is demoted to a quieter utility action. The fullscreen hero viewer now uses a visible on-image photo CTA (`Фото N` when multiple images exist), lazy-hydrates gallery images from `data-gallery-src` only after opening/navigating, uses full-viewport-height `cover` + one-way right-to-left auto-pan for `visual_only` photos to avoid black side fields, auto-advances to the next photo after the pan, pauses forward auto-advance after a manual backward swipe, keeps OCR/text images in the base `contain` mode, keeps the service tag visible in the gallery, and places the event title in a readable bottom stripe. JSON-LD offer `validFrom` is emitted as ISO 8601 with timezone and JSON-LD `image[]` includes the event gallery assets so lazy images remain connected to the event for SEO/GEO. The historical comparison is in `docs/features/static-site-pages/event-card-ui-ab-2026-06-27.md`; hero decisions are in `docs/features/static-site-pages/event-hero-lab-2026-06-27.md`. The inherited v32 interaction/SEO hardening remains: in fullscreen gallery the service tag is a real top-flush navigation link, not a floating label; visual-photo pan starts at `38%` and moves to `64%` over `17.9s`, which gives a slower/farther right-to-left image motion; manual backward motion uses `64% → 38%`; the event title in the fullscreen viewer uses inline/subline stripes via `box-decoration-break`, not a full-width bottom slab. Event cards now place the title before time/status meta because the feed scan task starts from “what is this?”, then date/conditions; service controls (`Не интересно`, share, like/undo plate) are marked `data-nosnippet` and remain buttons, not crawlable links. Phone-only desktop CTAs keep the branded primary button, use the shared copy icon instead of a redundant phone pictogram, reveal the number on click, copy it to the clipboard and announce success without adding a layout-shifting helper row.

Other v39 contracts remain: visible description, one vertical neutral `Смотрите дальше` feed, no user-facing “try another genre” block, large right-thumb like buttons with counts and unlike, “Не интересно” negative feedback, native-share-first button, transparent terracotta wide-«о» tag favicon, prefetch for static links, and sticky CTA hiding while the hero is visible and again when the user reaches the feed. The after-hero drawer handle remains visible when closed so navigation is never lost while scrolling; the monolithic root transform keeps the panel off-screen and only the handle protrudes. Current media rule is surface-specific: compact event-detail recommendation cards all use bounded `cover` with exported focal/object-position metadata when available; OCR/unknown documents constrain the globally optimized row and only very tall documents may crop up to 20%. Large OCR/document surfaces retain their strict full-poster policy, while every `visual_only` hero/gallery asset fills through `cover` even when semantic-role metadata is uncertain. Compact date-listing thumbnails remain a separate navigation-preview exception. Desktop event detail uses the continuous Editorial composition for a strong
`visual_only` landscape and a split/contained fallback for documents; a
dedicated companion remains reserved for strict `event_identity_poster`.
Within the detail hero/fullscreen gallery a positive `visual_only` result is
the display crop boundary even while asynchronous `media_role` enrichment is
pending: ordinary photos use bounded `cover`, while OCR/unknown-text documents
and positively classified non-photo documents use `contain`. There is no crop for OCR, no duplicate underlay, no
blur/backdrop fill and no repeated image edges. Each event page statically preloads up to 10 continuation candidates in HTML; after JS starts, the page uses only a consented compatible local profile (`ke_personalization_profile`, UUID `anon_id/session_id`, `event-detail-related-v1` + `event-taxonomy-v1`) to filter/rerank. The client removes already hidden / `not_interested` / strong negative-interest matches from the preloaded cards, performs one same-origin JSON hydration from `/data/discovery/<event_id>.json`, where the payload is an `event_detail_related` manifest with `related_static[]`, and top-ups relevant candidates; subsequent expansion is only by `Показать ещё`. Local strong actions currently write a compact browser log with `served_list_id` / `served_list_hash` context for future Supabase telemetry mapping. Important status: the v39 static preview does **not** persist first-party likes/profile snapshots to Supabase yet; the preview contract keeps like/profile writes local-only and forbids treating this as Supabase persistence. Source counters are already synced to Supabase by the production metric pipeline, but browser feedback remains same-browser/local until the dedicated gated write path (same-origin endpoint or append-only Supabase RPC with RLS/grants) is implemented. Cards are full-clickable for users while keeping real HTML links on media/title for SEO/GEO; double-tap like is disabled because it conflicted with navigation. `Не интересно` turns the acted-on card into a grey explanatory plate with an explicit `Отменить` action; tapping the plate itself must not navigate to detail. Visible like/share counters are hidden when the total is zero. Visible like counts are honest totals: `likes_count = source_likes_count + service_likes_count`, where source likes come from production TG/VK post metrics and service likes are first-party KenigEvents likes; public HTML/UI shows only this total, not the technical source/service split. The hero no longer duplicates facts as a second info block; it keeps only a compact meta line, while the single `Коротко` block owns icon facts (`Где` combines venue + address, `Вход`, optional `Пушкинская карта`/festival), no longer links to Telegraph, and no longer exposes source count/views in public HTML. The registered-user sources/mentions notice belongs to the parent details section as a bottom strip, not to the `Коротко` fact block. Footer is now a compact navigation block: top links (`Сегодня`, `Завтра`, `Выходные`, `Все анонсы`), crawlable editorial social links with icons + short labels, and contact email `info@kenigevents.ru`.

Desktop deliberately keeps a visible semantic boundary after the explicitly
similar `Смотрите дальше` set. The following `Ещё события` / mature-profile
`По вашим интересам` section is a finite broad-discovery module, not another
similarity claim and not an infinite feed: it renders at most six deduplicated
cards (a few desktop rows according to the resolved grid), then `Все анонсы`.
Its candidate mix may interleave compatible profile results, non-rejected
vector-adjacent tail and a diverse upcoming fallback under category/venue caps,
so a user is not trapped in a theatre/type bubble. Mobile keeps the established
single continuation surface and must not duplicate this desktop-only module.
Both server-rendered related cards and runtime-selected broad cards use the same
canonical `EventCard` DOM contract and interaction controller; sharing only row
geometry while maintaining a second handwritten card renderer is forbidden.
The keyboard graph treats these as two explicit zones: arrows may bridge from
the last row of `Смотрите дальше` to the finite broader section, while headings
and selection semantics keep “clearly similar” separate from anti-bubble
discovery. Runtime feedback, share and rerank operations must preserve the same
card controller and logical focus owner rather than recreating card markup.

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
- `Вход` uses a compact primary chip plus subtype text. Free events must keep the word `Бесплатно`: `Бесплатно · вход свободный`, `Бесплатно · регистрация`, `Бесплатно · по записи`; then `За донат`, `Запись по телефону`, the neutral destination `Билеты` for paid/ticketed events without a reliable price, `Источник события` for a source-only/unknown ticket state, or the real price/range when present. The rejected labels `Узнать цену`, `Открыть условия` and `По билетам` must not return. If a paid event has both `price_label` and a ticket URL, the price chip itself may be the purchase link; because it is transactional/external, it uses `rel="noopener noreferrer nofollow"` and `data-nosnippet`, while the internal event page remains the SEO/GEO target.
- Real production scan on 2026-06-28 found 385 future active rows with free/admission clues: `free_word=59`, `registration=34`, `booking/запись=25`, `comments=4`, `limited=18`, `donation=2`. This confirms that “free” is not a single state: it can mean open entry, registration, booking, comment signup, limited seats, donation, or mixed paid/free ranges.

## Media/gallery and Smart Update image metadata requirements

Static exporter — read-only consumer [event-media approved projection](../event-media/README.md):
он выбирает ровно один display URL на один `EventPoster`, не объединяет managed
и source alternate locations и не добавляет `Event.photo_urls`, если approved
poster rows уже существуют. `pending_review`, `duplicate`, `rejected` и
`unavailable` никогда не входят в gallery.

For events with multiple images, MVP must **not** auto-rotate the first-paint hero by default. The event page chooses one deterministic best hero image for first paint. A tap/click on the hero opens an explicit immersive fullscreen gallery; only inside this opened viewer `visual_only` photos may run a controlled auto-forward pan/advance sequence. Current v43 state machine: forward pan is `38% → 64%` object-position over `17.9s` (right-to-left visual motion), auto-advances by an `8.88s` timer so there is no long dead wait with `gallery-pan-forward` as a fallback, preloads and `decode()`s the next adjacent image slides before the timer fires so the transition does not start over a black empty slide, manual backward swipe/click sets `autoAdvance=false` and runs only the local reverse pan, and `prefers-reduced-motion` remains a hard guard. The final gallery slide can be an explicit CTA card (`Купить билет`, `В календарь`, `Смотреть похожее`), visibly labeled as a CTA, not disguised as another image. Pull-to-expand/top overscroll hero behavior is P1/P2 lab-only because it can conflict with browser pull-to-refresh and mobile address-bar gestures.

Smart Update / image preparation must enrich image assets in batch/offline, not in the static page runtime. When OCR/media analysis runs, it must return enough metadata for safe crop decisions:

- `image_assets[]` with `src`, dimensions, `alt`, `image_text_mode`, `image_kind`;
- `ocr_boxes[]`, `image_text_density`, dominant text regions and `recommended_hero_fit`;
- `face_boxes[]`, `face_count`, `face_confidence`, `saliency_boxes[]`;
- `focal_point` / `recommended_object_position` in normalized 0..1 coordinates or CSS object-position form;
- `safe_crop` / crop guards: do not crop OCR text, and do not cut detected faces, especially top face bounds.

Runtime policy is surface-specific. Event-detail recommendation rows have a
fixed compact ratio: `visual_only` authorizes `cover`, with exact geometry or
exported focal/object-position metadata improving its position and a centered
fallback when enrichment is missing. OCR/text/unknown images remain contained.
The event-detail hero, fullscreen gallery and other large/responsive surfaces
retain the stricter contract: `cover` requires classified `event_photo`,
positive semantic permission and current exact-pixel geometry whose protected
face/value union fits the known target ratio; otherwise they use `contain`.
Thus the compact-card correction does not weaken large-surface protection.

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
- Telegraph event-detail остаётся временным compatibility/fallback layer на
  **10 дней после фактического T0**, затем новые event Telegraph pages больше не
  создаются; month/weekend/festival surfaces выключаются отдельно после появления
  static replacements.
- Доступы к Yandex Cloud/Object Storage будут выданы отдельным шагом; до этого проектируем контракт и пайплайн без привязки к конкретным credentials.

## Связанные документы

- Release umbrella and F1–F17 map: `docs/features/static-personal-announcements/README.md`.
- Event-page production release and 10-day Telegraph cutover: `docs/features/static-site-pages/release-plan.md`.
- Test/release scenario inventory: `docs/features/static-site-pages/test-scenarios.md`.
- Release UI contract: `docs/features/static-site-pages/release-ui-contract.md`.
- Event sharing/generated images: `docs/features/static-site-pages/event-sharing.md`.
- Image framing/focal metadata: `docs/features/static-site-pages/image-framing.md`.
- Event transport: `docs/features/event-transport/README.md`.
- Event comment feedback: `docs/features/event-comment-feedback/README.md`.
- Admin issue reporting: `docs/features/event-issue-reporting/README.md`.
- Astro SSG preview runbook and public URLs: `docs/features/static-site-pages/astro-preview.md`.
- CDN asset delivery: `docs/features/static-site-pages/cdn-asset-delivery.md`.
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

`static_related_v1` is a build artifact over the current active event catalog. If a new event appears, it can become a good recommendation for already published event pages. MVP decision: do not maintain per-event reverse dependency updates in the hot path. Rebuild and republish the full static event slice nightly, because the generated HTML/JSON is cheap and this avoids stale related blocks without complex invalidation.

Operational rule:

- immediate/same-run build publishes the new/changed event page and its own `/data/discovery/<event_id>.json`;
- existing pages may keep yesterday's `Смотрите дальше` until the scheduled nightly rebuild;
- emergency/manual rebuild is allowed after major imports, large festivals, or quality fixes;
- future optimization may add reverse-impact rebuilds by same city/category/date bucket, but only after manifest-hash evidence shows nightly full rebuild is too slow.

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
- “Другие даты” рендерятся компактным selector рядом с primary date/CTA, не
  full-card grid, и не смешиваются с “Похожие события”;
- client island может после consent переупорядочить уже полезный block, но не должен ломать CTA/SEO;
- mobile рендерит related как вертикальную continuation/feed-секцию `Смотрите дальше`, не горизонтальный rail; desktop остаётся desktop-native grid/module, а не растянутой мобильной лентой;
- после явно похожих карточек desktop отдельно и честно обозначает переход к
  конечной широкой выдаче `Ещё события` / `По вашим интересам`: максимум шесть
  карточек без load-more/infinite scroll, с anti-bubble diversity; этот модуль
  не дублируется на mobile;
- статическая и runtime-персонализированная выдача используют один `EventCard`
  DOM/behavior contract; отдельный строковый `eventCardHtml` не допускается;
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

Канонический период dual-run для **event-detail pages** — 10 дней после
фактического production `T0`, а не месяц и не десять дней от даты написания
документа. D0 начинается только после production profile, stable URL registry,
catalog parity, manifest/promotion/rollback и downstream resolver gates.

Переход: D0 10% static links → D2 25% → D4 50% → D6 100% → D7–D9 72-hour
soak → D10 static-only outward links и Telegraph `existing_only`. На D10 запрещены
create и fallback-recreate новых event pages, но старые `telegraph_url/path` и
старые Telegram/VK посты сохраняются. Aggregate month/weekend/festival Telegraph
pages остаются отдельным scope до появления их static replacements.

Flags, измеримые go/no-go thresholds, rollback и обязательные cutover-тесты
зафиксированы в [release-plan.md](release-plan.md#десятидневный-telegraph-coexistence).

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

## Mobile calendar rail research v22 (2026-07-21)

В `v22` параллакс-вариант `right-corner-hero` принят как основной вариант
мобильной календарной страницы; статическая версия остаётся только контрольной.
Начиная с focus candidate 29.07 эта механика принадлежит общему
`DateListingHero.astro` и используется на `/segodnya/`, `/zavtra/` и
`/date-YYYY-MM-DD/`; диапазон `/vyhodnye/` намеренно исключён.
Build-time selector выбирает событие только на точную дату и только при
активном occurrence с `visual_only`, photo-safe role, `safe_crop=true`,
`cover`, источником не меньше `1000px/1MP`, focal point и совпадающими current /
geometry pixel hashes. Если такого события нет, календарь остаётся без hero:
OCR, unknown, соседняя дата и случайный poster fallback запрещены.
Композиция переносит принятый `v23` contract без свободной интерпретации:
`11×6` квадратов, невидимый `1×1` preload вместо видимого полного изображения,
слабое поле внизу слева и сильное вверху справа. Поэтому до и после decode
пользователь никогда не видит цельный raster, а дата остаётся первым визуальным
слоем. Базовое поле совпадает с donor; `crypto.getRandomValues()` один раз за
page load задаёт irregular entry, jitter и порядок исчезновения. Height-only
resize сохраняет этот seed, reload получает новый. Пересечение с текстом
ограничивает opacity до `.04`, первые два столбца — до `.06`, лицо при fresh
geometry защищается минимумом `.56`, общий максимум `.92`.

Hero начинается у `y=0` внутри page surface; mobile shell остаётся высотой
`64px`, без дополнительной двадцатипиксельной полосы. Перемещение декора
замедлено до компенсации `scrollY × 0.28` (видимая скорость примерно `0.72×`
скорости страницы). При скролле вперёд opacity каждой плитки может только
уменьшаться, при обратном — восстанавливаться. Вся композиция является обычной
внутренней `<a>` на выбранный occurrence уже в no-JS HTML; tiles декоративны,
focus видим, а `prefers-reduced-motion` оставляет именно статичную частичную
мозаику и отключает parallax — не возвращает цельное фото.

Компактный rail сохраняет fail-closed правило: OCR и неизвестный text-mode не
кропаются только ради заполнения карточки. Исключение допускается для конкретного
визуально подтверждённого `visual_only` asset и не переносится на соседние
`unknown`. Для такого одиночного фото используется горизонтальное окно `5:4`
(`140×112`) с focal-aware `cover`; при отсутствии focal metadata допустим центр
`0.5/0.5`. Исследовательский override события `6764`/постера `13792` имеет
источник решения `user-verified-visual-only-v22`. Его исходник `180×320` не даёт
настоящей Retina-детализации: `2x/3x` rendition здесь лишь апскейл, поэтому
production-ready исправление требует повторного semantic/geometry enrichment и
более крупного источника, а не расширения override на весь каталог.

Обязательные browser gates для этого варианта: отсутствие горизонтального
overflow на `320/390px`, декодирование media, окно события `6764` ровно `5:4`
без искажения, неизменность hero schedule при height-only resize, монотонность
tile opacity в обе стороны и после reload, transform
`22.4±1px` на `scrollY=80` и `transform:none` при reduced motion.

## Automatic home Hero Talk (2026-07-29)

Главная использует production `HomeHeroTalk.astro`, который переносит механику
принятого `briefing` donor `b5f4797d`, а не пересобирает её «по мотивам».
Production deck строится офлайн из текущего snapshot в
`buildHomeHeroTalkDeck()`: только `active` события, которые ещё идут или
начнутся не раньше `getCurrentDate()`, максимум один member взаимной explicit
occurrence family и только существующая редакционная запись. Исторический
engagement не возвращает прошлое, отсутствующая или просроченная запись
fail-closed исключается.

Launch-bank `homeHeroTalkEditorial.ts` содержит 28 подготовленных коротких
реплик как смысловые fragments. Это не названия карточек: только отмеченный
фрагмент является ссылкой на событие, остальные слова остаются обычным текстом.
Вся сцена ссылкой не является, вложенных ссылок нет. Seed включает дату
snapshot и immutable base path, поэтому один build получает стабильный порядок
без runtime provider-запросов.

На desktop hero — viewport-wide плоскость без карточной рамки, скругления и
тени. Copy выровнен по общему page shell, а photo mosaic имеет `16/18/20×5`
ячеек, ширину `75vw`, нулевой row-gap и доходит до правого края viewport.
Прямой `<img>` используется только как невидимый decoder. После загрузки
детерминированное directional/noise field оставляет большинство ячеек
частичными (`.03/.1/.24/.5/.78/.96`), защищает copy и face zones, допускает
лишь две крайние полностью проявленные колонки и никогда не превращается в
цельную фотографию. Upscale выше `1.10` отклоняет media, но не текст.

Цикл содержит до четырёх уникальных событий и форматы `text-only` /
`photo-mosaic`, автоматически останавливается вне viewport и в скрытой вкладке.
Play/Next/Replay/progress/Lab controls в product surface отсутствуют.
`prefers-reduced-motion` и no-JS оставляют первую полезную сцену и fragment-link
без автопереключения.

Целевой следующий этап — Smart Update authoring pipeline: bounded fact pack
актуальных событий → batch LLM generation смысловых fragments → deterministic
schema/fact/link validator → human/editorial gate → versioned static bank.
LLM не вызывается из браузера и не публикует текст без проверки. До этого
работает только проверенный launch-bank из 28 реплик.

## Mobile calendar feedback v23 (2026-07-21)

На странице `Сегодня` прошедший start без явного `end_at` нельзя называть
завершившимся и нельзя дополнять выдуманной длительностью. Когда точное время
начала уже прошло, row сохраняет semantic state `is-started`, получает
фактическую подпись `Уже началось` и тот же принятый neutral/desaturated visual
treatment, что помогает отличить его от будущих событий. `all_day` и
`day_program` не обесцвечиваются. Начальный scroll marker ставится перед первым
будущим событием с точным временем и не использует программу дня как target.

Pinned mobile subheader сохраняет высоту `64px` и появляется атомарно вместе с
текстом/фоном. Его двухстрочная иерархия: `24 июля · 20 событий` и ниже город
`Вся область ⌄` / compact multiselect `Калининград +2 ⌄`. Дата — единственный
сильный текст (`18/18`), count и city — `10.5/12` muted; pill/border отсутствуют,
terracotta остаётся только у стрелки. Селекторы обязаны быть scoped: общий
`.sticky-date span` запрещён, потому что он превращал `20 событий` в две строки
и визуально смешивал count с city. Gates проверяют nowrap на `320/390px`, safe
lane справа от brand tag и совместное появление backdrop/content.

Calendar v23 и Astro Search публикуются разными noindex prefixes, но образуют
один mobile journey: calendar bottom-nav ведёт в проверенный Astro `/poisk/`, а
Search и materialized-query pages возвращают тот же четырёхпунктовый dock с
активным `Поиск`. На Search скрыт только старый горизонтальный desktop nav,
который визуально попадал под fixed brand top-sheet; сам top-sheet и Search
donor не перестраиваются. Preview base URLs задаются публичными build-time env,
поэтому в source нет зашитого versioned prefix. Dock и mobile drawer используют
один resolver; v24 acceptance проверяет, что calendar/personal links ведут в
принятый v23 prefix, а Search — обратно в текущий v24 prefix.
