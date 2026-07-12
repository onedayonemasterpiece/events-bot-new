# Подготовка публичного релиза статического сайта персональных анонсов

> Дата аудита: **2026-07-11**
>
> Актуализация product scope: **2026-07-12** — D-1 event reminders, one-time localStorage email, Yandex/manual-email choice, saved-search public tags, metric-backed personalization E2E и экологичный Supabase 500 MB capacity/compaction gate.
>
> Базовая ревизия: `origin/main@323cb1e407c6`
>
> Post-audit integration milestone: `origin/main@c6396331` (2026-07-12) contains the transactional Postbox worker, authorized-key preamble fix and live worker evidence; source/fix/docs branches were merged and deleted from `origin`.
>
> Решение на дату аудита: **NO-GO для публичной презентации полного заявленного функционала**
>
> Назначение: единый release-readiness checklist. Он связывает канонические feature/operations/incident-документы, но не заменяет их.

## 1. Как читать статусы

- **Done** — код находится в `origin/main`, есть автоматические проверки и актуальное production/canary evidence.
- **Partial** — есть рабочий slice, preview, code-only path или неполное production evidence.
- **Designed** — требования/архитектура описаны, пользовательский production path не реализован.
- **Missing** — канонический feature surface или необходимый код не найден.
- **Blocked** — переход к следующей стадии небезопасен до закрытия указанного gate.

Важно различать четыре слоя доказательств:

1. `origin/main` — единственная steady-state база для production;
2. feature/integration branches — незавершённая работа, не release truth;
3. опубликованный preview — canary evidence, но не доказательство production automation;
4. production state/logs/DB — подтверждение того, что путь реально включён и обслуживается.

Текущий основной checkout аудита (`docs/unsigned-personalization-review-20260625@12ad425e`) был на **724 commits позади `origin/main`**, содержал большой объём незакоммиченной работы и не может использоваться как release candidate. Этот документ создан в отдельном чистом worktree от `origin/main`.

## 2. Краткий вывод

### Что уже является хорошей основой

- Astro SSG, event pages, listings, JSON-LD, sitemap, robots, ICS и preview deployment существуют: [Static Site Event Pages](../features/static-site-pages/README.md), [Astro preview/runbook](../features/static-site-pages/astro-preview.md).
- Kaggle builder, immutable-input/status-ledger pattern и coalesced `+15 min` outbox contract реализованы на уровне кода/preview: [Kaggle static-site builder](../operations/kaggle-static-site-builder.md).
- Supabase pgvector sidecar и semantic related/search pipeline имеют canary evidence и rollback на sparse manifests: [Semantic vector retrieval](../features/unsigned-personalization/semantic-vector-retrieval.md), [Authorized event search](../features/unsigned-personalization/authorized-event-search.md).
- Статическая страница остаётся полезной без JS, а недоступность personalization/telemetry не должна ломать CTA и навигацию: [Production integration](../features/unsigned-personalization/production-integration.md).
- Incident management и regression contracts для качества событий развиты значительно лучше среднего: [Incident management](../operations/incident-management.md), [incident index](incidents/README.md).
- Transactional email now has a production-safe disabled control plane, live authenticated Postbox feedback consumer and a deployed transactional-only Fly worker/monitor with one controlled delivered canary; application producers and user flows remain gated: [Email delivery](../operations/email-delivery.md).

### Почему сейчас NO-GO

1. Production Smart Update → Kaggle → checked artifact → atomic CDN promotion **не включён и не замкнут**.
2. В production отсутствуют env-флаги static builder/vector-related handoff; на момент read-only проверки не было ни одного `static_site_build` в outbox/ledger.
3. В coalesce-коде есть риск потери обновления во время долгого static build: running follow-up не создаётся для `static_site_build`, а общий stale threshold `600s` конфликтует с разрешённым runtime до `5400s`.
4. Публичный root всё ещё `noindex`; production `/poisk/` не опубликован. Последний полный preview отстаёт от текущего каталога.
5. Значительная часть заявленного функционала находится только в side branches либо остаётся design-only: персональное письмо/страница, transport refresh, comment feedback, admin→ArtKodex, durable favorites, verified-email identity, personalization merge.
6. Smart Update остаётся владельцем предотвращения дублей и ошибок фактов, но перед релизом ещё не формализован регулярный контроль его результата: cadence аудитов, incident-rate trend/SLO, обязательное заведение инцидентов и подтверждённое закрытие root causes до устойчиво низкого уровня дефектов.
7. Локальная release-проверка текущего WIP 2026-07-11: Astro собрал `110` страниц, но `npm run check:preview` упал из-за рассинхронизации UI (`Смотрите дальше`) и contract assertion (`Вам могут быть интересны`). Это не дефект `origin/main`, но доказательство, что активный WIP не является чистым RC.

## 3. Матрица требований

| ID | Требование | Статус | Текущее доказательство | Главный release gate |
|---|---|---|---|---|
| **G1** | Публичная надёжность и доступность | **Partial / Blocked** | Static-first fallback, CDN и rollback protocol описаны; root/preview/CDN отвечают `200` | Atomic promotion, last-good rollback, monitoring/SLO, clean `origin/main` RC, security/a11y/load/real-device evidence |
| **G2** | Сокращены инциденты: дубли, локации, даты/время | **Partial / Blocked** | Bounded audits 305/305 и 308/308, repairs и regression incidents | Smart Update root-cause prevention; регулярный аудит новых/изменённых и всего active/future inventory; incident burn-down/trend/SLO; closure-grade replay для повторяющихся классов дефектов |
| **F1** | Smart Update effect → rebuild через 15 минут | **Partial / Blocked** | Код coalesced `static_site_build` и Kaggle runner существует | Включить prod env; исправить running/deferred/stale semantics; atomic CDN promotion; prove two updates during one long build |
| **F2** | Качественные похожие события через vector search | **Partial** | pgvector `gemini-embedding-2/vector(768)`, v48 canary, sparse rollback | 95%+ current coverage, golden/hard-negative editorial gate, whole-catalog recompute, production static integration |
| **F3** | Умный поиск + сохранение результата как публичного тега | **Partial** | Search UI/Edge source/canary и private feedback/tag-candidate intake существуют | Production `/poisk/`; explicit save; normalization/idempotency/result-novelty curation; current-catalog static tag generation; public anonymous tag E2E |
| **F4** | Email: 3 предложения + персональная static page | **Designed** | Canonical design v2 добавлен в release-doc branch; прежняя YDB-owned docs branch superseded | Subscription/double opt-in; issue/page generator; outbox; token security; canary/live delivery |
| **F5** | UI отработан и зафиксирован | **Partial** | Большой preview/check contract; отдельная UX V3 branch активна | Design freeze + owner sign-off; visual baselines 375/768/1366; a11y/keyboard/reduced-motion/real devices; no failing RC assertions |
| **F6** | Views/list/detail/social-action personalization telemetry and application | **Partial** | Local profile/actions/served-list Playwright contract exists; full DB integration/application E2E missing | Detailed Gherkin/Playwright: localStorage → accepted/deduped DB rows → profile rollup → next feed; golden personas and `cards_to_first_relevant <=20` |
| **F7** | На выбор Yandex identity/email или вручную введённый verified email | **Partial** | Yandex PKCE login/logout работает; manual email path описан | Yandex email sync/fallback; one-use email code/link with TTL/replay/rate limits; both paths converge without duplicate identity/profile; real-device proof |
| **F8** | Sender subdomains, D-1 event reminder, bounce/complaint handling | **Partial / live worker foundation** | `main@c6396331`: disabled Supabase control plane, authenticated Postbox feedback/suppression, live transactional-only worker+monitor canary, authorized-key fix | Event-specific calendar/reminder producers; templates/UX; D-1 schedule/reschedule E2E; cross-provider placement warm-up; NotiSend recommendation flow/key gate |
| **F9** | Избранное пользователя | **Missing** | Local `liked_event_ids` — это не durable favorites | Define favorite semantics; DB/RLS/API/page; cross-device/merge/lifecycle/delete/export |
| **F10** | Yandex login/logout + merge personalization | **Partial** | Login/logout реализованы; merge описан только архитектурно | Explicit consent, idempotent merge API/schema, conflict/logout/unlink/delete policy and E2E |
| **F11** | Rail/bus schedules, daily Kaggle refresh, transport card/favorite | **Partial, branch-only** | Rail Светлогорск/Зеленоградск + один bus example + leg ICS в integration branch | Merge; full city/provider matrix; nightly validated atomic last-good refresh; stale alert; real browser/ICS; persistent favorite if required |
| **F12** | Add to calendar = favorite + видимый статус D-1 email reminder | **Partial** | Stable `.ics` работает; favorite mutation/reminder UX отсутствуют | Atomic/idempotent ICS+favorite; masked reminder status or inline email/consent choice; undo/cross-device/reschedule/cancel tests |
| **F13** | Site events do not become stale vs bot/core DB | **Partial / Blocked** | Candidate freshness filters and rebuild design exist | Production rebuild/promotion loop, two-update race fix, catalog parity manifest, max-staleness alert and SLO |
| **F14** | Comment-derived event feedback on page | **Partial research, branch-only** | Offline Kaggle probe/strict gates and static manifest design in stale branch | Rebase; YDB incremental collector; optional verifier; production manifest/Astro UI; PII/safety/canary evidence |
| **F15** | Share generates image | **Partial** | Preview Web Share file → generated `1080×1350` canvas → text/copy fallback | Stable offline/server assets 1200×630, 1080×1350, 1080×1080; stale regeneration; CORS; Telegram/VK/MAX real-device tests |
| **F16** | Correct image focus/crop | **Partial** | Renderer accepts focal/face metadata and keeps OCR-safe contain fallback | Producer currently emits empty focal/face metadata; implement enrichment, confidence/manual override, golden visual corpus |
| **F17** | Admin issue report → ArtKodex repair/history | **Partial, branch-only** | Admin Edge/UI/history design and branch implementation exist | Merge; unique active/idempotency key; atomic poller claim; real ArtKodex owner; structured repair result; end-to-end repair/rebuild/history |

**Итог:** для полного заявленного публичного релиза нет ни одного требования, которое можно честно отметить `Done` по строгому определению `main + tests + current production evidence`. Это не означает, что продукт начинается с нуля: сильные vertical slices уже есть, но release integration и эксплуатационные доказательства отстают от объёма реализации.

## 4. Текущее production/canary evidence

Read-only срез на 2026-07-11:

- `ENABLE_STATIC_SITE_KAGGLE_BUILDER`, `STATIC_SITE_RELATED_MODE`, `STATIC_SITE_SYNC_PGVECTOR_VECTORS`, `STATIC_SITE_GEMMA_RELATED_VERIFY` в production отсутствовали;
- `joboutbox`: `0` static-site jobs; `kaggle_run_ledger`: `0` static-site runs;
- vector sidecar отдельно работал: `ops_run=3591`, success, `343` documents, `search_v3` и `related_v1`, `complete=true`, `2` embeddings updated, `684` unchanged, no cap remainder;
- `https://kenigevents.ru/` отвечал `200`, но оставался `noindex,nofollow`;
- root `/poisk/` — `404`; preview `/poisk/` — `200`; Edge preflight — `200`, unauthenticated POST — `401`;
- v48 preview и stable ICS отвечали `200`; sample referenced CDN WebP отвечал `200`;
- последний документированный full preview был сгенерирован 2026-07-02 и содержал max event id `6613`; текущий prod catalog имел max id `6828`, причём `126` active ids были новее `6613`.
- core `/healthz` был ready, SQLite `quick_check=ok`, но свободное место `/data` составляло только около `256 MiB`; root overlay имел около `7.87 GiB`.

Вывод: vector sidecar имеет актуальную operational основу, но статический публичный surface не синхронизирован с ней автоматически.

Post-audit email evidence on 2026-07-12 (`origin/main@c6396331`):

- `feature/email-postbox-worker`, `fix/postbox-authorized-key-preamble` and `docs/postbox-worker-live-evidence` were merged through PRs #34–#36 and removed from `origin`;
- the transactional-only Fly outbox worker and PII-free monitor are deployed; controlled send stored a real Postbox MessageId and reached `delivered` through authenticated/verified `Send`/`Delivery` events;
- replay/dedupe, scoped suppression, alerting, private DLQ failure/replay/cleanup, automatic trigger probe and destination rollback have live evidence;
- the Yandex authorized-key parser now accepts only a matching key-id warning preamble and strips it before PS256 signing;
- database send switches remain off/dry-run-only; event-specific producers, calendar/reminder UX/templates and cross-provider placement warm-up are still release work.

Ключевые code anchors в `origin/main@323cb1e4`: effect-only Smart Update fanout — `smart_event_update.py:15940-15946,17364-17382`; `+15 min` schedule — `main.py:15400-15408`; running/coalesce semantics — `main.py:14132-14220`; static max runtime — `main.py:17042-17053`; Kaggle command assembly — `main.py:21547-21625`; vector projection owner — `event_vector_sync.py:77-198`; public projection filters — `site/scripts/export-production-preview-data.py:303-319,786-905`; authenticated search — `supabase/functions/event-search/index.ts`.

## 5. Канонические документы и незамерженные feature homes

### В `origin/main`

- [Static site](../features/static-site-pages/README.md)
- [Astro preview and build evidence](../features/static-site-pages/astro-preview.md)
- [Static builder operations](../operations/kaggle-static-site-builder.md)
- [Release governance](../operations/release-governance.md)
- [Unsigned personalization](../features/unsigned-personalization/README.md)
- [Production integration gates](../features/unsigned-personalization/production-integration.md)
- [Related recommendations](../features/unsigned-personalization/event-detail-related.md)
- [Semantic/vector retrieval](../features/unsigned-personalization/semantic-vector-retrieval.md)
- [Authorized search](../features/unsigned-personalization/authorized-event-search.md)
- [Listing personal feed](../features/static-site-pages/listing-personal-feed.md)
- [Reaction counters](../features/static-site-pages/reaction-counters.md)
- [CDN delivery](../features/static-site-pages/cdn-asset-delivery.md)
- [Incident index](incidents/README.md)

### Только в side branches на дату аудита

Эти пути нельзя считать каноническими, пока соответствующая ветка не перебазирована, проверена и не слита в `origin/main`:

| Surface | Branch snapshot | Документ/код | Интеграционный риск |
|---|---|---|---|
| F4 personal email/page | `origin/agent/personal-email-announcements-docs` (21 behind / 6 ahead) | прежний `docs/features/personal-email-announcements/README.md` | Superseded ownership design; port to a new main-based v2 branch only |
| F8 historical transactional prototype | `origin/feature/event-email-notifications-static-20260702` (677 behind at audit time) | old `email_notifications/` prototype | Superseded. Current feedback/worker foundation was reimplemented and merged in `main@c6396331`; do not revive this branch |
| F5 UI V3 | `origin/feature/event-page-ux-lab-v3-20260710` (69 behind / 15 ahead) | event-page decision/onboarding lab | UI not frozen; requires clean rebase and visual acceptance |
| F11 transport | `origin/integration/event-transport-schedule` (1 behind / 9 ahead) | `docs/features/static-site-pages/event-transport-schedule.md` | Closest to merge; nightly refresh still P0-open |
| F14 comment feedback | `origin/agent/event-comment-feedback-kaggle-runner` (724 behind / 10 ahead) | `docs/features/event-comment-feedback/README.md`, runner/kernel/tests | Architecture/code may be stale; no public UI/YDB path |
| F17 admin issue reporting | `origin/feature/event-issue-report-artkodex-20260703` (69 behind / 12 ahead) | `docs/features/event-issue-reporting/README.md`, UI/Edge/migration | Mixed branch; no DB-level double-start guard/poller proof |

### Есть ли отдельная корректная feature branch для каждого требования

Нет. Requirement ID — это release capability, а не всегда отдельная ветка. Текущая карта:

| IDs | Состояние ветки/документации |
|---|---|
| F1, F2, F3, F13 | Основные slices уже находятся в `origin/main` внутри parent features `static-site-pages` и `unsigned-personalization`; отдельная незамерженная feature branch не требуется, но production integration остаётся незакрытой задачей. |
| F4 | Canonical v2 home/route подготовлены в release-doc branch; реализация требует новой main-based feature branch. Старая YDB-owned docs branch superseded. |
| F5 | Есть активная UX V3 branch, но UI не frozen и branch отстаёт от main. |
| F6, F7, F10 | Parent personalization docs и новый canonical `site-user-identity` home подготовлены; отдельных завершённых implementation branches для remote telemetry, verified-email identity и profile merge нет. |
| F8 | Current transactional Postbox feedback/worker foundation is in `main@c6396331`; the old prototype remains superseded. Application event producers, reminder UX/templates/warm-up and the separate NotiSend recommendation flow are still incomplete. |
| F9 | Canonical `event-favorites-calendar` home подготовлен; implementation branch/schema/API всё ещё отсутствуют. |
| F11 | Есть близкая к main integration branch, но nightly production refresh остаётся P0-open. |
| F12 | ICS находится в main; отдельной branch для product contract “calendar = favorite” нет. |
| F14 | Есть старая probe branch; production collection/YDB/Astro feature не оформлены как merge-ready implementation. |
| F15 | Preview implementation находится в main, production share-asset generator остаётся debt без отдельной завершённой ветки. |
| F16 | Renderer contract находится в main; producer focal/face metadata и его feature branch не найдены. |
| F17 | Есть side branch, но она mixed/stale и не закрывает idempotent ArtKodex poller/result E2E. |

Следовательно, документацию нельзя считать полностью и корректно оформленной по всем F1–F17. Корректная цель — один canonical feature home на capability/связную feature family, актуальный status matrix, routes, operations/tests и явная связь с implementation branch; создавать искусственную ветку на каждый ID необязательно.

## 6. Аудит полноты документации

| Область | Полнота | Проблема консолидации | Требуемое действие |
|---|---|---|---|
| Static pages/build/CDN | Высокая | `README.md` смешивает current contract, preview diary и historical decisions | Оставить current contract + gates; version diary вынести в reports |
| Vector related/search | Высокая | `routes.yml`/README statuses не отражают capability-by-capability stage | Ввести status matrix: infra / canary / prod / operations |
| Anonymous personalization | Высокая design, низкая prod | README одновременно говорит anonymous-only и содержит authorized search | Разделить identity/search/telemetry/feed, сохранить один parent index |
| Personal email/page | Canonical design v2 подготовлен | Реализация отсутствует; старая branch назначала неверного владельца | Новый main-based feature branch по принятому Supabase control-plane ADR |
| Email delivery | Средняя, branch-only | Transactional follow и recommendation digest смешиваются концептуально | Разделить transactional/reminder и recommendation marketing streams |
| UI/share/focus | Средняя/высокая | Решения разбросаны по длинным preview/UI review docs | Зафиксировать единственный release UI contract + visual baselines |
| Transport | Хорошая branch spec | Нет main route; coverage намного уже требования | Merge как отдельную feature; city/provider/source matrix и ops runbook |
| Comment feedback | Сильная design/probe | Старая ветка; нет main feature home/public implementation | Rebase architecture, отдельно storage/probe/UI/operations stages |
| Admin incident report | Хорошая product prose | Документ переоценивает готовность ArtKodex poller/idempotency | Явно разделить UI, DB queue, poller, repair result, E2E statuses |
| Event quality / Smart Update | Сильные incident records | `routes.yml` не перечисляет свежие Jul-7/9/10/11 contracts; нет единого cadence/SLO/dashboard и release burn-down | Добавить routes; закрепить Smart Update как prevention owner; описать регулярный audit → incident → root-cause fix → replay → monitoring workflow |

Дополнительные найденные дефекты документации/contract hygiene:

- в `origin/main` отсутствует единый release checklist — этот документ закрывает навигационный пробел;
- свежие event-quality incidents присутствуют в incident index, но отсутствуют в `docs/routes.yml`;
- side-branch features не должны попадать в main feature index до merge, но release plan обязан явно показывать их статус;
- названия/пути `alanytics.md`, `requitements.md`, `trip-recomendation` являются legacy typo debt; переименование допустимо только через redirect stubs;
- часть старых review docs содержит вердикты “not canary-ready”, хотя pgvector canary уже выполнен; review artifact нельзя использовать как current status без application matrix;
- current WIP имел один broken relative link в `event-token-medallions.md` и рассинхрон UI/check-preview assertion; исправлять нужно в соответствующих feature tasks, не в release-plan ветке.
- parent static-site README всё ещё одновременно говорит “preview/auth out of scope”, описывает старый v44 как current и называет регулярный DB export следующим шагом; current status нужно сверить с Jul-02 build и реальным `origin/main` code path;
- operations doc обещает follow-up build при update во время running build, но `main.py` не реализует это для `static_site_build` — документацию нельзя считать доказательством поведения;
- `event_active_where`/public projection tests требуют отдельной проверки поля `silent`: текущий exporter/tests не дают уверенности, что silent rows не попадут в публичный static surface;
- `INC-2026-07-08-prod-root-overlay-disk-full.md` найден только в старом dirty checkout, но отсутствует в `origin/main`; regression contract недолговечен, пока record не интегрирован канонически.

## 7. Обязательные release stages

Каждая стадия выполняется отдельными feature/task branches. Следующая стадия не делает предыдущую необязательной.

### Stage 0 — Scope freeze и интеграционная база

- [x] Утверждён полный F1–F17 launch scope; staged canaries не исключают ни одной фичи из первой публичной презентации.
- [ ] Создать integration branch только от свежего `origin/main`.
- [ ] Для каждой side branch записать owner, base/head SHA, rebase plan, tests, merge/reject decision.
- [ ] Не переносить весь dirty checkout; cherry-pick/re-implement только проверяемые feature commits.
- [ ] Зафиксировать UI release version и data/schema versions.
- [ ] Закрыть продуктовые решения из раздела 10.

### Stage 1 — Стабилизация качества Smart Update и incident burn-down

Smart Update является владельцем семантического предотвращения дублей, неверных локаций, дат и времени. Предрелизный контур не должен переносить эту ответственность в static exporter или отдельный широкоформатный regex/keyword gate.

- [ ] Утвердить cadence: ежедневный аудит всех новых/изменённых событий и регулярный полный аудит active/future inventory до релиза.
- [ ] Для каждого подтверждённого дефекта создавать/обновлять incident record, классифицировать failure family и фиксировать затронутые публичные поверхности.
- [ ] Для повторяемого класса находить root cause в extractor/import/Smart Update match-merge/writer/reference layer, а не ограничиваться repair строки или страницы.
- [ ] Каждый root-cause fix проходит closure-grade replay через реальный production import boundary и `smart_event_update.py` на snapshot/shadow DB, затем проверяется на Telegram/VK/Telegraph/static/ICS surfaces.
- [ ] Прогнать обязательный regression pack: duplicate doors/start, two vendors, venue aliases, prose/person venue, default-venue offsite, compact/hashtag dates, recurring occurrence/season, exhibition duplicates, valid multi-session no-merge.
- [ ] На release cutoff выполнить полный аудит exact active/future catalog и устранить все подтверждённые high-impact дефекты.
- [ ] `silent`, merged/review/cancelled/inactive и структурно невалидные rows fail closed across listings, sitemap, related, search snapshots and ICS; это узкий projection safety net, а не замена Smart Update semantics.
- [ ] Вести dashboard/trend: абсолютное число и rate дублей, wrong-location и wrong-date-time по import batch/day; отдельно new incidents, reopened incidents и root causes closed with replay.
- [ ] Перед GO показать **14 последовательных дней** с `0` новых critical event-quality defects и `0` recurrence/reopen по root causes, объявленным закрытыми. Любое нарушение перезапускает окно после repair + closure-grade replay; lower-severity baseline не может скрыть critical/repeat средним значением.

Обязательные regression contracts:

- [Future event semantic audit](incidents/INC-2026-07-10-future-event-semantic-audit.md)
- [Zoo validity non-event](incidents/INC-2026-07-10-zoo-ticket-validity-non-event.md)
- [Recurring occurrence date drift](incidents/INC-2026-07-09-recurring-occurrence-date-drift.md)
- [New event quality degradation](incidents/INC-2026-07-07-new-event-quality-degradation.md)
- [Active duplicate recall gate](incidents/INC-2026-05-30-active-duplicate-events-recall-gate.md)
- [Exhibition duplicates surfaced by static site](incidents/INC-2026-07-02-exhibition-duplicates-static-site.md)
- [TG prose location](incidents/INC-2026-06-18-tg-location-prose-still-extracted.md)
- [Future date/default venue](incidents/INC-2026-06-24-future-event-date-default-venue-regressions.md)
- [Vector sidecar stalled](incidents/INC-2026-07-11-event-vector-sidecar-sync-stalled.md)

### Stage 2 — Production static build/publish platform

- [ ] Исправить `static_site_build` coalesce: deferred follow-up while running и feature-specific stale/runtime semantics.
- [ ] Test: update A starts build; update B during build; B guarantees exactly one later build and publishes newer snapshot.
- [ ] Включить production env only after clean deploy.
- [ ] Immutable SQLite snapshot → unique Kaggle input → status heartbeat → checked artifact.
- [ ] Release manifest содержит snapshot hash/max event update, page/event counts, quality/freshness result, asset hashes.
- [ ] Гарантировать CDN delivery всего публичного статического сайта: canonical HTML/JSON/sitemap/robots, Astro assets, event media и ICS отдаются через CDN-backed hosts; generated HTML не содержит прямых Object Storage/source-CDN URL, а release smoke подтверждает CDN response/cache headers на canonical URL.
- [ ] Release asset audit подтверждает `0` runtime PNG/JPEG/GIF и других тяжёлых raster-исключений: вся растровая графика конвертирована в оптимизированный WebP и проходит утверждённые byte/dimension budgets, а векторная графика поставляется как безопасный SVG без embedded raster. Нарушение блокирует promotion.
- [ ] Upload только в unique staging prefix.
- [ ] Atomic promotion marker/pointer; failed build never alters current release.
- [ ] Last-good rollback tested; retention and cleanup tested.
- [ ] Catalog parity check: every eligible core event has expected HTML/JSON/ICS and no ineligible event leaks.
- [ ] Max-staleness and missed-build alerts; operator runbook and catch-up command.
- [ ] Capacity/retention guard for `/data`, `/tmp`, downloaded Kaggle outputs and retained release trees; low disk blocks a new build before `ENOSPC`.

### Stage 3 — Related/search readiness

- [ ] Active/future vector coverage target `>=95%`, one model/dimension per build.
- [ ] Whole active/future related graph recomputed after any changed/new event.
- [ ] Golden anchors + hard negatives pass editorial review; no popularity boost corrupts pure-related slots.
- [ ] Product contract explicitly decides whether vector-only candidates may be shown under neutral `Смотрите дальше` or every `Похожие` candidate requires verifier evidence; UI label and manifest metadata match the decision.
- [ ] Fallback sparse manifest is last-good, explicit and not labelled semantic.
- [ ] Production `/poisk/` published; Yandex OAuth/Edge env enabled.
- [ ] Latest mobile browser E2E: login → callback → quota → search → result cards → fallback/logout.
- [ ] Quota, latency, provider error, fallback and storage alerts visible.
- [ ] После полезной выдачи авторизованный пользователь может идемпотентно выбрать `Сохранить как тег`; сохраняются normalized intent и immutable served-list evidence, но не публикуется identity автора.
- [ ] Curation сравнивает candidate со всеми pending/accepted tags по normalized hash, semantic intent и top-K result-set overlap; эквивалентный запрос привязывается к существующему canonical tag.
- [ ] Новый публичный тег принимается только при доказанной заметной новизне выдачи на утверждённом multi-user golden pack; числовые overlap/novelty thresholds и минимальный объём качественных результатов зафиксированы до implementation acceptance.
- [ ] Каждый static build пересчитывает accepted tag по текущему каталогу, публикует canonical anonymous HTML/JSON + manifest/fingerprint, suppresses empty/stale tags и не вызывает embedding/LLM при обычном просмотре страницы.
- [ ] Tag curation полностью автоматическая: strict multi-pass offline LLM normalization/audit + deterministic overlap/safety/eligibility gates; `accept|merge|reject` исполняются без human queue, а disagreement/low-confidence/provider/schema failure остаётся private `pending` и fail-closed автоматически retry later.

### Stage 4 — Identity, telemetry, favorites, calendar

- [x] Выбран один identity/profile ownership contract: personalization Supabase/Postgres; secrets stay server-side. См. ADR.
- [ ] No consent = no local profile mutation and no trusted remote telemetry.
- [ ] Remote telemetry write path passes RLS/grant/body/rate/dedupe/bot/retention tests.
- [ ] Valid impressions, detail views, dwell, card click, ticket/calendar/share/like/hide contain surface/rank/layout context.
- [ ] Verified email supports both one-time code and one-click link for the same identity/transaction, with TTL and replay/attempt limits.
- [ ] На всех email-dependent surfaces есть одинаковый выбор: Yandex login/email или ручной ввод почты; Yandex без usable email переводит в manual verification, не ломая identity и не создавая второй профиль.
- [ ] Manual-email path writes/reuses versioned `ke_contact_email_v1`: one entry per browser, pending/verified state survives reload and later saves, explicit `Забыть почту` works, and local cache never substitutes for server verification/consent.
- [ ] Anonymous→authorized merge explicit, idempotent, auditable, reversible; logout behavior defined.
- [ ] Favorite, like and calendar semantics no longer conflated.
- [ ] Add-to-calendar/favorite is atomic/idempotent; repeat/undo/cross-device/lifecycle cases pass.
- [ ] После save UI показывает либо `Напомним за день на a***@domain`, либо inline action для email verification/transactional consent; при старте менее чем через 24 часа обещание D-1 не показывается.

### Stage 4A — Supabase capacity and ecological storage

- [ ] Канонический [500 MB storage/compaction contract](../operations/personalization-storage-budget.md) реализован; current provider limit перепроверен перед canary.
- [ ] Launch baseline находится в Green (`<60%` verified plan limit) и имеет forecast headroom through canary/hypercare; измеряются таблицы **и индексы**, bytes/user-day и projected days to Orange.
- [ ] Raw weak telemetry по умолчанию выключена; browser/session compacts signals, Supabase хранит current state + bounded evidence, YDB получает только asynchronous de-identified TTL analytics, artifacts идут в Object Storage/CDN.
- [ ] Retention/fold/compaction jobs идемпотентны, observable и проверены на restorable snapshot; stale vector/event/tag versions и debug/provider payloads не растут бесконечно.
- [ ] Yellow/Orange/Red/Critical alerts и admission/kill switches доказаны: disposable telemetry/debug прекращаются раньше, чем становятся недоступны consent withdrawal, unsubscribe/suppression, favorite/reminder cancellation и send guards.
- [ ] Synthetic volume model проверяет launch cohort, 1k и 10k active-user scenarios по утверждённому retention horizon; upgrade/architecture decision принимается до Red, не во время outage.
- [ ] Product/legal утвердили retention для profiles/actions/served lists/aggregates/consent/suppressions; storage pressure не сокращает send-critical evidence самовольно.

### Stage 5 — Email recommendations, event reminders and deliverability

- [x] Transactional-only Postbox worker/monitor, authenticated feedback correlation, suppression, bounded retry/ambiguity quarantine and private DLQ control are merged and live-verified in `main@c6396331`.
- [x] Yandex authorized-key warning preamble is accepted only when its key id matches the JSON key id, then stripped before PS256 signing.
- [ ] Personal issue schema, profile snapshot, deterministic hero+3 selection and 12–24 recommendation page.
- [ ] Personal page is published before email enqueue; forwardable public secret URL works without login, token is high-entropy/hash-only/revocable, page is `noindex` and outbound navigation cannot leak the token.
- [ ] Outbox idempotency/fatigue/quiet-hours/stale/cancelled/rescheduled gates pass.
- [ ] Recommendation and transactional sender streams/subdomains separated.
- [ ] SPF/DKIM/DMARC alignment verified; bounce/complaint webhooks signed/deduped.
- [ ] Hard bounce/complaint/unsubscribe suppression blocks future sends.
- [ ] Warm-up, per-domain limits, delivery dashboard and kill switch exist.
- [ ] Canary list only; no broad send before live evidence review.
- [ ] D-1 scheduler derives `send_at` from the current canonical start/timezone, revalidates event + verified identity + transactional consent + suppression before claim and emits at most one reminder per user/event/start-version.
- [ ] Save retry, undo, event merge, cancellation, reschedule, late save (<24h), scheduler restart and provider retry cannot duplicate or misdirect a reminder; calendar/ICS remains usable when mail is unavailable.
- [ ] Real Postbox seed E2E proves calendar save → visible masked promise → scheduled `event_reminder_24h` → provider message id/event, plus opt-out/suppression/kill-switch rollback.

### Stage 5A — Personalization E2E and quality metrics

- [ ] Канонический [personalization E2E/KPI contract](../features/unsigned-personalization/e2e-acceptance.md) реализован как traceable Gherkin scenario IDs + Playwright tests, а не только как документ.
- [ ] Deterministic contract suite проверяет consent/no-consent, localStorage schema/migration/corruption, valid impressions/actions, dedupe, hidden state, email cache, static fallback и DB/network failure.
- [ ] Integration suite запускает реальный browser flow через изолированный Supabase namespace/project; service-side fixture отдельно проверяет accepted rows, dedupe, profile snapshots и cleanup, не передавая secret key в browser.
- [ ] Один correlated run доказывает четыре стадии: сигнал появился в localStorage → принят в БД → изменил ожидаемый профиль → применён к следующей выдаче.
- [ ] Golden-persona pack включает «Чайковский», cold/mature, positive/negative interests, mobile/desktop, no-relevant-supply и catalog refresh; ground truth строится по canonical event features, не по broad title-regex.
- [ ] Для каждого eligible mature golden scenario `cards_to_first_relevant <= 20`; mature fixture и правила valid impression/meaningful action/versioning зафиксированы в evidence.
- [ ] Dashboard/artefact считает collection success, duplicate/drop reasons, profile rollup lag, profile-to-feed application, MRR/precision@20, relevant supply, top-20 hide/skip/diversity/fallback guardrails.
- [ ] Near-cap E2E `PERS-STORAGE-001` доказывает, что nonessential telemetry shed не создаёт retry storm и не блокирует durable user-control/email-safety state.

### Stage 6 — Transport, discussion signals, admin repair, media

- [ ] Transport city/provider matrix approved; rail/bus sources/licensing/refresh SLAs recorded.
- [ ] Nightly Kaggle transport refresh uses lease/status/validator/bounded diff/atomic last-good/stale alert and one coalesced static rebuild.
- [ ] Comment feedback: incremental YDB state, PII redaction, phrase-bank/vector-first gates, cached group verifier, static manifest, Astro fallback and manual canary.
- [ ] Admin report: allowlisted auth, event snapshot, idempotency key/unique active constraint, atomic poller claim, crash/retry safety, structured repair result/history/re-report.
- [ ] Offline focal/face/saliency enrichment with confidence/fallback/manual override and golden crop corpus.
- [ ] Stable share assets and real Telegram/VK/MAX tests.

### Stage 7 — Release candidate, canary и public launch

- [ ] Clean `origin/main`-reachable RC SHA; no release fixes only in side branches.
- [ ] Full site build/check from clean checkout passes.
- [ ] Playwright/mobile/desktop visual baselines, keyboard/a11y/reduced-motion, no-JS and slow/offline fallback pass.
- [ ] Security review: RLS/grants, auth callback, bearer tokens, admin allowlist, email webhooks, secret exposure, abuse limits.
- [ ] Performance/load: static/CDN, Edge search quota, telemetry ingest, promotion under full catalog size.
- [ ] Limited canary continues through the approved **14-day event-quality stability window** with availability/quality dashboards; GO requires zero critical defects and zero recurrences of closed root causes across the full window.
- [ ] Rollback drill and incident on-call/contact tree executed.
- [ ] Remove root `noindex` only after all launch blockers are signed off.
- [ ] Post-launch 72-hour hypercare, then 14-day review before declaring stable.

### Stage 8 — После публичной презентации

Эта стадия намеренно не является GO-блокером презентации и начинается только после её завершения.

- [ ] Перевести ежедневные анонсы в Telegram и VK на canonical ссылки соответствующих страниц статического сайта вместо прежних целевых страниц.
- [ ] До переключения подтвердить parity ссылок для всех eligible событий, корректные UTM/source attribution, доступность CDN и отсутствие утечки preview/personal secret URL.
- [ ] Выполнить раздельный canary для Telegram и VK, проверить опубликованные сообщения через реальные channel surfaces и сохранить быстрый rollback на предыдущий link target до окончания post-presentation hypercare.

## 8. Reliability objectives to approve

Это **предложения**, а не уже принятые SLO:

| Objective | Proposed launch target |
|---|---|
| Static page availability | `>=99.9%` monthly for canonical HTML/assets/ICS |
| Build scheduling | due time = last effective Smart Update + 15 min |
| Publication freshness | approve separate P95/P99 from due time to atomic promotion; do not call `+15 min` a publication SLA |
| Known critical catalog defects at cutover | `0` duplicate identities / wrong logistics / invalid dates in release inventory, plus 14-day window with zero new criticals and zero closed-root-cause recurrences |
| Catalog projection coverage | `100%` eligible events present; `0` ineligible/quarantined leaks |
| Search/vector current coverage | `>=95%` embeddings plus explicit fail-safe for uncovered rows |
| Rollback | last-good release restorable by documented operator action and drill |
| Email safety | `0` sends to suppressed/unverified/unsubscribed recipients |
| Admin issue idempotency | one active ArtKodex task per report/idempotency key |
| Mature-persona time to interest | every eligible golden scenario reaches the first relevant event within `<=20` validly inspected cards; production percentile target follows measured canary baseline |
| Personalization E2E integrity | one correlated run proves localStorage collection, exactly-once accepted DB evidence, expected profile change and application to the next served list |
| Supabase ecological capacity | verified limit ≈`500 MB`; launch `<60%`, disposable-write shedding from Orange, current control state remains writable through simulated near-cap test |

## 9. Release evidence pack

Каждый feature task перед integration должен приложить:

- [ ] base/head SHA and `origin/main` reachability;
- [ ] files changed and canonical docs updated;
- [ ] unit/contract/replay/E2E commands with terminal result;
- [ ] preview/canary URL and immutable build/release id;
- [ ] production config diff without secrets;
- [ ] DB migration/RLS/grant evidence when applicable;
- [ ] release manifest and catalog parity report;
- [ ] incident regression checklist and public-surface evidence;
- [ ] rollback command/result;
- [ ] for personalization: mapped Gherkin/Playwright results, redacted before/after localStorage, DB assertions/profile snapshot and `cards_to_first_relevant` calculation;
- [ ] for Supabase capacity: current plan/size, top table+index attribution, retention/compaction result, growth forecast and tested storage-band alerts/kill switch;
- [ ] remaining risks, owner and due date.

## 10. Открытые продуктовые и архитектурные решения

1. **Scope — решено:** все F1–F17 обязательны для первого публичного релиза/презентации; staged canaries only manage risk.
2. **Personalization storage — решено:** Supabase/Postgres owns current identity/profile/favorites/subscriptions/email control plane; YDB owns analytics/history and the independent comment-feedback sidecar. См. `docs/architecture/personalization-data-ownership.md`.
3. **Identity — решено на product level:** email-only user becomes a Supabase Auth identity through code or link; anonymous profile links automatically/intelligently under eligible personalization consent.
4. **Favorites — решено:** calendar save и favorite — один durable saved-event state; like остаётся отдельным сигналом; email reminder — отдельный explicit transactional opt-in.
5. **Email reminder — решено на product level:** один D-1 (`24h`) reminder для saved event при verified email + consent; остаются implementation decisions по quiet hours/catch-up и общая legal unsubscribe policy.
6. **UI freeze — owner решён:** финальный sign-off даёт project owner/user; execution task ещё должна выбрать точную branch/SHA + immutable preview baseline.
7. **Transport:** первый provider/city matrix, источник истины, лицензирование/кэширование и acceptable stale window.
8. **Discussion signals:** допустимые источники комментариев, retention/PII/moderation и правила показа negative/price signals.
9. **ArtKodex:** API/poller owner, task/thread contract, retry/idempotency and structured repair-result schema.
10. **Event-quality window — решено:** 14 consecutive days, `0` new critical defects, `0` recurrence/reopen of closed root causes; остаются availability/publication-freshness numerical SLO.
11. **Related semantics:** strict Gemma acceptance for “Похожие” vs pgvector-only neutral continuation; required precision@K/coverage/diversity thresholds.
12. **Public search-tag governance — решено:** strict fully automated LLM-first accept/merge/reject, no manual process, fail-closed private pending/retry. Implementation/eval task утверждает numerical semantic/result-overlap thresholds и minimum result count against the golden pack.
13. **Personalization production KPI:** golden release gate `<=20` принят; после canary утвердить production percentile/SLO и minimum relevant-supply coverage, не смешивая mature/cold-start/no-supply cohorts.
14. **Retention/ecological budget — требуется понятное product решение:** забывать/анонимизировать ли compact interest profile после `365d` без визита? Короткие technical logs всё равно удаляются раньше; consent/suppression/send-critical evidence живёт по отдельной safety/legal policy.

## 11. Следующие отдельные задачи в рекомендуемом порядке

1. **P0 — static release platform:** F1/F13/G1, coalesce race, atomic promotion/rollback/monitoring.
2. **P0 — Smart Update quality stabilization:** G2, регулярные аудиты, incident burn-down, root-cause fixes, closure-grade replay, dashboard/SLO.
3. **P0 — Supabase ecological capacity:** fresh size/budget, compact schema, retention/compaction, growth forecast, alerts and near-cap fail-safe before remote telemetry/email activation.
4. **P0 — vector/search/tag integration:** F2/F3, prod `/poisk/`, whole-catalog sync, save-as-tag curation/novelty/static generation и golden review.
5. **P0 — UI release freeze:** F5 plus clean preview contract and real-device/a11y evidence.
6. **P1 — identity/telemetry/favorites/calendar:** F6/F7/F9/F10/F12, включая Yandex/manual-email choice и видимый reminder state после save.
7. **P1 — email recommendations/reminders/deliverability:** F4/F8 after identity/consent foundation, включая D-1 scheduler/Postbox E2E, using the accepted storage ADR.
8. **P1 — admin repair loop:** F17 after idempotency/poller contract.
9. **P1 — media quality/share:** F15/F16.
10. **P2 — transport:** F11 after nightly source pipeline is production-safe.
11. **P2 — discussion signals:** F14 after rebase and safety evaluation.

Все одиннадцать streams остаются blockers. Staged canaries допустимы для управления риском, но `P2` и другие capabilities нельзя исключить из первого публичного release scope.

## 12. Closure checklist этого аудита

| ID | Результат аудита | Статус |
|---|---|---|
| G1 | Надёжность разложена на build/promotion/rollback/SLO/security/canary gates | **Done (planning only)** |
| G2 | Активные incident families, Smart Update ownership и регулярный audit/incident/root-cause/replay workflow перечислены | **Done (planning only)** |
| F1–F17 | Каждое исходное требование имеет отдельный статус, evidence class и gate | **Done (planning only)** |
| Документация | Main docs и side-branch feature homes разведены; gaps перечислены | **Done (planning only)** |
| Реализация | В этой задаче намеренно не менялась | **Not in scope** |
| Public release | Не разрешён данным аудитом | **Blocked / NO-GO** |
