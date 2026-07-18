# Подготовка публичного релиза статического сайта персональных анонсов

> **Восстановление контекста 2026-07-17.** Полная версия этого checklist была
> найдена в `origin/agent/static-release/checklist-cdn-social@fbb5b6a2` после
> аварийного обрыва предыдущей задачи и возвращена в main-based release branch.
> Базовый восстановленный реестр содержит **228 отдельных checkbox-требований**
> в Stage 0–11; пять D0–D10, семь video-guide и пять calendar/
> personal-hub строк добавлены при сверке, поэтому текущая версия
> Stage 0–12 содержит **245 checkbox-требований**;
> scope **F1–F18, H1, M1–M6** и не сводится к пяти задачам production-платформы.
> Источники и расхождения с текущим `origin/main` зафиксированы в
> [отчёте восстановления](static-site-release-context-recovery-2026-07-17.md).
>
> **Текущий срез сверки:** `origin/main@d1690043`, 2026-07-17. Исторические SHA,
> PR и production-probes ниже сохранены как evidence времени аудита; они не
> становятся текущим release truth без отдельной пометки.

## Обновление 2026-07-17 — что изменилось после исходного аудита

| Область | Текущий факт | Влияние на checklist |
|---|---|---|
| Desktop event template | `58abfb19` достиг `origin/main` | Реализация foundation есть; presentation checklist всё ещё требует frozen-RC regression evidence. |
| Event age rating | `aa95900a` достиг `origin/main` | Data pipeline больше не `Missing`, но M5 остаётся **Partial/Blocked** до 100% all-surface parity и accepted-value audit. |
| Social popularity | `fe211a88`, `d25b15d6`, `b34a97d3` достигли `origin/main` | Owned-channel aggregation реализован; M3 не закрыт, пока source+site projection и все consumers не сведены в один versioned contract. |
| Event media M2 | production cleanup после baseline `79/266` дала `0` projection/TG/VK/Telegraph duplicate violations; automatic gate в main | M2 всё ещё Partial: нужен repeat full-catalog audit на текущем snapshot, static canonical-surface evidence и полное 14-day observation window. |
| Interest clubs | `98180d1e`, `6b234a52` и evidence `6cdae545` достигли `origin/main`; production canary включён | Stage 11 research/implementation baseline частично superseded; обязательны семидневное наблюдение, freshness/false-merge/rollback closure и отдельное решение о stable release. |
| Full event-site publisher | `62ba7110` и последующее hardening остаются вне `origin/main` | Stage 2 production publish/promotion остаётся Blocked независимо от успешного preview. |
| Design-system catalog | candidate `128a2d6a` находится в side branch | Это полезный F5 candidate, но не main/release truth и не заменяет immutable RC sign-off. |
| Test inventory | 221 стабильных scenario IDs задокументированы | Автоматическая traceability пока нулевая: ID не встречаются в tests/scripts; это отдельный P0 release-evidence gap. |

Текущий общий verdict остаётся **NO-GO**. Ни один новый merged slice не отменяет
остальные требования реестра или 14-дневное quality window.

> Дата аудита: **2026-07-11**
>
> Актуализация product scope: **2026-07-12** — D-1 event reminders, one-time localStorage email, Yandex/manual-email choice, saved-search public tags, metric-backed personalization E2E и экологичный Supabase 500 MB capacity/compaction gate.
>
> Актуализация product scope: **2026-07-14** — F18: mobile menu/footer share of KenigEvents through one centrally prerendered metric-bound service card, desktop copy-link behavior, claim-evidence and CDN/real-device gates.
>
> Актуализация post-release scope: **2026-07-18** — Stage 12: видеогайды
> «Как быстро найти событие» для сайта «Полюбить Калининград Анонсы» после
> стабильного production UI и D10 cutover; production-SHA, subtitles, privacy,
> multi-target publication и freshness/withdrawal gates обязательны.
>
> F18 desktop clarification: **2026-07-14** — Windows/macOS rich clipboard (`image/png` + `text/html` + `text/plain`) remains an evidence-gated candidate; current baseline is text+URL copy until the native browser/target-app matrix and owner decision are complete.
>
> F18 implementation milestone: **2026-07-15** — preliminary footer-only slice is pushed in [PR #44](https://github.com/onedayonemasterpiece/events-bot-new/pull/44) at `421d6bca`: mobile system share, two explicit desktop copy intents, renderer/manifest/assets/lab and automated preview evidence exist. Owner also completed a native Windows test which drove the two-action refinement. The header was intentionally untouched; mobile-menu placement, formal Windows evidence + final RC rerun, macOS/mobile device coverage, `main` merge and production activation remain open.
>
> Preliminary homepage candidate: **2026-07-15** — «Городской обзор» is added to release planning as H1 with `Conditional Go`; the owner must choose `ship|defer` before F5 UI freeze. Research/consultation exists at `feature/static-typed-intro-prototype-20260715@8045599b`, while implementation/public preview and native usability evidence are absent.
>
> Event age-rating coverage: **2026-07-15** — M5 is a mandatory cross-cutting release gate: every accepted canonical `0+|6+|12+|16+|18+` must survive Smart Update/static projection and appear on every public event-bearing surface; unknown never defaults to `0+`.
>
> Linked multi-occurrence coverage: **2026-07-15** — M6 is a mandatory release blocker: when one programme repeats at several dates/times, every event card must visibly expose eligible alternatives. Existing core/Telegraph links are the foundation, but static export/card projection must become canonical and fully tested.
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
5. Значительная часть заявленного функционала находится только в side branches либо остаётся design-only: персональное письмо/страница, automatic transport refresh/final UI integration, admin→ArtKodex, durable favorites, verified-email identity, personalization merge. Предварительный rail+bus transport slice уже консолидирован и проверен в draft PR #37, но ещё не является release-ready.
6. Smart Update остаётся владельцем предотвращения дублей и ошибок фактов, но перед релизом ещё не формализован регулярный контроль его результата: cadence аудитов, incident-rate trend/SLO, обязательное заведение инцидентов и подтверждённое закрытие root causes до устойчиво низкого уровня дефектов.
7. Локальная release-проверка текущего WIP 2026-07-11: Astro собрал `110` страниц, но `npm run check:preview` упал из-за рассинхронизации UI (`Смотрите дальше`) и contract assertion (`Вам могут быть интересны`). Это не дефект `origin/main`, но доказательство, что активный WIP не является чистым RC.
8. Связанные occurrence уже существуют в core и Telegraph, но static exporter hardcode-ит `other_date_ids=[]`, detail/cards полагаются на неполную title/venue inference, а shared cards не показывают пользователю другие доступные даты/время. Поэтому M6 пока не закрыт.

## 3. Матрица требований

| ID | Требование | Статус | Текущее доказательство | Главный release gate |
|---|---|---|---|---|
| **G1** | Публичная надёжность и доступность | **Partial / Blocked** | Static-first fallback, CDN и rollback protocol описаны; root/preview/CDN отвечают `200` | Atomic promotion, last-good rollback, monitoring/SLO, clean `origin/main` RC, security/a11y/load/real-device evidence |
| **G2** | Сокращены инциденты: дубли, локации, даты/время | **Partial / Blocked** | Bounded audits 305/305 и 308/308, repairs и regression incidents | Smart Update root-cause prevention; регулярный аудит новых/изменённых и всего active/future inventory; incident burn-down/trend/SLO; closure-grade replay для повторяющихся классов дефектов |
| **F1** | Smart Update effect → rebuild через 15 минут | **Partial / Blocked** | Код coalesced `static_site_build` и Kaggle runner существует | Включить prod env; исправить running/deferred/stale semantics; atomic CDN promotion; prove two updates during one long build |
| **F2** | Автоматически актуальные похожие события через vector search + LLM verify | **Partial / vector lane configured, publication loop disabled** | `related_v1` pgvector/Gemma canaries and incremental/full vector sync exist; production config enables vector sync after Smart Update and every 180 min, but static Kaggle/strict-related flags are absent | Every effectful create/update → vector hash barrier → coalesced +15 min full-graph Kaggle build → LLM-verified changed windows → checked atomic promotion; periodic drift/lifecycle recovery, reverse-anchor E2E, 95%+ coverage and golden/hard-negative gate |
| **F3** | Умный поиск + сохранение результата как публичного тега | **Partial** | Search UI/Edge source/canary и private feedback/tag-candidate intake существуют | Production `/poisk/`; explicit save; normalization/idempotency/result-novelty curation; current-catalog static tag generation; public anonymous tag E2E |
| **F4** | Email: 3 предложения + персональная static page | **Designed** | Canonical design v2 добавлен в release-doc branch; прежняя YDB-owned docs branch superseded | Subscription/double opt-in; issue/page generator; outbox; token security; canary/live delivery |
| **F5** | UI отработан и зафиксирован | **Partial** | Большой preview/check contract; medallion baseline consolidated in draft PR #38; adaptive navigation direction documented; отдельная UX V3 branch активна | Design freeze + owner sign-off; shared global identity shell; shallow desktop-tag hybrid comparison; medallion P0 shortlist + final visual QA; visual baselines 375/768/1366; a11y/keyboard/reduced-motion/real devices; no failing RC assertions |
| **F6** | Views/list/detail/social-action personalization telemetry and application | **Partial** | Local profile/actions/served-list Playwright contract exists; full DB integration/application E2E missing | Detailed Gherkin/Playwright: localStorage → accepted/deduped DB rows → profile rollup → next feed; golden personas and `cards_to_first_relevant <=20` |
| **F7** | На каждой static HTML page: Yandex identity/email или вручную введённый verified email | **Partial / search-only implementation** | Yandex PKCE login/logout сейчас фактически живёт в `/poisk/`; site-wide shell и manual passwordless email описаны только в плане | Shared controller on 100% HTML page families; Yandex email sync/fallback; one-use email code/link with TTL/replay/rate limits; both paths converge without duplicate identity/profile; real-device proof |
| **F8** | Sender subdomains, D-1 event reminder, bounce/complaint handling | **Partial / live worker foundation** | `main@c6396331`: disabled Supabase control plane, authenticated Postbox feedback/suppression, live transactional-only worker+monitor canary, authorized-key fix | Event-specific calendar/reminder producers; templates/UX; D-1 schedule/reschedule E2E; cross-provider placement warm-up; NotiSend recommendation flow/key gate |
| **F9** | Избранное, календарь, unique counter и «Мои события» | **Missing** | Live Supabase не имеет owner/calendar table и save RPC; local `liked_event_ids` — likes, а ICS request не доказывает import; `/moi-sobytiya/`, shared badge и idempotent counter отсутствуют | Private Supabase owner/occurrence state + RLS/API; server-derived `calendar_savers_count`; global `Моё`/`Мои события` upcoming-union badge; one lifecycle-aware hub with calendar/favorite filters; cross-tab/device/merge/logout/privacy/delete/export E2E |
| **F10** | Site-wide login/logout/account state + personalization merge | **Partial / search-only implementation** | Search has Yandex account state; other static pages do not yet share it; email-only auth/forget and merge are design-only | Global restore/login/logout/add-email/forget-email across navigation/reload/tabs; explicit consent, idempotent merge API/schema, conflict/unlink/delete policy and E2E |
| **F11** | Rail/bus schedules, daily Kaggle refresh, transport card/favorite | **Partial, validated draft PR** | Draft PR [#37](https://github.com/onedayonemasterpiece/events-bot-new/pull/37) at `4577b334`: rail/bus blocks, route directories, type-prefixed event/transport ICS, directory validators and a 421-page preview/check pass; no automatic provider notebooks/orchestrator yet | Integrate into frozen UI; KPPK+bus provider jobs (separate Kaggle notebooks allowed) → one schema/fan-in/combined atomic manifest → exactly one changed-hash rebuild; provider last-good/stale alerts; exact-date matrix, gallery decision and browser/ICS evidence |
| **F12** | Add to calendar = favorite + видимый статус D-1 email reminder | **Partial** | Stable `.ics` работает; favorite mutation/reminder UX отсутствуют | Atomic/idempotent ICS+favorite; masked reminder status or inline email/consent choice; undo/cross-device/reschedule/cancel tests |
| **F13** | Site events do not become stale vs bot/core DB | **Partial / Blocked** | Candidate freshness filters and rebuild design exist | Production rebuild/promotion loop, two-update race fix, catalog parity manifest, max-staleness alert and SLO |
| **F14** | Verified comment facts + «Активно обсуждают» medallion | **Post-release planned / not a presentation GO blocker** | 2026-07-15 live canary on 20 events/109 posts/105 comments proved non-zero official decision-fact value and exposed authority/thread gaps; canonical research plan now prioritizes typed facts over the broad carousel | Separate post-release 30-day shadow → authority/Q-A/BGE+E5/literal-slot gates → deterministic «Важно знать» → Smart Update shadow ingress → independently calibrated medallion; own RC/canary/rollback |
| **F15** | Share generates image | **Partial** | Preview Web Share file → generated `1080×1350` canvas → text/copy fallback | Stable offline/server assets 1200×630, 1080×1350, 1080×1080; stale regeneration; CORS; Telegram/VK/MAX real-device tests |
| **F16** | Correct image focus/crop | **Partial** | Renderer accepts focal/face metadata and keeps OCR-safe contain fallback | Producer currently emits empty focal/face metadata; implement enrichment, confidence/manual override, golden visual corpus |
| **F17** | Admin issue report → ArtKodex repair/history | **Partial, branch-only** | Admin Edge/UI/history design and branch implementation exist | Merge; unique active/idempotency key; atomic poller claim; real ArtKodex owner; structured repair result; end-to-end repair/rebuild/history |
| **F18** | Поделиться самим сервисом: mobile menu/footer card, evidence-based Windows/macOS desktop copy | **Partial / open PR; release blocker** | [PR #44](https://github.com/onedayonemasterpiece/events-bot-new/pull/44) at `421d6bca` implements a footer-only preview with one mobile system-share action, separate desktop card/text copy actions, manifest/renderer/assets/lab and `14/14` local/public Playwright evidence; owner-confirmed native Windows test drove the refinement; header/mobile menu was intentionally untouched | Review/merge to `main`; add mobile-menu placement without destabilizing the frozen header; record exact Windows evidence and repeat it on final RC; complete macOS + Android/iOS matrices and Pharmastaff traceability; owner accepts desktop mode; production schedule/publisher and RC-bound CDN evidence |
| **H1** | Preliminary homepage «Городской обзор»: compact editorial briefing before categories/feed, optionally animated by semantic fragments | **Conditional Go / side prototype; not yet a release blocker** | Research/prototype/labs advanced through `origin/feature/static-typed-intro-prototype-20260715@02095bcd` and `origin/integration/typed-briefing-dynamic-shield-face-kinetics-20260717@b5f4797d`; neither is current-main release truth | Current-main clean slice; V1/V2 comparison; viewport/feed visibility, no-JS/reduced-motion/manual-mobile, zero-CLS/a11y/perf and grounded-copy gates; immutable preview; owner `ship|defer` before F5 freeze. Only `ship` promotes H1 gates into RC blockers. |
| **M1** | Event-detail medallion release readiness | **Partial / consolidated draft PR** | Draft PR [#38](https://github.com/onedayonemasterpiece/events-bot-new/pull/38): clean main-based slice with 25 organizer/venue + 11 festival/venue-brand entries; 420-page preview/check and 38/38 browser image load evidence | Produce/accept P0 shortlist; refresh production gap within 48h of RC; provenance/alias/no-false-match/a11y/no-overflow gates; owner mobile/desktop visual sign-off; merge to main |
| **M1-QA** | Exhaustive static-site medallion visual cleanliness | **Missing / release gate specified** | Lab load/screenshot evidence exists, but there is no SHA-bound Playwright inventory and screenshot verdict for every actual static-site target page/layout | Discover every static-site renderer/surface; capture every actual page at 390/1440 plus breakpoint combinations; zero clipping, dirty/cut shadows, alpha mattes, overlap, overflow, broken/unreadable medallions or uncaptured targets. Telegram medallions are out of scope. |
| **M2** | No duplicate images inside an event gallery | **Partial / cleanup foundation complete, observation pending** | 2026-07-13 baseline `79/266` was cleaned; final projection/TG/VK/Telegraph violations were `0` and the automatic event-media gate is in main | Repeat the full current-snapshot audit including static canonical surfaces to zero confirmed/unreviewed failures and keep the gate healthy through the 14-day stability window |
| **M3** | Consolidated source + site event engagement | **Partial / owned-source aggregation advanced** | `fe211a88`, `d25b15d6`, `b34a97d3` merged owned TG/VK collection/dedup, but site views/likes/shares and one shared projection for all consumers remain absent | One versioned batch aggregate plus shared popular-event projection for source+site views/likes/shares; migrate `/populyarnoe/` and all other consumers; idempotency/freshness/reconciliation E2E; one compact current event row + bounded evidence/TTL and size forecast |
| **M4** | Final SEO/GEO and AI-search transparency | **Designed / sequencing gate** | Static HTML, canonical, sitemap and JSON-LD foundations exist in preview, but there is no final feature-complete frozen-UI audit or reconciled multi-agent release evidence | Start only after public-feature integration plus UI/UX owner freeze; independent Codex + approved `agy` Gemini Pro + `a-opus` audits; remediate crawl/index/schema/content/performance/GEO gaps; rerun all gates on final RC |
| **M5** | Возрастная маркировка события везде, где событие показывается | **Partial / release blocker** | `aa95900a` and follow-up fixes provide canonical declared/assessed data, Smart Update/backfill and declared-only static export; production sweep completed, but no shared visible all-surface renderer/evidence exists | One projection/formatter across homepage/listings/search/tags/related/personal/favorites/festival/transport/detail/share and applicable ICS/structured data; full renderer Playwright/check-preview coverage; no invented `0+` or unresolved reliable-source loss |
| **M6** | Связанные occurrence одной программы: другие даты/время видны прямо в карточке | **Partial / release blocker** | `Event.linked_event_ids`, symmetric recompute and Telegraph `Другие даты` exist; Astro detail has a preview block, but production static export emits `other_date_ids=[]`, frontend inference is non-canonical and `EventCard`/`EventListItem` expose no visible alternatives | Canonical graph projection; Smart Update duplicate-vs-linked semantics and sibling rebuild; one accessible card pattern across all card families; occurrence-specific URL/ICS/favorite/reminder; full graph/catalog audit and Playwright matrix with zero false/dangling/same-slot alternatives |

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

Повторная read-only проверка Fly config и имён secrets 2026-07-13 подтвердила тот же разрыв: `ENABLE_EVENT_VECTOR_SYNC=1`, debounce `90s`, reconciliation `180m`; `ENABLE_STATIC_SITE_KAGGLE_BUILDER`, `STATIC_SITE_RELATED_MODE`, `STATIC_SITE_SYNC_PGVECTOR_VECTORS` и `STATIC_SITE_GEMMA_RELATED_VERIFY` отсутствуют и как config env, и как secrets. Это доказывает production-конфигурацию автоматического vector sidecar, но не текущий успешный cadence и не автоматическую публикацию блока похожих событий. Новый ledger probe в этот момент не состоялся, потому что Fly сообщил `no started VMs`; последним execution evidence остаётся успешный `ops_run=3591` от среза 2026-07-11.

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
| F11 transport | `origin/integration/event-transport-schedule` (validated refresh merge `4577b334`), draft PR [#37](https://github.com/onedayonemasterpiece/events-bot-new/pull/37) | `docs/features/event-transport/README.md` plus renderer/rail/bus child contracts | Refreshed from `main@c6396331` and validated; release UI integration and nightly atomic refresh remain P0-open |
| F14 comment feedback + Region Talk prior art | `origin/agent/event-comment-feedback-kaggle-runner@7068510c` (752 behind / 10 ahead) and `origin/agent/region-talk/bge-m3-enrichment-test@b4c3c999` (752 behind / 295 ahead) at the 2026-07-13 audit | F14 docs/runner/kernel/tests plus Region Talk compact YDB, queues, orchestrator, vector/session skills and tests | Both are evidence, not merge bases. The separate post-release Stage 8A starts with the formal adoption audit, then a clean current-main 30-day shadow and typed-fact implementation. |
| F17 admin issue reporting | `origin/feature/event-issue-report-artkodex-20260703` (69 behind / 12 ahead) | `docs/features/event-issue-reporting/README.md`, UI/Edge/migration | Mixed branch; no DB-level double-start guard/poller proof |
| H1 homepage «Городской обзор» | [`origin/feature/static-typed-intro-prototype-20260715@8045599b`](https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/static-typed-intro-prototype-20260715) (`0` behind / `1` ahead of `origin/main@926dad8a` on 2026-07-15) | `typed-briefing-hero-research.md`; no site code despite the branch name; raw Gemini prompt/output/metadata only in ignored `artifacts/codex/static-typed-intro-consultation-20260715/` | Clean research source, not an implementation or release decision. Import the canonical contract, then build isolated V1/V2 prototypes on a fresh current-main implementation branch; never claim raw local artifacts as RC evidence. |

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
| F11 | Предварительный rail+bus slice консолидирован в обновлённой от main ветке и draft PR #37; nightly production refresh и интеграция с финальным UI остаются P0-open. |
| F12 | ICS находится в main; отдельной branch для product contract “calendar = favorite” нет. |
| F14 | Есть stale probe branch и divergent Region Talk evidence branch; ни одну нельзя вливать целиком. Post-release implementation begins on current main with the canonical 30-day authority/Q-A/BGE+E5 shadow, then typed YDB/static slices. |
| F15 | Preview implementation находится в main, production share-asset generator остаётся debt без отдельной завершённой ветки. |
| F16 | Renderer contract находится в main; producer focal/face metadata и его feature branch не найдены. |
| F17 | Есть side branch, но она mixed/stale и не закрывает idempotent ArtKodex poller/result E2E. |
| F18 | Canonical contract дополнен предварительным current-main-based footer slice в PR #44 (`421d6bca`): shared component/controller, renderer/manifest/assets/lab и browser evidence существуют; owner-confirmed native Windows test привёл к текущему раздельному image/text copy behavior. Это не full closure: header/mobile-menu placement намеренно не реализован, Windows evidence нужно формализовать и повторить на RC, macOS/mobile matrix, Pharmastaff traceability, main merge и production activation отсутствуют. |
| H1 | Current-main-based branch `feature/static-typed-intro-prototype-20260715@8045599b` correctly documents a `Conditional Go`, but it contains no Astro component, lab page, manifest, automated test or public preview. The release plan imports the contract without promoting the candidate into the mandatory presentation scope. |

Следовательно, документацию нельзя считать полностью и корректно оформленной по всем F1–F18, а наличие H1 research нельзя считать реализацией homepage feature. Корректная цель — один canonical feature home на capability/связную feature family, актуальный status matrix, routes, operations/tests и явная связь с implementation branch; создавать искусственную ветку на каждый ID необязательно.

## 6. Аудит полноты документации

| Область | Полнота | Проблема консолидации | Требуемое действие |
|---|---|---|---|
| Static pages/build/CDN | Высокая | `README.md` смешивает current contract, preview diary и historical decisions | Оставить current contract + gates; version diary вынести в reports |
| Vector related/search | Высокая | `routes.yml`/README statuses не отражают capability-by-capability stage | Ввести status matrix: infra / canary / prod / operations |
| Anonymous personalization | Высокая design, низкая prod | README одновременно говорит anonymous-only и содержит authorized search | Разделить identity/search/telemetry/feed, сохранить один parent index |
| Site identity/account | Canonical product contract expanded | Фактический Yandex login/logout сосредоточен в `/poisk/`; shared shell, email-only passwordless session and forget semantics are not implemented | One common-layout controller, 100% HTML route matrix, code/link+Yandex callback restore, logout/forget/cross-tab/session-expiry E2E |
| Personal email/page | Canonical design v2 подготовлен | Реализация отсутствует; старая branch назначала неверного владельца | Новый main-based feature branch по принятому Supabase control-plane ADR |
| Email delivery | Средняя, branch-only | Transactional follow и recommendation digest смешиваются концептуально | Разделить transactional/reminder и recommendation marketing streams |
| UI/share/focus/medallions | Средняя/высокая | Medallion implementation was split across mixed branches; remaining shortlist was not release-routed | Use clean PR #38 only; finish production-backed P0 shortlist, provenance/alias checks and owner visual acceptance inside the frozen UI |
| Homepage «Городской обзор» H1 | Сильная research specification, нулевая implementation evidence | Branch name says prototype, but commit `8045599b` changes only docs; raw Gemini evidence is ignored/local, and production inclusion is intentionally unresolved | Preserve `Conditional Go`; build V1 static and V2 semantic-motion lab separately, compare downstream discovery and accessibility/performance, then obtain explicit owner `ship|defer` before F5 freeze |
| Event age rating M5 | Source extraction частично существует, end-to-end contract отсутствует | `age_restriction` живёт в отдельных parser payloads/tests or prose; canonical Event/static types/exporters/renderers do not guarantee preservation or all-surface display | Add nullable canonical/provenance field, Smart Update LLM reconciliation and backfill; inventory every event renderer/derivative; shared formatter + check-preview/Playwright/data audit to 100% accepted-value coverage |
| Linked occurrences M6 | Core relation and Telegraph UI are real, static-card support is not end-to-end | `Event.linked_event_ids` is persisted/recomputed, but static exporter writes `other_date_ids=[]`; build-time title/type/venue inference can miss or falsely join groups, and shared cards do not show alternative date/time choices | Export canonical symmetric links; make link changes effectful for F1; add one shared card projection/UI; audit duplicate-vs-linked graph; test every surface and occurrence-specific action |
| Service sharing F18 | Footer-only implementation preview готов в PR #44 (`421d6bca`): mobile share, explicit desktop image/text copy, component/controller, metric manifest, deterministic WebP+PNG renderer, lab/browser checks и owner-confirmed native Windows test с последующими доработками | Код ещё не в `main`; шапка/mobile menu не тронуты; Windows record/final-RC rerun, точный Pharmastaff source/SHA, Android/iOS и macOS matrices отсутствуют; production scheduler/publisher выключен | Review/merge PR; реализовать вторую обязательную placement после согласования шапки; формализовать и повторить Windows evidence на RC; привязать Pharmastaff reference; выполнить macOS/mobile matrices; owner утверждает desktop mode/copy/visual; включить и проверить production promotion |
| Transport | Канонический home/route и renderer/rail/bus child contracts консолидированы; draft PR #37 прошёл directory + full preview checks | Coverage уже требования; нет nightly atomic refresh и принятой интеграции в release UI | Сохранить один reusable slice; добавить city/provider/source matrix, ops runbook, automatic last-good refresh и UI acceptance |
| Comment feedback | Live canary proved official decision-fact value; Region Talk gives prior art for YDB/queues/vectors/Telethon/compaction | Authority/thread metadata is lost, current bank is question-oriented, fact lifecycle/medallion baseline are absent and old branches are not merge bases | Execute the post-release plan: exact-SHA clean-port audit → 30-day shadow → typed ledger → deterministic «Важно знать» → Smart Update shadow → medallion calibration |
| Admin incident report | Хорошая product prose | Документ переоценивает готовность ArtKodex poller/idempotency | Явно разделить UI, DB queue, poller, repair result, E2E statuses |
| Event quality / Smart Update | Сильные incident records | `routes.yml` не перечисляет свежие Jul-7/9/10/11 contracts; нет единого cadence/SLO/dashboard и release burn-down | Добавить routes; закрепить Smart Update как prevention owner; описать регулярный audit → incident → root-cause fix → replay → monitoring workflow |

Дополнительные найденные дефекты документации/contract hygiene:

- в `origin/main` отсутствует единый release checklist — этот документ закрывает навигационный пробел;
- свежие event-quality incidents присутствуют в incident index, но отсутствуют в `docs/routes.yml`;
- side-branch features не должны попадать в main feature index до merge, но release plan обязан явно показывать их статус;
- названия/пути `alanytics.md`, `requitements.md`, `trip-recomendation` являются legacy typo debt; переименование допустимо только через redirect stubs;
- часть старых review docs содержит вердикты “not canary-ready”, хотя pgvector canary уже выполнен; review artifact нельзя использовать как current status без application matrix;
- historical WIP had a broken relative link in `event-token-medallions.md`; the clean PR #38 canonical doc now passes scoped link validation. The separate UI/check-preview assertion drift still belongs to the release-UI task.
- parent static-site README всё ещё одновременно говорит “preview/auth out of scope”, описывает старый v44 как current и называет регулярный DB export следующим шагом; current status нужно сверить с Jul-02 build и реальным `origin/main` code path;
- operations doc обещает follow-up build при update во время running build, но `main.py` не реализует это для `static_site_build` — документацию нельзя считать доказательством поведения;
- `event_active_where`/public projection tests требуют отдельной проверки поля `silent`: текущий exporter/tests не дают уверенности, что silent rows не попадут в публичный static surface;
- `INC-2026-07-08-prod-root-overlay-disk-full.md` найден только в старом dirty checkout, но отсутствует в `origin/main`; regression contract недолговечен, пока record не интегрирован канонически.

## 7. Обязательные release stages

Каждая стадия выполняется отдельными feature/task branches. Следующая стадия не делает предыдущую необязательной.

### Stage 0 — Scope freeze и интеграционная база

- [x] Утверждён presentation launch scope: F1–F13 and F15–F18 remain mandatory; F14 is the explicit separate post-release exception and partial F14 code cannot enter the first RC.
- [x] M6 linked-occurrence choices in cards добавлен как cross-cutting release blocker, а не post-release enhancement.
- [ ] Создать integration branch только от свежего `origin/main`.
- [ ] Для каждой side branch записать owner, base/head SHA, rebase plan, tests, merge/reject decision.
- [ ] Не переносить весь dirty checkout; cherry-pick/re-implement только проверяемые feature commits.
- [ ] Зафиксировать UI release version и data/schema versions.
- [ ] Зафиксировать adaptive navigation baseline: одинаковые destinations/order/labels/account semantics, mobile tag/disclosure, desktop persistent horizontal row; сравнить plain bar / recommended shallow hybrid / pronounced-tag control and получить owner sign-off на immutable preview.
- [ ] Before F5 freeze resolve the preliminary H1 homepage candidate: compare categories-first control with a V1 static «Городской обзор» and only then V2 semantic-fragment motion; record owner `ship|defer`. `Defer` leaves the mandatory presentation scope unchanged; `ship` adds every H1 acceptance item to the RC and requires the final SEO/GEO audit to run after integration.
- [ ] Закрыть продуктовые решения из раздела 10.

### Stage 1 — Стабилизация качества Smart Update и incident burn-down

Smart Update является владельцем семантического предотвращения дублей, неверных локаций, дат и времени. Предрелизный контур не должен переносить эту ответственность в static exporter или отдельный широкоформатный regex/keyword gate.

- [ ] Утвердить cadence: ежедневный аудит всех новых/изменённых событий и регулярный полный аудит active/future inventory до релиза.
- [ ] Для каждого подтверждённого дефекта создавать/обновлять incident record, классифицировать failure family и фиксировать затронутые публичные поверхности.
- [ ] Для повторяемого класса находить root cause в extractor/import/Smart Update match-merge/writer/reference layer, а не ограничиваться repair строки или страницы.
- [ ] Каждый root-cause fix проходит closure-grade replay через реальный production import boundary и `smart_event_update.py` на snapshot/shadow DB, затем проверяется на Telegram/VK/Telegraph/static/ICS surfaces.
- [ ] Прогнать обязательный regression pack: duplicate doors/start, two vendors, venue aliases, prose/person venue, default-venue offsite, compact/hashtag dates, recurring occurrence/season, exhibition duplicates, valid multi-session no-merge.
- [ ] На release cutoff выполнить полный аудит exact active/future catalog и устранить все подтверждённые high-impact дефекты.
- [ ] M5 age-rating audit joins the quality cadence: compare structured source fields and source-grounded text/OCR with canonical values; classify `source_absent|canonical_missing|conflict|invalid|projection_lost|renderer_missing`, backfill reliable losses through Smart Update and investigate false positives where a programme/door/audience age was mistaken for the event rating.
- [ ] M6 linked-graph audit joins the same cadence: verify symmetric/no-self/no-dangling groups, distinguish same-slot duplicates from different occurrences, investigate false «Другие даты» and missing siblings, and replay merge/split/reschedule/cancel incidents through core + Telegraph + static cards.
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
- [ ] Protected pre-release operations scorecard reconciles expected slot → run → accepted delivery for static promotion/CDN, KPPK/bus refresh, current image-dedup audit and promo fulfilment; missing evidence is `unknown/critical`, transition/recovery alerts are deduplicated, and an immutable RC snapshot is attached. The full [web dashboard](../backlog/features/operations-control-dashboard/README.md) remains a separate post-release release.

### Stage 3 — Related/search readiness

- [ ] Active/future vector coverage target `>=95%`, one model/dimension per build.
- [ ] Каждый effectful Smart Update create/update автоматически ставит `event_vector_sync:prod` после short debounce и один `static_site_build:prod` на 15 минут после последнего эффекта; no-effect retry не тратит provider/build ресурсы.
- [ ] Static build проходит vector freshness barrier: для snapshot совпадают eligible ids и `search_v3`/`related_v1` text hashes; incomplete/capped sync вызывает retry/fail candidate build, а не stale retrieval.
- [ ] Whole active/future related graph recomputed after any changed/new event, including reverse impact on older anchors; rebuild only of the changed event is insufficient.
- [ ] Changed anchor/candidate fingerprints проходят LLM verify/reorder; cache hit допустим только при совпадении обоих document hashes и policy signature. Raw pgvector candidates never appear under verified `Похожие` after provider failure.
- [ ] Running-build race E2E: update A starts a build, update B during it creates exactly one deferred build from a newer immutable snapshot and final promoted manifest contains B.
- [ ] Independent reconciliation at least every current vector interval (`180m`) compares catalogue/vector hashes with the promoted related manifest and automatically enqueues drift recovery; scheduled lifecycle build expires started/ended rows even without Smart Update.
- [ ] Correlated automation evidence records Smart Update effect/event ids → vector `ops_run` → outbox/static job → Kaggle run/build → manifest hashes/coverage/LLM calls+cache+rejects+errors → promoted release id; alerts and catch-up cover every missing transition.
- [ ] E2E create/update/no-effect/burst/vector-failure/LLM-failure/periodic-recovery/time-expiry asserts promoted discovery JSON and rendered order for changed plus reverse-affected older anchors.
- [ ] Golden anchors + hard negatives pass editorial review; no popularity boost corrupts pure-related slots.
- [ ] UI/manifest enforce the accepted label contract: every `Похожие` candidate has current verifier evidence; vector-only material is allowed only in a separately identified neutral/degraded continuation if explicitly accepted, never silently under the verified label.
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
- [ ] Один shared identity controller/account menu подключён к common layout и покрывает 100% generated HTML page families: root, listings/categories/tags, event, search, forwardable personal page, transport-enabled event and admin HTML; machine artifacts explicitly excluded.
- [ ] На любой HTML page доступны одинаковые anonymous actions `Войти через Яндекс` и `Добавить почту`; login/code/link callback returns to the same cleaned URL and the restored state survives navigation, reload and same-origin tabs.
- [ ] Verified manual email becomes a real lightweight passwordless Supabase session without extra profile fields; pending/localStorage-only email is never treated as authentication.
- [ ] Global account menu consistently supports `Выйти` and `Забыть почту на этом устройстве`: logout preserves durable data, forget clears email cache/email-only browser session but does not silently delete account/consents/scheduled mail.
- [ ] Search/favorites/reminder/personalization consume the shared session/controller; no page-specific competing auth store, callback or logout implementation remains.
- [ ] No consent = no local profile mutation and no trusted remote telemetry.
- [ ] Remote telemetry write path passes RLS/grant/body/rate/dedupe/bot/retention tests.
- [ ] Valid impressions, detail views, dwell, card click, ticket/calendar/share/like/hide contain surface/rank/layout context.
- [ ] One canonical `load_consolidated_event_engagement`-equivalent batch function combines distinct TG/VK source posts with accepted site valid views/current likes/shares, retains components/freshness and supplies `/populyarnoe/`, `/popular_posts`, daily, CherryFlash/`/v`, static counter export and allowed rankers.
- [ ] `/populyarnoe/` receives a SHA-bound precomputed order/score from one versioned domain ranker: no independent Astro median/weights remain; source-only, site-only and blended golden events each affect ranking materially; rendered IDs/order equal the projection manifest.
- [ ] The main popular list stays global and current/future-only; user filters only narrow it, while `PersonalFeedSlot` is separately labelled and cannot reorder/insert into the popular projection.
- [ ] Combined-popularity public copy is enabled only with fresh source and site components; missing site data is not a fresh zero, and a partial failure preserves last-good full projection or shows an explicitly accepted degraded state.
- [ ] Consolidated engagement contract proves latest-valid source maturity bucket (no `age_day` double count), canonical post mapping, site action/view idempotency, event merge/recompute, source-only/site-only/stale/last-good behavior and versioned popularity scoring.
- [ ] Verified email supports both one-time code and one-click link for the same identity/transaction, with TTL and replay/attempt limits.
- [ ] На всех email-dependent surfaces есть одинаковый выбор: Yandex login/email или ручной ввод почты; Yandex без usable email переводит в manual verification, не ломая identity и не создавая второй профиль.
- [ ] Manual-email path writes/reuses versioned `ke_contact_email_v1`: one entry per browser, pending/verified state survives reload and later saves, global `Забыть почту на этом устройстве` works, and local cache never substitutes for server verification/consent.
- [ ] Automatic anonymous→authorized merge is idempotent, auditable and reversible; logout/unlink/delete behavior is distinct and defined.
- [ ] Favorite, like and calendar semantics no longer conflated.
- [ ] Shared navigation contains one `Моё` (mobile) / `Мои события` (desktop/accessibility) item on every interactive static HTML family; after restore its numeric badge appears only for distinct upcoming calendar/favorite union `N>0`, counts dual state once and never reuses likes, downloads, reminders, past or transport legs.
- [ ] `/moi-sobytiya/` is a `noindex` static shell with no user data in CDN HTML/cache; one authenticated batch/RLS load shows calendar/favorite union plus upcoming/rescheduled/cancelled/merged/past groups without silent disappearance or per-card remote loops. `/izbrannoe/` opens the same shell with the favorite filter rather than a second store.
- [ ] Add-to-calendar/favorite is atomic/idempotent; repeat/undo/cross-device/lifecycle cases pass.
- [ ] `calendar_savers_count` считает один `first calendar save` на trusted profile + canonical occurrence; repeated ICS requests, retries, concurrent tabs и remove/re-add не увеличивают его.
- [ ] Calendar-save mutation имеет unique owner/occurrence constraint, `action_id` idempotency и transactional anonymous→authorized/event-merge reconciliation; browser не пишет aggregate напрямую.
- [ ] ICS request/download и принятое KenigEvents save action измеряются раздельно; UI не утверждает, что внешний calendar app завершил import, и не завышает trusted counter при backend failure.
- [ ] `Моё` (mobile) / `Мои события` (desktop) ведёт на один `/moi-sobytiya/` shell с upcoming/past groups и filters `В календаре`/«Избранное»; `/izbrannoe/` не создаёт второй store.
- [ ] Personal-hub badge считает distinct upcoming union, dual state один раз; lifecycle changes видимы, а remove copy объясняет, что сайт не может удалить уже импортированную запись из внешнего календаря.
- [ ] Playwright `FAV-MENU/PAGE/LINK/PRIVACY/DEGRADED` scenarios prove count/list updates, cross-tab/device convergence, Yandex/email merge, logout/account-switch isolation and ICS independence during favorite-backend failure.
- [ ] После save UI показывает либо `Напомним за день на a***@domain`, либо inline action для email verification/transactional consent; при старте менее чем через 24 часа обещание D-1 не показывается.

### Stage 4A — Supabase capacity and ecological storage

- [ ] Канонический [500 MB storage/compaction contract](../operations/personalization-storage-budget.md) реализован; current provider limit перепроверен перед canary.
- [ ] Launch baseline находится в Green (`<60%` verified plan limit) и имеет forecast headroom through canary/hypercare; измеряются таблицы **и индексы**, bytes/user-day и projected days to Orange.
- [ ] Raw weak telemetry по умолчанию выключена; browser/session compacts signals, Supabase хранит current state + bounded evidence, YDB получает только asynchronous de-identified TTL analytics, artifacts идут в Object Storage/CDN.
- [ ] Social/site engagement uses one compact current aggregate row per event; raw site page views are never an unbounded Supabase stream, source snapshot history is not copied into Postgres, and relation+index bytes/updates per event are included in the launch/1k/10k model.
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
- [ ] Deterministic contract suite проверяет `identity_shell_html_coverage=100%`, Yandex/email login from non-search pages, logout/forget/cross-tab/session-expiry, consent/no-consent, localStorage schema/migration/corruption, valid impressions/actions, dedupe, hidden state, static fallback и DB/network failure.
- [ ] Integration suite запускает реальный browser flow через изолированный Supabase namespace/project; service-side fixture отдельно проверяет accepted rows, dedupe, profile snapshots и cleanup, не передавая secret key в browser.
- [ ] Один correlated run доказывает четыре стадии: сигнал появился в localStorage → принят в БД → изменил ожидаемый профиль → применён к следующей выдаче.
- [ ] Golden-persona pack включает «Чайковский», cold/mature, positive/negative interests, mobile/desktop, no-relevant-supply и catalog refresh; ground truth строится по canonical event features, не по broad title-regex.
- [ ] Для каждого eligible mature golden scenario `cards_to_first_relevant <= 20`; mature fixture и правила valid impression/meaningful action/versioning зафиксированы в evidence.
- [ ] Dashboard/artefact считает collection success, duplicate/drop reasons, profile rollup lag, profile-to-feed application, MRR/precision@20, relevant supply, top-20 hide/skip/diversity/fallback guardrails.
- [ ] Near-cap E2E `PERS-STORAGE-001` доказывает, что nonessential telemetry shed не создаёт retry storm и не блокирует durable user-control/email-safety state.

### Stage 5B — Preliminary homepage «Городской обзор» decision gate

This stage resolves H1 before F5/UI freeze. It is a mandatory **decision**, not an automatic requirement to ship the candidate.

- [ ] Treat [`feature/static-typed-intro-prototype-20260715@8045599b`](https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/static-typed-intro-prototype-20260715) as research only: the commit contains documentation, not a prototype. Raw `artifacts/codex/static-typed-intro-consultation-20260715/` stays ignored and may support review but cannot replace committed tests/preview evidence.
- [ ] V1 static prototype renders one immediately useful grounded editorial scene in Astro SSG/no-JS HTML before categories and feed, without personalization, Gemini/runtime LLM or remote blocking read. Compare it to categories-first control using real header/category/feed geometry.
- [ ] Pass fixed layout budgets at `1440×900`, `1366×768`, `390×844`, `360×800`, `320×568`: briefing `<=50svh`, combined header+briefing+categories `<=72svh`, and at least `min(96px,16svh)` of feed visible; no interactive link is truncated or hidden by line clamp.
- [ ] Only after V1 content/layout value is accepted, V2 may add semantic-fragment entrance. Literal typewriter remains diagnostic only. Mobile and `prefers-reduced-motion` are static/manual; no infinite loop; pointer/focus completes and pauses without breaking first-click link activation; link hitboxes are stable and scene changes produce CLS `0`.
- [ ] Fact and link tokens are deterministic, versioned and fail closed: counts/time/new-since-visit/popularity/editorial/personal claims carry provenance and `safe_until`; missing/stale data selects a hand-written generic fallback. Client LLM and ungrounded urgency/fake intimacy are forbidden.
- [ ] Playwright/visual/a11y evidence covers no-JS, reduced motion, keyboard, screen reader announcement model, scroll/visibility/BFCache/resize, corrupted local state, slow/offline, low-end mobile performance and 320–1440 screenshots. Approved lockup is unchanged; decorative wide-«о», if tried, is separate and `aria-hidden`.
- [ ] Evaluate A categories-first, B static briefing and C semantic motion on downstream `event_detail_reached` / Discovery Transition Rate, time to first category/event action, feed visibility, bounce/misclick, CWV/JS errors and Day 7/14 novelty decay. Hero CTR alone cannot justify shipment; D literal typewriter is diagnostic, not a release candidate.
- [ ] Owner signs one immutable preview branch/SHA as `ship` or records `defer` with reason. On `ship`, H1 is integrated before F5 freeze and becomes part of Stage 6A/7/evidence/rollback; on `defer`, remove the experimental surface from the RC and continue with the ordinary categories/feed homepage.

### Stage 6 — Transport, admin repair, media

- [x] Static medallion archaeology is consolidated in clean draft PR #38: 25 organizer/venue + 11 festival/venue-brand entries, 420-page preview/check, 38/38 lazy images loaded, zero browser errors and no 390px overflow.
- [ ] Complete or explicitly owner-defer every canonical P0 medallion shortlist item: Понарт; Дом железнодорожников/ДКЖ; Солёная ворона; Центр «Мой бизнес»; Гусевский музей; Закхаймские ворота; Замок Тапиау; Pianissimo.
- [ ] Re-run medallion gap audit within 48h of RC; review every uncovered venue/festival with `>=4` active current/future events, deduplicate address aliases, and record `implemented|deferred_by_owner` with reason.
- [ ] Final medallion acceptance proves official/source-faithful provenance, no guessed/distorted logo, correct alias boundaries, no duplicate tokens/broken assets, accessible labels, no overflow and owner-approved 390px/1440px visuals in the frozen release UI.
- [ ] Run the separate [medallion visual QA protocol](../features/static-site-pages/medallion-visual-qa.md): inventory every actual static-site renderer/URL, capture every medallion-bearing page through Playwright at `390×844` and `1440×1000`, capture each distinct breakpoint/layout plus the lab asset sheet, and attach the SHA-bound inventory/verdict ledger. Telegram custom-emoji medallions are explicitly outside M1-QA.
- [ ] Medallion visual gate has zero missing captures or unresolved defects: no cropped artwork/rings/shadows, dirty alpha fringe/matte, abruptly cut shadows, overlap, horizontal overflow, broken fallback, unreadable mark, wrong duplicate token or `blocked_capture`; prior lab-only evidence cannot satisfy this gate.
- [x] Preliminary rail/bus renderers, official route directories and type-prefixed event/transport ICS are consolidated in refreshed draft PR #37 and pass directory validators plus a 421-page preview/check.
- [ ] Integrate the reusable transport slice into the frozen release UI without duplicating selector/data logic; approve mobile/desktop/no-JS/stale/unsupported visual baselines.
- [ ] Prototype the optional single gallery slide **«Как добраться»** after genuine event media; generate it from the canonical validated selector as safe SVG/lightweight WebP, retain the full accessible block, exclude it from hero/OG/event-image JSON-LD, and either accept or explicitly owner-defer it without weakening F11.
- [ ] Transport city/provider matrix approved; rail/bus sources/licensing/refresh SLAs and exact-date public coverage recorded.
- [ ] Nightly/manual KPPK rail and bus refresh jobs run automatically; provider-specific Kaggle notebooks are permitted, but each has its own lease/heartbeat/retry/catch-up and both feed one versioned normalized schema plus server-side fan-in.
- [ ] Provider atomic last-good and combined-manifest promotion pass empty/partial/excessive-diff/layout-drift drills; one provider failure never erases the other, stale age alerts/fails closed after the approved limit, and a changed combined hash triggers exactly one coalesced static rebuild.
- [x] F14 presentation-scope decision recorded: verified comment facts, Smart Update comment ingress and the «Активно обсуждают» medallion are routed to the separate post-release Stage 8A and do not block Stage 6/7 GO. No partial collector/UI is allowed to leak into the first RC.
- [ ] Admin report: allowlisted auth, event snapshot, idempotency key/unique active constraint, atomic poller claim, crash/retry safety, structured repair result/history/re-report.
- [ ] Offline focal/face/saliency enrichment with confidence/fallback/manual override and golden crop corpus.
- [x] Establish the exhaustive 2026-07-13 [event image duplicate baseline](../operations/event-image-duplicate-audit.md): union `Event.photo_urls` + `EventPoster`, exact byte/pixel hashes, candidate review and recorded visual decisions for `158/158` eligible multi-image events; it found confirmed duplicate refs in `79/266` eligible events.
- [ ] After production-safe cleanup/root-cause replay and public rebuild, repeat that full inventory audit on the RC snapshot and attach the zero-failure ledger; baseline evidence alone does not close M2.
- [ ] RC/hypercare gate: `events_with_confirmed_intra_event_duplicates=0`, `confirmed_excess_duplicate_refs=0`, `unreviewed_candidate_clusters=0`; new/changed multi-image events remain clean through the 14-day quality window. Repair confirmed ingest/Smart Update/persistence/render root causes and replay incidents, not only DB URLs.
- [ ] Stable share assets and real Telegram/VK/MAX tests.
- [ ] M5 canonical contract is in main: nullable `age_restriction` accepts only normalized `0+|6+|12+|16+|18+`, retains bounded provenance/decision version and is reconciled LLM-first by Smart Update. Unknown/ambiguous never defaults to `0+`; audience age, accompaniment, programme-only and `DC/FC` constraints are not silently collapsed.
- [ ] Static export/types/fingerprints and every event projection carry the canonical value; one shared accessible formatter renders it on homepage/listing/search/tag/related/personal/favorite/festival/transport cards, event detail and generated first-party share summaries, with supported ICS/structured-data parity and no description reparsing.
- [ ] M5 full-surface QA inventories every concrete event renderer, captures all five ratings plus unknown at `320/360/390/768/1366/1440`, and blocks on missing/contradictory/invented/clipped/color-only labels, overflow or no-JS mismatch. `check:preview` fails when a non-null projected rating is absent on an inventoried surface.
- [ ] Full RC catalog audit has zero unresolved reliable-source → canonical loss and zero confirmed false ratings; create/update/merge/backfill changes trigger F1 checked rebuild/promotion and monitoring opens `canonical_missing|projection_lost|renderer_missing|conflict` incidents.
- [ ] M6 canonical projection replaces `other_date_ids=[]`: every eligible static event carries its symmetric `linked_event_ids`, while frontend title/type/venue inference is removed as source of truth or kept only as a fail-closed diagnostic.
- [ ] One shared linked-slot formatter/component renders nearest alternative dates/times and total count in every `EventCard`/`EventListItem` family; event detail keeps the complete `Другие даты` block and siblings never enter `Похожие`.
- [ ] M6 actions are occurrence-specific: alternative link, ICS, favorite/calendar, D-1 reminder, ticket/status and transport facts all belong to the selected sibling; same-day collapsed cards expose all eligible times or an explicit `ещё N` control.
- [ ] M6 Playwright/check-preview matrix covers same-day times, different dates, mixed groups, long groups, no/past/cancelled/postponed/sold-out alternatives, same-slot duplicate, false similar title, no-JS, mobile/tablet/desktop and timezone boundary.
- [ ] F18 service share: one common shell component is visible under the expanded mobile brand tag and in the footer on every public page family; desktop never invokes native share and keeps the Windows-tested intents explicit — image-only card copy and separate text+URL copy — until final-RC matrix/owner acceptance.
- [x] Preliminary native Windows copy/paste test was completed by the owner on the PR #44 preview and directly drove the split into image-only card copy and separate text+URL copy. Treat it as iteration evidence, not final-RC evidence.
- [ ] Bind the exact tested Pharmastaff organization-card share reference/SHA, then prove the transferred `canShare(files)`/transient-activation/cancel/error/fallback behavior with Playwright stubs and real Android/iOS Telegram/VK/MAX checks.
- [ ] Generate the service card centrally from the accepted catalog snapshot: conservative event/city metrics + copy/template versions → deterministic `1080×1350` WebP (`<=350 KiB`) plus a true same-payload PNG only for desktop clipboard compatibility + manifest/hash → CDN upload before one atomic release promotion. No browser/per-click composition render and no user data.
- [ ] Complete the [F18 Windows/macOS clipboard matrix](../features/static-site-pages/service-sharing-desktop-clipboard-research.md) on the final RC for the actually shipping `copy_text_link` and `copy_image_png` actions: Windows Edge/Chrome/Firefox; macOS Safari/Chrome/Firefox; controlled plain/image targets plus Telegram, VK, MAX where available and native apps. Record exact versions, `2/2` repeatability, paste taxonomy and redacted screenshots. D1/D2 multi-representation candidates are repeated only if reintroduced.
- [ ] The selected desktop clipboard behavior passes secure-context/focus/user-activation/CORS/iframe-negative cases and all fallbacks. If rich multi-representation D1/D2 returns, add Safari-Promise and exact `ClipboardItem` type-order gates. API success is never reported as paste/send success, and `empty_paste` after declared success is a blocker.
- [ ] After final-RC matrix synthesis the owner confirms the current two-action behavior or explicitly selects D0/D1/D2 and accepts labels/status copy. Playwright payload tests, the preliminary Windows run or Linux WebKit do not substitute for final native Windows/macOS/application evidence.
- [ ] F18 claim gate: superlatives/comparisons require reproducible dated evidence; the `<=20` personalization promise and D-1 reminder wording appear only after their own gates. Otherwise the accepted concise copy is «Найдите своё событие быстрее» with factual `{N}+`/city coverage and a visible `kenigevents.ru` CTA.
- [ ] Owner approves the exact V1 lettering/logo card, thumbnail readability, both mobile placements and the matrix-backed desktop copy state. The historical poster-cube/bento concept remains a future V2 and cannot delay release V1.

### Stage 6A — Final SEO/GEO optimization after UI/UX freeze

This is the last pre-RC quality stage and a hard successor of F5/UI/UX acceptance plus integration of all release-scope features that can change public HTML. H1 must already have a recorded `ship|defer`; an accepted H1 is integrated and frozen before this stage, while a deferred H1 is absent from the RC. No final SEO/GEO audit or optimization task starts against a moving navigation, page-family set, content hierarchy, layout or interaction design.

- [ ] Record the immutable `origin/main`-reachable feature-complete UI/UX acceptance SHA, full preview build id and owner sign-off before opening SEO/GEO work; no unresolved release task may still change public page families/navigation/content.
- [ ] Build the neutral full-site evidence pack from the frozen output: URL/page-shape inventory, rendered HTML crawl, status/canonical/robots/sitemap/redirect map, JSON-LD, link graph, no-JS/mobile screenshots, media/performance/CDN evidence and negative private/preview/personal surfaces.
- [ ] Complete three independent blind audits against the same evidence: Codex; Gemini Pro High through `agy` resolving only to `gemini-3.1-pro-preview` or `gemini-3-pro-preview`; Opus high through `a-opus`. Save exact prompts, raw outputs, model/provider status and timestamps.
- [ ] Do not substitute Flash/Lite/Gemma or an unspecified fallback for Gemini Pro, and do not mark an empty/failed `a-opus` run complete. Because all three lanes are owner-required, a missing lane blocks M4.
- [ ] SEO audit covers 100% intended indexable/negative surfaces: crawlability, status/redirect/canonical identity, robots/sitemaps/`lastmod`, metadata/snippets, internal links/orphans/thin duplicates, lifecycle pages, rendered JSON-LD/visible-fact parity, images/CDN/mobile/no-JS/current Core Web Vitals and Google/Yandex/Bing behavior.
- [ ] GEO audit proves transparent static answerability for what/where/when/who/price/status, source/freshness/entity clarity, crawler search-versus-training policy, optional evidence-based `llms.txt` decision, passage citability without AI-only filler, and a representative regional event query pack.
- [ ] Codex reconciles every finding from all three reports into one evidence ledger; unique Critical/High findings are reproduced or disproved, disagreements use current primary documentation/live output, and implementation is routed to separate scoped tasks.
- [ ] Zero unresolved Critical/High findings, factual visible/structured-data conflicts, unintended indexable surfaces, sitemap/canonical leaks or missing page families remain; Medium/Low findings have explicit disposition.
- [ ] Any remediation that changes visible copy, order, navigation, interaction or layout reopens the affected UI/UX visual acceptance; after it passes, the final full SEO/GEO audit reruns on the new SHA.
- [ ] Codex full deterministic rerun plus independent Gemini Pro and `a-opus` final diff/regression reviews accept the exact RC SHA to be promoted. See the canonical [SEO/GEO release contract](../features/static-site-pages/seo-geo-release-optimization.md).

### Stage 7 — Release candidate, canary и public launch

- [ ] Clean `origin/main`-reachable RC SHA; no release fixes only in side branches.
- [ ] Full site build/check from clean checkout passes.
- [ ] Full active/future image-uniqueness ledger is attached to the RC evidence pack and current static/Telegraph/TG/VK surfaces have no confirmed within-event duplicate images.
- [ ] Engagement reconciliation on the RC snapshot proves the shared function equals distinct TG/VK latest snapshots plus accepted site summaries; all named consumers use it and storage/freshness/last-good evidence is attached.
- [ ] Playwright/mobile/desktop visual baselines, keyboard/a11y/reduced-motion, no-JS and slow/offline fallback pass.
- [ ] H1 decision is bound to the RC: `defer` means no experimental briefing code/manifest/UI ships; `ship` means the accepted static/motion variant, grounded manifest, viewport/feed budgets, accessibility/performance/experiment evidence and rollback all match this SHA.
- [ ] M5 evidence is bound to this RC SHA/catalog hash: all accepted canonical age ratings appear consistently on every inventoried event-bearing HTML surface and first-party summary derivative, unknown stays unlabelled, source/canonical/projection audits have zero unresolved reliable loss/false rating and rollback cannot restore contradictory generated pages.
- [ ] M6 evidence is bound to this RC SHA/catalog hash: canonical linked graph equals the static artifact, every card family visibly exposes eligible alternatives, each selection opens/saves the correct occurrence and the audit has zero self/dangling/asymmetric/false/same-slot-as-alternative defects.
- [ ] F18 evidence is bound to this RC SHA: all page families expose both shell placements; mobile shares current WebP/text/service URL or fallback; desktop uses the owner-confirmed explicit image-only and text+URL actions (or another newly approved mode) without native share; final native Windows/macOS matrix, card/clipboard asset hashes, catalog metrics and CDN rollback follow the release manifest.
- [ ] Favorites/calendar RC evidence covers every page-family menu surface and `/moi-sobytiya/` plus `/izbrannoe/` compatibility: first-save/repeat/undo, unique aggregate, deduplicated `N>0` badge, lifecycle/filter rows, cross-tab/device, identity/event merge, logout/account switch, back/cache isolation and degraded Supabase with working ICS.
- [ ] Security review: RLS/grants, auth callback, bearer tokens, admin allowlist, email webhooks, secret exposure, abuse limits.
- [ ] Performance/load: static/CDN, Edge search quota, telemetry ingest, promotion under full catalog size.
- [ ] M4 SEO/GEO gate is complete on this exact RC SHA: all three audit lanes, reconciled ledger, remediation and final acceptance evidence are attached.
- [ ] Limited canary continues through the approved **14-day event-quality stability window** with availability/quality dashboards; GO requires zero critical defects and zero recurrences of closed root causes across the full window.
- [ ] Rollback drill and incident on-call/contact tree executed.
- [ ] Remove root `noindex` only after all launch blockers, including the post-UI-freeze M4 SEO/GEO gate, are signed off.
- [ ] Post-launch 72-hour hypercare, then 14-day review before declaring stable.

### Stage 8 — После публичной презентации

Эта стадия намеренно не является GO-блокером презентации и начинается только
после её успешного завершения/promote. Этот момент становится `T0/D0` для
[десятидневного Telegraph coexistence](../features/static-site-pages/release-plan.md#десятидневный-telegraph-coexistence):
Stage 7 доказывает готовность static root, Stage 8 постепенно переключает outward
links и только на D10 запрещает создание новых event-detail Telegraph pages.

- [ ] Перевести ежедневные анонсы в Telegram и VK на canonical ссылки соответствующих страниц статического сайта вместо прежних целевых страниц.
- [ ] До переключения подтвердить parity ссылок для всех eligible событий, корректные UTM/source attribution, доступность CDN и отсутствие утечки preview/personal secret URL.
- [ ] Выполнить раздельный canary для Telegram и VK, проверить опубликованные сообщения через реальные channel surfaces и сохранить быстрый rollback на предыдущий link target до окончания post-presentation hypercare.
- [ ] Зафиксировать точные `T0` UTC/Europe-Kaliningrad, production SHA/snapshot/build/manifest, `EVENT_PUBLIC_PAGE_MODE=dual`, `TELEGRAPH_EVENT_WRITE_MODE=create_edit` и rollback target; формулировка «через 10 дней» без записи не запускает отсчёт.
- [ ] Выполнить D0/D2/D4/D6 ramp `10%→25%→50%→100%` static outward links только через manifest-aware resolver; сохранить real Telegram/VK/MAX unfurl evidence.
- [ ] Выдержать D7–D9 72-hour soak на 100% static links с нулём broken/release-critical outward errors и полной catalog/revision reconciliation.
- [ ] На D10 переключить `EVENT_PUBLIC_PAGE_MODE=static`, `TELEGRAPH_EVENT_WRITE_MODE=existing_only`; новые event-detail create/recreate attempts после cutoff равны `0`, legacy URL/path не очищаются.
- [ ] Не выключать month/weekend/festival Telegraph producers этим event-detail switch; для них нужен отдельный parity/acceptance/cutover.
- [ ] После стабилизации D-1 писем отдельно исследовать [post-event attendance feedback для повторяющихся событий](../backlog/features/post-event-attendance-feedback/README.md): одно письмо после надёжно завершившегося сохранённого occurrence, green/yellow/red rating, свободный ответ или public-review URL; unique/ambiguous series fail closed.
- [ ] Не считать calendar save согласием на post-event письмо или публичную публикацию; до эксперимента утвердить consent purpose, frequency cap, series/edition idempotency, reply/URL privacy/moderation and withdrawal rules.

### Stage 8A — Пострелизный релиз verified comment facts и «Активно обсуждают»

Эта стадия является отдельным F14 release scope и не расширяет задним числом presentation GO. Канонический план: [Research and minimal implementation plan](../features/event-comment-feedback/probe-plan.md).

- [ ] **Clean-port prerequisite:** exact-SHA Region Talk/F14/current-main audit, `reuse|adapt|reject|defer` matrix, accepted credential/data boundary and validated `region-talk-ydb-funnel-audit` + `event-comment-feedback-pipeline`; never merge the divergent branches wholesale.
- [ ] **Research/collection:** 30 consecutive daily shadow runs over at least 30 current/future events; preserve complete TG/VK post/thread/parent/reply/source-owner metadata; classify permanent/retryable/config fetch failures; no borrowed Region Talk `DISCOVERY1/2`, E2E or S22 production session roles.
- [ ] **BGE/E5 calibration:** closed fact families, separate question/assertion/negative/uncertain/correction/request/past/hearsay prototypes, `reply` and `Q+A` embeddings, event/source-disjoint evaluation, per-family accepted-fact precision `>=0.99` and zero wrong-author/event/question/uncertainty/negation/stale-current failures.
- [ ] **Product go gate:** at least 3 current/future events and at least 2 fact families prove genuinely new decision usefulness; duplicates are no-ops and low yield fails closed without public UI.
- [ ] **PFR-1 typed shadow ledger:** current-main clean implementation, EventSource-derived incremental collector, isolated compact YDB, literal exact-span slots, authority/scope/TTL/retraction state, fact-set hash and truthful product-funnel status; no public UI or Smart Update write.
- [ ] **PFR-2 «Важно знать»:** render only 1–3 active verified facts through a separate static manifest and deterministic attributed templates; changed fact-set hash schedules one checked rebuild; main description, JSON-LD, critical event fields and public source count remain unchanged.
- [ ] **PFR-3 Smart Update experiment:** dedicated verified snapshot ingress bypasses event matching and LLM comment extraction; start in shadow, preserve last-good output, support correction/retraction/expiry and do not route date/time/place/cancellation through this path.
- [ ] **PFR-4 medallion:** after a real 30-day source/age baseline, publish only **«Активно обсуждают»** for qualified human event-bound discussion; exclude official answers, bots, duplicates, crossposts, giveaways/polls/engagement bait and controversy-dominated threads; no ranking effect in the first experiment.
- [ ] Own static preview, privacy/schema checks, accessibility/no-JS/mobile evidence, canary, rollback drill and owner sign-off close this post-release release. Broad discussion carousel, generated summaries, arbitrary facts, ranking boost and «Особо обсуждают» remain deferred.

### Stage 9 — Отдельный пострелизный релиз раздела «Фестивали»

Эта стадия является самостоятельным release scope после первой публичной презентации и имеет собственные UI freeze, RC и evidence; она не расширяет задним числом presentation GO.

- [ ] Закрыть root causes фестивальной очереди: atomic claim/lease, normalized source idempotency, retry/quarantine/stale recovery, видимый backlog/run result и реально существующие live VK/TG/site queue E2E.
- [ ] Запустить универсальный мониторинг как минимум официальных сайтов: source registry/cadence, changed-content fingerprints, bounded Playwright/PDF fetch, LLM-first evidence extraction, model migration/eval, atomic last-good and freshness alerts.
- [ ] Перевести публичную связь со строки `event.festival` на стабильную identity выпуска с merge/rename redirects and versioned static projection.
- [ ] После owner decision опубликовать `/festivali/` и stable edition pages: current/upcoming index + archive, distinct `Фестиваль` card, separately labelled programme-only rows and reverse list of linked events.
- [ ] На event cards/details показать canonical `В рамках фестиваля …` link; медальон не заменяет relationship semantics.
- [ ] Festival-only and relation changes schedule one coalesced standard build; checked artifact, manifest, atomic current pointer, CDN parity, stale/rollback and catalog/link parity pass.
- [ ] После собственного festival UI/UX freeze rerun Playwright/a11y/no-JS and SEO/GEO audits for the new page family. Canonical contract: [static festival release](../features/festivals/static-site-release.md).

### Stage 10 — Важный пострелизный релиз operations dashboard

- [ ] Нормализовать versioned check registry with owner/criticality/expected slot/freshness/SLO/last attempt/last success/last delivery/status/reason/evidence; missing evidence never becomes green.
- [ ] Reconcile event source ingestion, video target publication, promo fulfilment, KPPK/bus transport, static build/promotion/CDN, current image-dedup coverage, event quality, email and runtime/capacity.
- [ ] Ship a protected admin-only read-only dashboard with summary, trends, filters and redacted evidence/incident drill-down; ordinary site auth, `noindex` or a bearer link are not access control.
- [ ] Store only compact current rows and bounded transition history with TTL; do not duplicate raw logs/provider payloads/history or expose PII/secrets/service keys.
- [ ] Add mutation/retry/catch-up/kill-switch controls only as a later separately accepted phase with confirmation, idempotency and immutable audit. Canonical plan: [operations control dashboard](../backlog/features/operations-control-dashboard/README.md).

### Stage 11 — Отдельный пострелизный релиз «Клубы по интересам»

Эта стадия не является GO-блокером первой публичной презентации. Исходные строки
ниже сохранены как requirement provenance, но их research/implementation порядок
частично superseded: identity/projection/pages достигли `origin/main` в
`98180d1e`, rollout — `6b234a52`, production evidence — `6cdae545`. Current
canonical plan: [interest clubs](../features/interest-clubs/README.md) и
[release plan](../features/interest-clubs/release-plan.md). Для closure всё ещё
нужны семь дней observation, freshness, false merge/split, rollback и отдельный
stable-release decision.

- [ ] Создать и рано запушить `feature/interest-clubs-postrelease` от свежего `origin/main`; весь durable research/ADR context сводить в этой ветке, raw evidence держать только в `artifacts/`.
- [ ] Провести read-only аудит явно указанного прошлого и будущего периода и выпустить evidence-backed список `confirmed|probable|rejected|needs_evidence` с точными counts, уникальными датами, cities/venues/sources и event ids/URLs.
- [ ] Зафиксировать taxonomy/golden pack, который отличает клубную identity от M6 linked occurrences, дублей, разовых/многодневных событий, venue/organizer series и фестивалей.
- [ ] Выполнить past→future time-split проверку: identities/policy выводятся на прошлом периоде, затем измеряется распознавание новых/будущих встреч и false merge/split без label leakage.
- [ ] Сравнить deterministic/existing-vector baseline и BGE candidate lane на Kaggle CPU по precision/recall or named proxies, cluster purity, future coverage, runtime/memory/storage; не выбирать BGE production architecture до owner review.
- [ ] После принятого research GO утвердить identity/schema/ownership, freshness/retention, LLM-first verdict, merge/split/redirect и ecological storage ADR.
- [ ] Только затем реализовать shadow incremental pipeline и отдельный static index/detail slice с checked artifact, coalesced rebuild, atomic promotion, last-good/rollback, UI freeze, a11y/no-JS и SEO/GEO evidence.
- [ ] Закрывать feature отдельным RC/canary: zero confirmed false public merge, no dangling relation и новая встреча подтверждённого клуба появляется в принятом freshness SLO.

### Stage 12 — Пострелизные видеогайды «Как быстро найти событие»

Эта стадия не блокирует public launch. Она начинается после стабильного Stage 7,
завершённого D10 event-detail Telegraph cutover и фиксации production
navigation/search/event-action flows. Каноника:
[video guides](../backlog/features/static-site-video-guides/README.md).

- [ ] Зафиксировать production SHA/build/manifest и подтвердить, что показанные navigation/search/event-action flows не находятся в эксперименте или незавершённом rollout.
- [ ] Утвердить первую серию: быстрый путь через `Сегодня`/`Завтра`/`Выходные`, точный поиск по пользовательскому намерению и сохранение выбранного occurrence.
- [ ] Подготовить storyboard/script для каждого guide id: одна задача на ролик, фактические labels/URL/CTA, без обещаний ещё не выпущенных функций.
- [ ] Записать основной mobile vertical master и отдельный desktop вариант там, где путь отличается; показывать только canonical production, не preview/lab.
- [ ] Пройти subtitles/transcript, privacy и accessibility QA: не раскрывать email, токены, private URLs, персональные интересы, admin state или чужие уведомления.
- [ ] Для каждого target (site/Telegram/VK/MAX) получить owner approval, опубликовать принятую версию и сохранить public URL, source/export hashes и production evidence.
- [ ] Проверить воспроизводимость новым пользователем и завести version/review/withdrawal process: изменение UI или критическая неточность снимает старый ролик до проверенной замены.

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
| Homepage briefing H1 (only if `ship`) | static/no-JS first scene; CLS `0`; agreed viewport/feed visibility budgets pass; no ungrounded/stale claim; mobile/reduced-motion manual/static; downstream discovery guardrails do not regress versus categories-first control |
| Event age-rating coverage M5 | `100%` of non-null canonical ratings visible on `100%` of inventoried event-bearing surfaces; `0` invented defaults, confirmed false ratings or unresolved reliable-source/projection losses |
| Linked-occurrence coverage M6 | `100%` of cards for events with eligible siblings visibly expose another date/time or truthful `ещё N`; `0` canonical→export losses, self/dangling/asymmetric links, confirmed false groups, same-slot duplicates shown as alternatives or siblings leaked into `Похожие` |
| SEO/GEO release transparency | `100%` intended public URL classes pass status/canonical/robots/sitemap/structured-visible fact parity; `0` leaked private/preview/bearer URLs and `0` unresolved Critical/High findings across the required three-agent final audit |

## 9. Release evidence pack

Каждый feature task перед integration должен приложить:

- [ ] base/head SHA and `origin/main` reachability;
- [ ] files changed and canonical docs updated;
- [ ] unit/contract/replay/E2E commands with terminal result;
- [ ] preview/canary URL and immutable build/release id;
- [ ] for M4: neutral crawl/render pack, all three raw consultant reports/provider evidence, reconciled finding ledger, fix SHAs and final reruns bound to the promoted RC;
- [ ] production config diff without secrets;
- [ ] DB migration/RLS/grant evidence when applicable;
- [ ] release manifest and catalog parity report;
- [ ] incident regression checklist and public-surface evidence;
- [ ] rollback command/result;
- [ ] for personalization: mapped Gherkin/Playwright results, redacted before/after localStorage, DB assertions/profile snapshot and `cards_to_first_relevant` calculation;
- [ ] for Supabase capacity: current plan/size, top table+index attribution, retention/compaction result, growth forecast and tested storage-band alerts/kill switch;
- [ ] for H1: source/research SHA, explicit owner `ship|defer`; if `ship`, categories-first/static/motion comparison, manifest/fact provenance, viewport/no-JS/reduced-motion/a11y/performance results, downstream discovery evidence and disable/remove rollback;
- [ ] for M5: source/canonical/projection gap ledger, migration/backfill/replay evidence, all-renderer URL/component inventory, five-rating+unknown Playwright screenshots, visible/share/ICS/structured parity and catalog/RC hashes;
- [ ] for M6: canonical/static graph parity ledger, duplicate-vs-linked decisions, all-card renderer inventory, same-day/different-date/no-JS Playwright screenshots, occurrence-specific URL/ICS/favorite/reminder evidence and incident replays;
- [ ] remaining risks, owner and due date.

## 10. Открытые продуктовые и архитектурные решения

1. **Scope — решено:** F1–F13 and F15–F18 remain in the first public presentation scope; F14 verified comment facts/medallion is an explicit post-release release and is not a presentation GO blocker. Staged canaries manage risk but do not silently pull partial F14 code into the first RC.
2. **Personalization storage — решено:** Supabase/Postgres owns current identity/profile/favorites/subscriptions/email control plane; YDB owns analytics/history and the independent comment-feedback sidecar. См. `docs/architecture/personalization-data-ownership.md`.
3. **Identity — решено на product level:** email-only user becomes a Supabase Auth identity through code or link; anonymous profile links automatically/intelligently under eligible personalization consent.
4. **Favorites — решено:** calendar save и favorite — один durable saved-event state; like остаётся отдельным сигналом; email reminder — отдельный explicit transactional opt-in.
5. **Email reminder — решено на product level:** один D-1 (`24h`) reminder для saved event при verified email + consent; остаются implementation decisions по quiet hours/catch-up и общая legal unsubscribe policy.
6. **UI freeze/navigation — направление решено:** финальный sign-off даёт project owner/user; сохраняем одну IA/order/labels, но адаптируем геометрию. Recommended desktop candidate — persistent horizontal header + shallow tag motif; execution task ещё должна выбрать точную branch/SHA + immutable preview baseline.
7. **Transport:** первый provider/city matrix, источник истины, лицензирование/кэширование и acceptable stale window.
8. **Comment feedback — post-release direction fixed:** official typed decision facts first, deterministic «Важно знать» before any Smart Update prose, independently calibrated «Активно обсуждают» second; remaining decisions are YDB/session lane, retention, first fact-family allowlist and thresholds after the 30-day shadow.
9. **ArtKodex:** API/poller owner, task/thread contract, retry/idempotency and structured repair-result schema.
10. **Event-quality window — решено:** 14 consecutive days, `0` new critical defects, `0` recurrence/reopen of closed root causes; остаются availability/publication-freshness numerical SLO.
11. **Related semantics:** strict Gemma acceptance for “Похожие” vs pgvector-only neutral continuation; required precision@K/coverage/diversity thresholds.
12. **Public search-tag governance — решено:** strict fully automated LLM-first accept/merge/reject, no manual process, fail-closed private pending/retry. Implementation/eval task утверждает numerical semantic/result-overlap thresholds и minimum result count against the golden pack.
13. **Personalization production KPI:** golden release gate `<=20` принят; после canary утвердить production percentile/SLO и minimum relevant-supply coverage, не смешивая mature/cold-start/no-supply cohorts.
14. **Retention/ecological budget — требуется понятное product решение:** забывать/анонимизировать ли compact interest profile после `365d` без визита? Короткие technical logs всё равно удаляются раньше; consent/suppression/send-critical evidence живёт по отдельной safety/legal policy.
15. **Festival release:** owner approves `/festivali/<edition-slug>/`, current/upcoming index plus archive, stable edition-id migration, daily changed-only website monitoring/max stale age and separately labelled public programme-only rows.
16. **Service sharing — частично решено:** F18 обязательна; mobile menu/footer share one centrally prerendered card and desktop never invokes native share. PR #44 (`421d6bca`) preliminarily implements the footer with one mobile action and two explicit desktop actions: image-only card or text+URL. The two-action behavior has already been tested natively on Windows and refined from that result, but final selection still requires the recorded Windows rerun plus macOS matrix on the exact RC and owner acceptance. Header/mobile-menu placement is intentionally deferred, V1 uses factual catalog-bound copy, and poster cubes remain future V2.
17. **Homepage «Городской обзор» — preliminary Conditional Go:** release planning includes H1, but the owner has not yet selected it for shipment. First build/compare categories-first, V1 static and V2 semantic-motion prototypes without personalization/runtime LLM; then decide `ship|defer` before F5 UI freeze. Open subdecisions: desktop autoplay vs manual-only, first-session generic copy, available grounded facts/freshness SLA and whether the decorative wide-«о» motif is worth a design-system exception.
18. **Event age rating — решено на product level:** when a confirmed canonical restriction exists it is visible on every concrete-event public representation; unknown is unlabelled and never becomes `0+`. Implementation/evaluation must settle the schema/provenance migration and explicit treatment of conflicting event/programme/door restrictions without weakening the LLM-first fail-closed boundary.
19. **Linked occurrences — решено на product level:** one programme repeated at different confirmed dates/times remains several occurrence-specific canonical events joined by a symmetric relation. Every card visibly exposes eligible alternatives; the complete list stays on detail. Same-slot rows merge, `Похожие` remains separate, and exact compact card geometry is selected during F5 freeze.
20. **Interest clubs — research boundary fixed:** это отдельный post-release release, сначала только read-only каталог прошлого/будущего и owner review. Открыты minimum recurrence/evidence, public index vs detail pages, stale/archive policy, canonical identity owner и BGE-vs-existing-vector choice по measured time-split evidence.

## 11. Следующие отдельные задачи в рекомендуемом порядке

1. **P0 — static release platform:** F1/F13/G1, coalesce race, atomic promotion/rollback/monitoring.
2. **P0 — Smart Update quality stabilization:** G2 plus M5 semantic age-rating reconciliation/backfill and M6 duplicate-vs-linked occurrence semantics/graph audit, регулярные аудиты, incident burn-down, root-cause fixes, closure-grade replay, dashboard/SLO.
3. **P0 — Supabase ecological capacity:** fresh size/budget, compact schema, retention/compaction, growth forecast, alerts and near-cap fail-safe before remote telemetry/email activation.
4. **P0 — vector/search/tag integration:** F2/F3, prod `/poisk/`, whole-catalog sync, save-as-tag curation/novelty/static generation и golden review.
5. **P0 — UI release freeze:** F5 plus clean preview contract, responsive-navigation comparison with the shallow desktop hybrid as default, H1 categories-first/static/semantic-motion prototype comparison and explicit owner `ship|defer`, M5 shared age badge/fact and M6 linked date/time choices across the complete renderer inventory, PR #38 medallion baseline, completed/owner-deferred medallion P0 shortlist and exhaustive SHA-bound Playwright medallion surface inventory/screenshots with zero visual defects, followed by real-device/a11y/owner evidence. If H1 ships it is integrated before freeze; UI/UX is refrozen if any later feature integration changes it.
6. **P1 — identity/telemetry/favorites/calendar/engagement:** F6/F7/F9/F10/F12 plus M3, включая Yandex/manual-email choice, видимый reminder state after save, migration of `/populyarnoe/`/`/popular_posts`/daily/video/static counters to one compact source+site engagement function and removal of the private Astro popularity formula.
7. **P1 — email recommendations/reminders/deliverability:** F4/F8 after identity/consent foundation, включая D-1 scheduler/Postbox E2E, using the accepted storage ADR.
8. **P1 — admin repair loop:** F17 after idempotency/poller contract.
9. **P1 — media quality/share:** F15/F16/F18. Для F18 сначала review/merge предварительного footer slice из PR #44 (`421d6bca`), сохранив доработки после native Windows test; затем аккуратная mobile-menu placement без незапрошенной переделки шапки, Android/iOS + macOS matrices, формализованный Windows rerun на финальном RC, owner desktop choice и production activation; poster-cube concept остаётся future V2.
10. **P2 — transport:** land the validated preliminary slice through PR #37 only after release-UI placement is accepted; separately prototype/accept-or-defer the optional «Как добраться» gallery slide; F11 becomes production-safe only after the nightly source pipeline/atomic last-good gate is closed.
11. **Post-release — verified comment facts + medallion:** F14 follows [the canonical research/minimal implementation plan](../features/event-comment-feedback/probe-plan.md): Region Talk clean-port boundary → 30-day daily authority/Q-A/BGE+E5 shadow → typed YDB ledger → deterministic «Важно знать» → Smart Update shadow ingress → separately calibrated «Активно обсуждают». It is not required before presentation GO.
12. **P0 final gate — SEO/GEO:** only after presentation-scope steps 1–10 are integrated and the resulting UI/UX is immutably owner-accepted, M4 runs Codex + approved `agy` Gemini Pro + `a-opus` independent audits over crawl/index/schema/internal-link/performance and AI transparency/citability, then re-reviews the exact final RC after remediation.
13. **Post-release — clubs by interest:** create `feature/interest-clubs-postrelease` from fresh `origin/main`, execute the [read-only catalog research prompt](../backlog/features/interest-clubs/research-prompt.md), review the exact club list/counts and time-split/BGE comparison, then decide technology and implementation scope. Do not start public pages or automation from an assumed club count.

Шаги 1–10 и 12 остаются presentation blockers. Шаги 11 и 13 — явные post-release streams и не блокируют GO.

После presentation GO идут самостоятельные важные релизы: **PR-F14** по verified comment facts/medallion, **PR-FEST** по [static festival section](../features/festivals/static-site-release.md), **PR-OPS** по [operations control dashboard](../backlog/features/operations-control-dashboard/README.md) и research-first **PR-CLUBS** по [interest clubs](../backlog/features/interest-clubs/README.md). Минимальный operator scorecard из Stage 2 при этом остаётся текущим release blocker.

## 12. Closure checklist этого аудита

| ID | Результат аудита | Статус |
|---|---|---|
| G1 | Надёжность разложена на build/promotion/rollback/SLO/security/canary gates | **Done (planning only)** |
| G2 | Активные incident families, Smart Update ownership и регулярный audit/incident/root-cause/replay workflow перечислены | **Done (planning only)** |
| F1–F18 | Каждое исходное требование имеет отдельный статус, evidence class и gate | **Done (planning only)** |
| F18 service share | Contract and release gates documented; preliminary footer-only component/controller, mobile share, explicit desktop image/text copy, metric-bound renderer/assets/lab, browser evidence and owner-confirmed native Windows refinement recorded from PR #44 (`421d6bca`) | **Partial implementation; header/mobile-menu, formal Windows RC rerun, macOS/mobile matrices, Pharmastaff reference, main merge and production activation missing** |
| H1 homepage briefing | Research/consultation branch, canonical contract, Conditional Go boundary, V1/V2 experiment gates and mandatory pre-freeze `ship|defer` decision are routed without falsely making the unimplemented candidate a release blocker | **Done (planning only; implementation/preview/owner decision missing)** |
| M5 event age rating | Canonical/LLM-first semantics, source audit/backfill, all-surface UI/derivative inventory, no-default policy, Playwright/check-preview and RC coverage gates documented | **Done (planning only; canonical field/projection/UI/audit missing)** |
| M6 linked occurrences | Existing core/Telegraph behavior audited; canonical static projection, duplicate-vs-linked semantics, all-card alternative date/time UI, occurrence-specific actions, graph parity and Playwright/RC gates documented | **Done (planning only; static export/card UI/full audit missing)** |
| F9 navigation | `Моё` / `Мои события`, unique calendar counter, upcoming-union badge, calendar/favorite filters, complete lifecycle page and privacy/E2E gates documented | **Done (planning only; live Supabase owner/RPC/aggregate and UI implementation missing)** |
| F11 refresh | KPPK/bus provider jobs, combined atomic manifest, provider last-good and single rebuild contract documented | **Done (planning only; implementation missing)** |
| F14 post-release | Live-canary evidence, research work packages, minimum typed-fact implementation, Smart Update shadow boundary, «Активно обсуждают» calibration and separate Stage 8A routed | **Done (planning only; 30-day shadow/implementation/RC missing; not presentation blocker)** |
| PR-FEST | Separate festival release has R01–R06 scope, stages, decisions, evidence and canonical routing | **Done (planning only; release missing)** |
| PR-OPS | Pre-release scorecard boundary and separate protected dashboard release documented | **Done (planning only; implementation missing)** |
| PR-CLUBS | Separate research-first release, exact branch handoff, club taxonomy, evidence/count/time-split audit, BGE comparison and production stop gate documented | **Done (planning only; research/catalog/technology decision/implementation missing)** |
| Документация | Main docs и side-branch feature homes разведены; gaps перечислены | **Done (planning only)** |
| M4 SEO/GEO | Post-UI/UX sequence, three independent audit lanes, detailed scope, synthesis and acceptance gates documented | **Done (planning only)** |
| Реализация | В этой задаче намеренно не менялась | **Not in scope** |
| Public release | Не разрешён данным аудитом | **Blocked / NO-GO** |
