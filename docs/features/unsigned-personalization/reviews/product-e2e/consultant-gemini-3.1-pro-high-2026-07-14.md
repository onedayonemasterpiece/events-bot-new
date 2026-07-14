# Gemini 3.1 Pro (High): critical product-E2E review

> **Gate status:** `ACCEPTED_AS_EXTERNAL_CONSULTANT_INPUT` — review **1 / 2**.
>
> Это первый provenance-qualified Gemini Pro-class input для будущего
> `reviews/product-e2e/synthesis.md`. Его рекомендации остаются consultant
> proposals и не становятся project decisions до второго eligible review и
> явного `accept/adapt/reject/defer` synthesis.

## Project-side provenance

| Поле | Значение |
|---|---|
| Дата | 2026-07-14 UTC |
| Repository / branch | `onedayonemasterpiece/events-bot-new` / `feature/personalization-product-e2e-design` |
| Reviewed HEAD | `bc4e1b2e6db5c8ed47f1c081d15217c357c75d28` |
| Comparison base | `492497fe1dfc8db717dd0bcca67686c61c77f0ff` |
| Provider route | Google Antigravity CLI `agy` 1.1.2 |
| Exposed model surface | `Gemini 3.1 Pro (High)` |
| Underlying API/deployment ID | `NOT EXPOSED` by Antigravity |
| Evidence delivery | 14-pack line-numbered capsule, `294468` counted input tokens, delivered inline in 17 ordered turns within one conversation |
| Conversation | `d0bd946e-7436-4002-8187-e7381fff87d7` |
| Receipt validation | exact `14 / 14`, no missing/extra receipts |
| Dynamic operations by consultant | none: no tests, browser, production API or DB calls |

The exposed surface is accepted under the project policy's eligible Gemini
3.1 Pro **ID/class** clause. The hidden deployment identifier is not invented.
A separate direct Gemini API probe addressed exact
`gemini-3.1-pro-preview`, but generation returned HTTP `429
RESOURCE_EXHAUSTED` before inference because that project had zero Pro free-tier
generation quota; that failed route did not produce this review.

The deterministic capsule covered governance/design, all 12 personas, both JSON
Schemas, strict real-data and holdout rules, ownership/runtime docs, Astro and
reference JS, Playwright/Gherkin, Supabase migrations/function/vector sync, a
bounded personalization-YDB search, old intake proposals, historical diff and
current primary Supabase/YDB documentation excerpts. Bulky captures remain in
ignored operational artifacts:

```text
artifacts/codex/personalization-product-e2e-consultants/
  full-gemini-audit-bc4e1b2e/
```

| Artifact | SHA-256 |
|---|---|
| `manifest.json` | `e2d3be8a46b60ff782a91dab9e3ea9b08440702e71725a4d3c38aa94676b830a` |
| `direct-api-audit-prompt.md` | `ae549fed66fa58cae87b446971c146676bce7b1c222bc815e71a077334ec090d` |
| `final-review.md` (raw capture) | `b92d3e2051e750b4602424231cc394331fc873969bd4ad39ff0fac366350f713` |
| `final-review-validation.json` | `1db1fd4f2e8c2af90ca564d94aa3f3efb0a1434862f9fef22617cfbcaa7691e5` |
| Committed normalized review body | `8178c1caabf6a9470935f0f70bb31e30f2d27e83246e6035f3fb31ca303deac9` |

Acceptance validation confirmed 14 exact receipts, 12 required sections, 20
severity findings, 18 decision tokens and no out-of-range repository citations.
The model output below preserves the final citation and architecture correction
turn content; trailing whitespace alone is normalized for repository hygiene.

---

Capsule receipt ledger:
```text
00-governance.md | RECEIPT-E7762B9BCA78697195B8F988
01-design.md | RECEIPT-666BCFAC791DBAC91C56D1EE
02-personas-real-data-db.md | RECEIPT-47CA4D1AE36B1DB02F71AD55
03a-ownership-database.md | RECEIPT-D1ACBD23696DA56B262282B6
03b-runtime-docs.md | RECEIPT-3B8BFD67C5D29D51C0BC03D6
04a-astro-feed.md | RECEIPT-181B188B85DC6FC3BEA0ABDD
04b-astro-actions-search.md | RECEIPT-096B6B03B3C2F367EA39F692
05a-reference-client.md | RECEIPT-45BFEACDA9E4054D37D44CE3
05b-tests.md | RECEIPT-A3A66089562793BA7BFD6664
06-db-runtime.md | RECEIPT-B53CB4F44B48A34D20864BE8
07a-old-intake-methods.md | RECEIPT-BD6B9E56E34CDEBAE8D86918
07b-old-intake-kpi-time.md | RECEIPT-68F58F8E527F809AF278985D
07c-old-intake-loop-roadmap.md | RECEIPT-D6A67C4C0F5FF0CCF650D100
08-diff.md | RECEIPT-5208FA94F655D2208D5E9209
```

# 1. Provenance and evidence ledger

*   **Repository:** `onedayonemasterpiece/events-bot-new`
*   **Branch:** `feature/personalization-product-e2e-design`
*   **Exact reviewed HEAD:** `bc4e1b2e6db5c8ed47f1c081d15217c357c75d28`
*   **Historical base:** `492497fe1dfc8db717dd0bcca67686c61c77f0ff`
*   **Model Source:** Ответ сгенерирован через Google Antigravity/agy. Visible model surface — `Gemini 3.1 Pro (High)`. Underlying API/deployment ID не раскрыт. Отдельная попытка вызова exact API model `gemini-3.1-pro-preview` была прервана (HTTP 429) до начала inference и не произвела данного ревью.
*   **Limitations:** Выводы сделаны исключительно из предоставленного статического evidence capsule (файлы, схемы, diffs). Я не выполнял tool execution в runtime, не подключался к YDB/Supabase и не запускал Playwright/tests. Выявленное отсутствие реализаций подтверждено bounded search внутри капсулы.

# 2. Executive verdict

*   **Design synthesis:** **READY FOR SECOND CONSULTANT**. Текущие артефакты достаточны для кросс-ревью и последующего синтеза, но не готовы к фазе имплементации.
*   **First implementation slice:** **NOT READY**. Ключевой ingestion/rollup слой и механизм сборки доказательств отсутствуют.
*   **Longitudinal evaluator:** **NOT READY**. Описаны схемы контрактов, но simulator harness для генерации E2E сессий отсутствует.
*   **Browser E2E:** **NOT READY**. Существующий Playwright проверяет reference fixture, а не собранный UI Astro-компонент.
*   **Canary / Production:** **NOT READY**. Блокируется отсутствием исполняемого Database Sustainability Gate и нереализованностью server-side cleanup limits.

# 3. Top critical findings

1.  **P0** `tests/playwright/static_personalization_contract.spec.ts:5-6` — Браузерный E2E-тест исполняет статический `demo.html`, а не собранный Astro-сайт. Longitudinal E2E validation coverage для production-билдов равен нулю.
2.  **P0** `product-e2e-design.md:53-59` — Архитектура полагается на Ingest и Snapshotting, однако миграции базы данных для `telemetry_quarantine` и профилей *NOT FOUND IN BOUNDED CAPSULE SEARCH* (отсутствие подтверждено также в `database-sustainability-e2e.md:22-30`).
3.  **P0** `EventLayout.astro:2094-2103,2584-2618` vs `product-e2e-design.md:71` — Баннер consent декларирует строго локальное хранение preferences, но дизайн требует отправки снимков профиля на сервер. Истинный privacy-режим противоречив.
4.  **P0** `EventLayout.astro:2976-2986` и `AuthorizedEventSearch.astro:525-534` — При переходе из ленты в поиск контекст authorized search теряется, разрывая E2E correlation envelope (`product-e2e-design.md:69-70`). Связь impression и downstream action обрывается.
5.  **P0** `docs/features/unsigned-personalization/schemas/product-e2e/golden-persona-v0.schema.json:51-63,85-108` — Риск evaluator leakage: если весь JSON-фиксюр передать ranker'у, тот может прочитать `age_cohort` и `latent_interests` в обход истории действий.
6.  **P0** `database-sustainability-e2e.md:22-30` — Cleanup и isolation logic для тестовых прогонов (`e2e_run_id`) *NOT FOUND IN BOUNDED CAPSULE SEARCH*. Риск масштабного засорения production Supabase синтетическим мусором.
7.  **P0** `golden-personas-real-data-v0.md:89-98` — 129 из 218 реальных событий не имеют цены. При forward projection без строгой `unknown_fact_policy` это приводит к массивному выпадению supply (silent eligibility fail) для персон с price constraints.
8.  **P1** `tests/e2e/features/static_site_personalization.feature:1-2` — Gherkin-файл остается черновиком (`@draft`), без executable bindings, что подтверждено поиском шагов в `05b-tests.md`.
9.  **P1** `product-e2e-design.md:53` — Заявленный замер `valid_impression` невозможен: *NOT FOUND IN BOUNDED CAPSULE SEARCH* реализация intersection observer и логики видимого dwell.
10. **P1** `docs/features/unsigned-personalization/schemas/product-e2e/golden-persona-v0.schema.json:279-297` — `expected_profile_facets` не разделяют short-term intent и long-term interest. Это блокирует проверку правил старения интересов (decay).
11. **P1** `product-e2e-design.md:72` vs `README.md:92,108` — Противоречащие intake-политики: старые документы разрешают anonymous intake, новые требуют credentialed API.
12. **P1** `database-sustainability-e2e.md:22-30` — Интеграция с YDB (outbox, projections, TTL) остается markdown-концептом и полностью отсутствует в коде.
13. **P1** `product-e2e-design.md:53-59` — Ingest/dedupe реализация *NOT FOUND IN BOUNDED CAPSULE SEARCH*. Будущий контракт обязан включать client action/summary id, run/actor ownership, schema/payload hash и retry idempotency.
14. **P1** `EventLayout.astro:2710-2758,3145-3154` — Served summary существует в памяти браузера/контексте, но remote accepted delivery для него *NOT FOUND IN BOUNDED CAPSULE SEARCH* (см. также `product-e2e-design.md:55-56`).
15. **P2** `golden-personas-real-data-v0.md:31` — Персона P06 стереотипизирована ("avoid late-night") через возрастную когорту. Должно доказываться action-rejection, а не демографическим фильтром.
16. **P2** `product-e2e-design.md:53-59` — Supabase functions реализуют vector search (embeddings), но код генерации персональной ленты с применением профиля (candidate filter) отсутствует.
17. **P2** `database-sustainability-e2e.md:97-107` — Zero Fly mutation описан как gate-требование, но его доказательство в рантайме (read-only query logs, logical ledger size validation до/после) не обеспечено инфраструктурой.
18. **P2** `database-sustainability-e2e.md:112-114` — Point baseline 36 MB преимущественно отражает существующий project/sidecar (включая ~17 MB embeddings), поэтому из него нельзя вывести bytes/action или изолированный telemetry budget. Телеметрические таблицы в нём физически отсутствуют.
19. **P2** `EventLayout.astro:2976-2986,3177-3210` — Action triggers срабатывают исключительно на явные UI clicks.
20. **P2** `catalog-timeline-v0.schema.json:310-314` — `sealed_holdout_releases` привязаны статично к дню. Риск protocol overfitting: при переиспользовании holdout перестанет быть контрольным (`golden-personas-real-data-v0.md:100-118`). Требуется versioned sealed rotation/reseal policy без мутации реальных фактов.

# 4. Implementation truth table

| Component | Status in Capsule | Evidence / Limitations |
| :--- | :--- | :--- |
| **Browser collection** | *Partial* | В памяти есть served context, но dwell/valid_impression *NOT FOUND* (`product-e2e-design.md:53`). |
| **Local profile** | *Implemented* | `personalization.js` + Astro `EventLayout.astro:3177-3210`. |
| **Served context** | *Partial* | Теряется при переходе в поиск (`AuthorizedEventSearch.astro:525-534`). |
| **Ingest / dedupe** | *Missing* | Нет client summary ID, payload hash, actor ownership. |
| **Rollup/snapshot** | *Missing* | Серверная агрегация в профиль *NOT FOUND*. |
| **Profile application**| *Missing* | Personal feed backend `get_listing_personal_feed_v1` отсутствует. |
| **Candidate generation**| *Partial* | Векторный слой есть, filter/ranker с профилем — нет. |
| **Vector search** | *Implemented* | Embeddings/sidecar подтверждены (`sync_event_search_vectors_to_supabase.py`). |
| **Evaluator** | *Missing* | JSON-схемы есть, simulator/engine отсутствует. |
| **Simulator** | *Missing* | Нет headless harness для E2E-сессий. |
| **Real timeline fixture**| *Design-only* | Контракт описан, рантайм валидации нет. |
| **Cleanup / bounds** | *Missing* | SQL для RLS/isolation (`e2e_run_id`) *NOT FOUND*. |
| **YDB projection** | *Missing* | Outbox path отсутствует полностью. |
| **Playwright built-Astro**| *Missing* | Используется `demo.html` (`static_personalization_contract.spec.ts:5-6`). |
| **Executable Gherkin** | *Design-only* | Файл `@draft`, шагов нет (подтверждено ограниченным поиском в `05b-tests.md`). |

# 5. Golden-persona audit

Панель содержит ровно 12 персон. Введение дополнительных (P13) недопустимо без доказательства от второго консультанта.

*   **Decoupling Age from Behavior:** Стереотип P06 (avoid late-night) должен быть оторван от `age_cohort`. P06 и P12 должны совместно перекрыть accessibility axis.
*   **Ranker Visibility State Isolation:** Ranker *не должен* получать raw telemetry или `evaluator_oracle_state`. Ранжировщик работает строго с *authorized derived/accepted action evidence* или profile features. Oracle-факты (истинные скрытые интересы) и полный сырой fixture остаются evaluator-only. Remote storage хранит компактные summaries профиля, а не raw history для нужд ranker'а.

# 6. Strict real-data audit

*   **Runtime Verification:** Наличие `real_data_snapshot: true` в JSON Schema декларирует контракт, но *не гарантирует* аутентичность. Runtime validator обязан программно проверять snapshot fingerprint, record/content hashes, evidence refs и chronology, чтобы исключить подмену фактов.
*   **Synthetic Worlds:** Корректно запрещены. Итоговый симулятор не может генерировать events или вымышленные даты, цены.
*   **Forward Projection:** Имитация течения времени (schedule advance) легальна над замороженной базой.
*   **Unknown Facts:** Слишком большое количество событий (129) без цены требует строгой baseline-политики `unknown_fact_policy`, чтобы отличать absence of data от ranker failure.

# 7. KPI/maturity/statistics decision

| Proposal / Parameter | Decision | Rationale |
| :--- | :--- | :--- |
| **Denominator = Catalog Supply** | *Accept* | Жестко пресекает gaming метрик. Ranker отвечает только за фильтрацию *доступного* relevant supply. |
| **NO_RELEVANT_SUPPLY** | *Accept* | Отдельная категория исхода. Пустая выдача при пустом каталоге — не вина генератора/ранжировщика. |
| **Abandonment = Competing Risk**| *Accept* | Right-censoring недопустим. Abandonment фиксирует исчерпание (exhaustion), а не нейтральный выход. |
| **F20 / F30 Thresholds** | *Adapt* | Использовать как diagnostic ceilings, не финализировать как release gates до сбора consented logs. |
| **Meaningful Action vs Encounter**| *Adapt* | `valid_impression` фиксирует encounter, но доказательство Exploration требует downstream actions (dwell > X, click). |
| **Maturity Thresholds** | *Defer* | Профиль строится исключительно из *accepted actions*. Evaluator отдельно проверяет maturity, но thresholds не хардкодить до evidence-based baseline. |

# 8. Database sustainability audit

Проектные политики установлены: Bands 0.60/0.80 (`database-sustainability-e2e.md:71-79`). Вычисления бюджета должны вестись раздельно для Supabase (telemetry/profile/vector/cache) и YDB (analytics/index/service). Повторных замеров нет, статус: `INSUFFICIENT_BASELINE`.

**Relation-Specific Projection Formulas:**
Для каждого стораджа E2E-гейт обязан считать:
1.  `daily_net_retained_bytes = p95(size_after_cleanup_grace - size_before_run)`
2.  Для D in {30, 90, 365}:
    `projected_D = current_attributed_bytes + max(0, daily_net_retained_bytes) * D + p95(cleanup_lag_bytes) + index_bloat_headroom`
3.  Для TTL-ограниченных пулов: capped retained ingest не превышает `retention_days + grace_days`, но к нему прибавляется измеренный retention debt/indexes.
4.  `headroom_D = projected_D / configured_budget_bytes`
    Гейт падает, если `headroom_D > 0.80`.

**Zero Fly Mutation:** Не следует полагаться только на exact byte-hash `/data/db.sqlite` (размер может меняться из-за operational vacuum/WAL). Требуется аппаратный read-only mode (query-only access) и before/after проверка logical fingerprint, rows size и write-ledger evidence.

# 9. Old-intake reconciliation

| Material Proposal (Old Intake) | Decision | Conflict & Required Diff |
| :--- | :--- | :--- |
| **10-persona panel** | *Reject* | Текущий дизайн применяет 12. P13 не нужна. |
| **Synthetic catalog worlds** | *Reject* | Противоречит strict `real_event_records_only`. |
| **Anonymous intake** | *Adapt* | Конфликт (`README.md:92` vs `product-e2e-design.md:72`). Same-origin rate-limited MVP остается. Gated Supabase append требует credentialed delivery/consent. |
| **F20/F30 hard limits** | *Defer* | Uncalibrated; использовать только для offline probe. |
| **LLM as sole judge** | *Adapt* | LLM применима только для поиска contradictions. |
| **Playwright for ranking calc**| *Reject* | Браузер — это sentinel. Метрики ранжирования считать offline. |
| **Abandonment = right-censor** | *Reject* | Это competing failure risk, не N/A. |
| **Direct profile injection** | *Reject* | Запрещено bypass; профиль — только результат accepted UI действий. |
| **Virtual clock install** | *Adapt* | Использовать injectable virtual clock contract для симулятора, не хардкодить `page.clock.install()`. |
| **Fixed exploration slots** | *Adapt* | Настроить IntraListDiversity penalty без вытеснения primary relevant supply. |
| **Seeded probabilities** | *Defer* | Калибровать только после сбора production distribution. |
| **Maturity = 4 sessions** | *Defer* | Профиль строится action'ами; threshold'ы для зрелости ждут логов. |

# 10. Dependency-ordered first implementation slice

Строгий цикл имплементации (в порядке зависимостей), замыкающий full loop, без канарейки:

1.  **Manifests:** Versioned run/persona/snapshot manifests над frozen real records.
2.  **DB Schema Guard:** Bounded Supabase accepted-summary/served-evidence/quarantine/dedupe/isolation schema с RLS и cleanup.
3.  **Delivery Interface:** Same-origin credentialed/gated delivery queue (с consent checking) и static fallback-интеграция.
4.  **UI & Triggers:** UI actions (likes/hides) + `valid_impression` intersection + persistent served context payload.
5.  **Rollup Engine:** Deterministic серверный rollup/maturity агрегатор, преобразующий очередь в versioned current profile snapshot.
6.  **Next-Feed Application:** Candidate generation / profile application (слияние вектора и профиля) и запись persisted served evidence.
7.  **Evaluator Harness:** Headless 14-day simulator/evaluator с жестким `supply -> candidate -> rank -> presentation` diagnosis.
8.  **Browser Sentinel:** Отдельный built-Astro Playwright sentinel, исполняющий UI actions (сохраняя старый reference suite для регрессии). Assertion БД вынести в service/integration слой.
9.  **Sustainability Gate:** Supabase/YDB conservation / cleanup / 30-90-365 threshold test.

**Exit Criteria:** Доказуемый flow `UI actions -> accepted DB -> rollup -> next feed -> served -> evaluator` + persona product outcome над frozen real data.

# 11. Required documentation/schema/test diffs

*   **Gherkin:** Не удалять. Оставить draft-спецификации как design-документ; bindings добавятся после появления UI seams.
*   **`docs/features/unsigned-personalization/schemas/product-e2e/golden-persona-v0.schema.json`:** Физически разделить JSON-поля. Evaluator-oracle состояние должно быть закрыто. `sealed_holdout_releases` (`catalog-timeline-v0.schema.json:310-314`) обязаны иметь ротацию (versioned reseal policy), не мутируя реальные факты событий.
*   **`database-sustainability-e2e.md`:** Заменить baseline 36MB и некорректные формулы на relation-specific metrics (см. секцию 8) с разделением storage-типов.
*   **`EventLayout.astro`:** Не встраивать прямой direct mutation (remote). Рекомендовать bounded local queue/same-origin delivery, сохраняя static fallback и local-first consent.

# 12. Final blockers, questions and confidence

**Phase B / Canary / Production Blockers:**
*   Не реализован E2E Egress-to-Ingress loop с авторизацией и дедупликацией (versioned manifests).
*   Playwright sentinel проверяет не production build (Astro), а статический `demo.html`.
*   Отсутствует runtime validator подлинности снимков `real_data_snapshot`.
*   Не реализовано разделение памяти ranker (derived profile) и evaluator (oracle truth).

**Questions to Second Consultant:**
1.  Как безопасно ротировать `sealed_holdout_releases` (avoid protocol overfitting), если количество подходящих events в каталоге жестко ограничено ~218 штуками без возможности синтеза?
2.  Можем ли мы усилить `P06`/`P12` новыми unknown fact conditions, не добавляя P13 и не ломая текущую панель из 12 акторов?

**Confidence & Limitations:**
*   **High Confidence:** Обнаруженные бреши в миграциях, E2E таргетинге и смешении state/oracle подтверждены точным чтением капсулы и схемы. Изоляция DB и sustainability requirements жестко выводятся из предоставленных формул.
*   **Medium Confidence:** Оценка пропускной способности local fallback queue vs. credentialed delivery; требует реальных telemetry logs.
*   **Limitation:** Утверждения о YDB projection lag базируются исключительно на markdown-документах, так как implementation (код выгрузки) полностью отсутствует в капсуле.
