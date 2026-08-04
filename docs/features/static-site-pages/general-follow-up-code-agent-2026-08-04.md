# Генеральная доработка статического сайта — контроль результата и follow-up для кодового агента

Дата аудита: **2026-08-04**  
Репозиторий: `onedayonemasterpiece/events-bot-new`  
Проверенный baseline: `main@ccafa55a2c23e4691738bf2aefc2e5384892668b`  
Назначение документа: **исполняемый prompt/handoff для продолжения работы**, а не ещё один проектный обзор.

## 1. Цель следующей итерации

Не переписывать уже сделанную интеграцию и не строить новый «универсальный фреймворк». Нужно:

1. доказать фактическое состояние Smart Update и публичного статического артефакта;
2. исправить несоответствие каталога/навигации подборок каноническим требованиям;
3. реализовать минимальный anonymous-first путь фокус-группы, который сейчас существует только в документации;
4. закрыть практические hosted/live-gates транспорта и Yandex-деградации без лишних OTP-писем;
5. довести до безопасных операторских gates уже подготовленные YDB и Weather контуры;
6. честно закрыть или пометить superseded старые PR.

Работать от свежего `origin/main` в чистых worktree. Старые ветки не сливать целиком: сначала сравнить их с текущим `main`, затем переносить только отсутствующие актуальные изменения.

## 2. Нормативные источники

Главные документы и evidence:

- `.codex/integration/static-site-unified-20260803/INTEGRATION_REPORT.md`;
- `docs/features/static-site-pages/podborki.md`;
- `docs/features/static-site-pages/gastronomy-collection.md`;
- `docs/features/static-site-pages/personalizaion/personalization-to-be.md`;
- `docs/features/static-site-pages/personalizaion/personalization-research-traceability.md`;
- `docs/features/static-site-pages/personalizaion/implementation-status.yml`;
- `docs/features/static-site-pages/focus-group-release/status.md`;
- `docs/testing/static-site-auth-session-fixture.md`;
- `docs/operations/static-site-autotest-strategy.md`;
- `docs/testing/static-site-autotest-scenarios.v1.yml`;
- `docs/operations/yandex-dependency-resilience.md`;
- `docs/features/static-site-pages/weather-calendar.md`;
- `docs/reports/incidents/INC-2026-08-03-static-site-builder-failure-storm.md`;
- `docs/reports/incidents/INC-2026-08-03-ydb-request-unit-billing.md`.

Связанные PR:

- основной merge: `events-bot-new#316`;
- recovery starvation fix: `events-bot-new#321`;
- незавершённый production-аудит Smart Update: `events-bot-new#322`;
- gastronomy data-prep, который не попал в основной merge: `events-bot-new#314`;
- weather producer: `onedayonemasterpiece/cat-weather-new#4`;
- старые контрактные PR для disposition: `#250`, `#270`, `#287`, `#295`.

## 3. Итог аудита: что принято, а что нет

| Контур | Вердикт | Фактическое состояние |
|---|---|---|
| Восстановление StaticSiteBuilder | `ACCEPTED_WITH_FOLLOWUP` | Exact-main candidate и browser gates были получены; starvation recovery исправлен в `#321`. Но свежая стабильность за сутки не доказана. |
| Общий page runtime | `ACCEPTED` | Generated inventory подтверждает единый runtime на 392 eligible HTML-поверхностях с явными исключениями. |
| Double click / double tap → like | `ACCEPTED` | Desktop, touch, dynamic cards, drag, nested controls и keyboard arbitration покрыты browser test. |
| VK-блок «Остались вопросы?» | `ACCEPTED` | Resolver, fail-closed provenance, desktop/mobile placement и rebuild trigger реализованы. Нужна только обычная production-наблюдаемость после реальной публикации. |
| P13N-00 | `ACCEPTED` | Legacy изолирован, runtime marker и route inventory добавлены без изменения production behavior, БД и remote writes. Следующие волны намеренно не начаты. |
| Standard onboarding seam | `ACCEPTED_AS_SEAM_ONLY` | На общих страницах есть inert typed placement. Это не означает, что полный onboarding, артефакты, клуб и розыгрыш уже включены. |
| Auth session fixture и локальная fault matrix | `ACCEPTED_LOCALLY` | No-mail fixture и deterministic direct/relay tests реализованы. Hosted browser/mobile и реальный product outbox ещё не доказаны. |
| Подборки | `NOT_ACCEPTED_AS_DONE` | Есть базовый registry/catalog, но он неполный, расходится с `podborki.md`, а часть меню обходит registry. Gastronomy фактически заблокирована без approved data. |
| Фокус-группа anonymous-first | `NOT_IMPLEMENTED` | Канонический контракт есть, но текущий `FocusGroupLabPanel.astro` всё ещё требует session/login перед feedback. |
| Yandex resilience | `PARTIAL` | Контракт и локальные тесты есть. Hosted/provider/OAuth cells и capability-specific diagnostic acceptance не закрыты. |
| Weather | `PARTIAL_DEFAULT_OFF` | Consumer и SVG-набор готовы; producer остаётся draft, live binding/provider/bucket и 7-day canary не выполнены. |
| YDB compaction | `PARTIAL_DEFAULT_OFF` | Typed bounded queue/read model и tests готовы; live YQL/server RU, alert, scheduler slot и 24-hour observation отсутствуют. |
| Публичная активация статического сайта | `UNPROVEN` | Acceptance candidate создан, но публичный root и stable ICS намеренно не промотировались. Последующая фактическая promotion/version не подтверждена. |

## 4. Главные обнаруженные разрывы

### 4.1. Smart Update: workflow green не равен production health

PR `#322` подготовил read-only аудит, но production probe был **skipped**, потому что `FLY_API_TOKEN` был пуст. Значит:

- production SQLite и runtime logs не читались;
- состояние Smart Update за сутки не классифицировано;
- нельзя утверждать, что генератор сейчас стабилен;
- нельзя утверждать, что merged site changes уже попали в публичный root.

### 4.2. `R2 Collections` был закрыт преждевременно

`site/src/data/static-collection-registry.json` содержит только девять ключей:

`free`, `exhibitions`, `festivals`, `popular`, `gastronomy`, `unusual`, `cinema`, `kids`, `guests`.

Канонический документ требует учитывать существенно больше контуров, в том числе:

`clubs`, `for_me`, `theatre`, `performances`, `science_pop`, `science`, `strong_impressions`, `russian_guests`, `foreign_guests`, `medieval`.

Дополнительные фактические проблемы:

1. `free` отмечен как `repair`, но мобильное главное меню содержит **безусловную** ссылку `/podborki/besplatnye-sobytiya/`.
2. Проверенного Astro route по ожидаемому пути в текущем source tree не найдено. Это должно быть подтверждено build-route test, а не предположением.
3. `clubs` находится в меню без registry-проекции и без общего six-month activity publication rule.
4. `kids` присутствует как blocked search URL, хотя канонический документ говорит, что детская подборка реализована под другим названием; состояние не сверено с фактическим route/data contract.
5. Тесты проверяют только четыре статуса и использование registry, но не полноту канонического набора и не существование target routes.
6. Gastronomy UI и lifecycle-код существуют, однако checked data сейчас имеют:
   - `audit_status=incomplete`;
   - пустой `decisions`;
   - `compute_status=blocked`;
   - `publication_status=blocked`;
   - `failure_reason=checked_audit_incomplete`.
7. Lane L5 инспектировал gastronomy data-prep до появления актуального head. Сейчас PR `#314` имеет отдельный содержательный diff, который в `main` не интегрирован.

### 4.3. Фокус-группа противоречит принятому продукту

Принятая модель:

```text
invite / QR
→ сайт, local personalization и feedback доступны без email/Яндекса
→ identity verification нужна для розыгрыша, восстановления и связи
```

Текущий runtime:

```text
score / issue
→ requireSession()
→ при отсутствии session: «подтвердите участие»
```

Это не частный test gap, а продуктовый blocker. Документация прямо помечает silent anonymous session, anonymous feedback и anonymous-to-verified upgrade как missing.

### 4.4. Live-gates были честно отложены, но итоговый отчёт звучит шире фактической готовности

Локальные tests подтверждают архитектуру, но не доказывают:

- hosted target и реальные RLS policies;
- Android/iOS поведение общей transport-системы;
- Yandex OAuth из anonymous focus session;
- no-loss recovery на реальном product outbox;
- server-side YDB RU и billing;
- 7 continuous weather days;
- фактический public root SHA после promotion.

## 5. Обязательная последовательность работ

### P0-A. Завершить read-only аудит Smart Update и статического артефакта

Использовать PR `#322` как временный instrument, **не сливать его в `main`**.

1. Перед запуском выставить актуальное полностью завершённое 24-часовое окно. Не переиспользовать старое окно, если оно уже не соответствует моменту запуска.
2. Добавить short-lived app-scoped `FLY_API_TOKEN` только на время probe.
3. Выполнить read-only сбор:
   - число Smart Update starts/completions/errors;
   - stage/fingerprint clustering ошибок;
   - median/p95 duration;
   - количество imported/updated/deferred событий;
   - очереди `static_site_build` по status/age/trigger;
   - recovery/debounce/coalescing показатели;
   - последний terminal successful build;
   - candidate SHA/build id/object count;
   - текущий public root SHA/version и факт promotion;
   - LLM limiter rejects/waits/parallel reservations без секретов;
   - sanitized warning/error excerpts.
4. Сформировать однозначный verdict: `HEALTHY`, `DEGRADED` или `UNHEALTHY`. Зеленый Actions job без production probe не считается PASS.
5. Проверить, что raw DB/logs/token/PII не вышли из Fly.
6. После сохранения evidence удалить временный workflow/payload и закрыть `#322` как `completed-not-merged`.
7. Не выполнять root promotion автоматически. Если новый candidate готов, отдельно отдать owner gate с exact SHA, route count, browser gates и diff summary.

**Acceptance:** есть датированный отчёт с фактическим production verdict и точной связкой `main SHA → builder release → candidate → public root`.

### P0-B. Привести подборки, меню и routes к одному источнику истины

Сделать отдельный PR от свежего `main`.

#### B1. Полный machine-readable registry

Один checked registry должен перечислять **все** канонические collection keys, даже если они `blocked`, `deferred` или относятся к отдельному track. Минимальные поля:

```yaml
key: string
product_status: public | repair | blocked | deferred | external_track
route_status: emitted | missing | intentionally_absent
path: string | null
data_status: ready | incomplete | stale | unavailable | not_started
catalog: boolean
navigation: boolean
sitemap: boolean
last_evaluated_at: ISO-8601
blockers: [stable_code]
source_contract: string | null
```

Не обязательно вводить сложную новую платформу. Допустим additive `v2` либо отдельный readiness projection, но каталог, меню и sitemap должны потреблять один authoritative result.

Обязательные ключи:

- `free`;
- `kids`;
- `clubs`;
- `festivals`;
- `exhibitions`;
- `popular`;
- `unusual`;
- `for_me`;
- `cinema`;
- `theatre`;
- `performances`;
- `science_pop`;
- `science`;
- `strong_impressions`;
- `russian_guests`;
- `foreign_guests`;
- `medieval`;
- `gastronomy`.

Семь типов festival pages пометить `external_track`; не строить их в этом PR.

#### B2. Route integrity

Добавить build-time test, который после реальной Astro build проверяет:

- каждый `catalog=true`/`navigation=true` path разрешается в реально сгенерированный HTML route;
- blocked/deferred entries не создают кликабельных ссылок;
- нет безусловных collection links, обходящих registry;
- sitemap содержит только `public + emitted`;
- preview-prefix не ломает route matching;
- canonical path и generated path совпадают.

Исправить `Reference4MobileMenu.astro`:

- `free` не должен быть безусловной ссылкой;
- `clubs` должен получать publication state из registry;
- secondary collection menu и main menu используют одинаковую политику.

Для `free` принять одно простое решение по факту build:

- если рабочая страница уже есть под другим canonical route — исправить registry/menu на неё;
- если route отсутствует — собрать минимальную listing page из существующего normalized admission без нового LLM extraction;
- если data contract недостаточен — убрать кликабельную ссылку и оставить честный `repair` до отдельного исправления.

Для `kids` найти фактическое название/route и синхронизировать его с registry. Не подменять отдельную детскую страницу query-string ссылкой без доказательства, что это и есть принятый продукт.

Для `clubs` добавить простое правило публикации: показывать клуб, если `last_observed_date` находится в пределах последних шести месяцев относительно build date. Источник — существующая projection, без сканирования внешних сайтов. Неактивные клубы исключаются из публичного каталога предсказуемо; данные не удаляются.

#### B3. Gastronomy

Не считать заблокированный scaffold готовой подборкой.

1. Сравнить PR `#314` с текущим `main`.
2. Перенести только отсутствующий актуальный data-prep contract: общий BGE head, candidate queue, exact source-bound review, owner decisions, family dedupe и fail-closed batch projection.
3. Не сливать старую ветку целиком и не дублировать уже существующий Astro lifecycle.
4. На свежем полном production snapshot выполнить один общий collection batch и получить реальную gastronomy candidate queue.
5. Сформировать bounded owner-review artifact с positive, hard-negative и boundary families. Агент не выдумывает owner decisions.
6. До завершения review оставить page `noindex` и `publication_status=blocked/shadow`.
7. После owner-approved decisions сформировать exact-ID manifest, проверить family dedupe, cold/warm `provider_calls=0`, recent-six-month и last-good behavior.

**Acceptance:** registry покрывает полный канонический набор; каждый видимый link существует; navigation/catalog/sitemap не расходятся; gastronomy имеет реальную review queue либо честный owner blocker, а не пустой «готовый» scaffold.

### P0-C. Реализовать минимальный anonymous-first focus path

Не пытаться в одном PR закончить весь розыгрыш, 12 артефактов и автоматическое определение победителя. Закрыть главный противоречащий продукту путь.

#### C1. Anonymous subject

1. После валидного invite и при активном focus marker вызвать `ensureFocusAnonymousSession()`.
2. Reuse одной Supabase anonymous session при reload/reinvite/PWA relaunch.
3. В Auth state различать:
   - `anonymous_focus`;
   - `verified`;
   - `signed_out_no_subject`;
   - `pending`;
   - `error`.
4. Anonymous session не должна отображаться как `Вошли как ID …`.
5. При временной недоступности Supabase сайт и local personalization остаются доступны.

#### C2. Feedback

1. Page score и issue/text работают под anonymous `auth.uid()` и RLS.
2. Screenshot использует private owner-scoped path.
3. Action, созданное до session/transport, остаётся в bounded outbox и отправляется ровно один раз после recovery.
4. Anonymous participant всегда имеет `raffle_eligibility=false`.
5. Исправить deadline: фиксированный `2026-08-31 18:00 Europe/Kaliningrad`, а не rolling 30 days.
6. Добавить один rendered route-matrix test: Lab panel ровно один раз после main content и до footer.

#### C3. Upgrade boundary

В этом же PR либо в непосредственно следующем bounded PR доказать хотя бы один путь:

```text
anonymous focus user
→ email или Yandex linkIdentity
→ тот же subject/data сохранены
→ verified participant может пройти eligibility gate
```

Если existing-account merge требует отдельной миграции, вынести его в отдельный PR, но не выдавать простой новый login user за completed upgrade.

Реальные OTP-письма не использовать для обычных тестов. `session_fixture` остаётся default. Один real-mail run допустим только если изменён сам OTP issue/delivery/mobile-input path.

**Acceptance:** неподтверждённый участник по invite может отправить score и issue без login prompt; reload сохраняет subject; offline/recovery создаёт одну server row; eligibility остаётся false до verification.

### P1-D. Hosted transport и Yandex degradation acceptance

На immutable hosted target, с no-mail session fixture, выполнить критическую матрицу для Auth/Data, Search, focus feedback и saved/personalization actions:

- normal;
- direct unavailable / relay available;
- relay unavailable / direct available;
- both unavailable;
- shared Supabase upstream unavailable;
- body/decode ambiguity;
- recovery after local queued action.

Обязательные инварианты:

- `selected-once dispatch <= 1`;
- при both-down dispatch `0`;
- idempotent effect `=1`;
- никакого false success;
- local-first UI переживает reload;
- sanitized receipt фиксирует реально активированный fault;
- OTP/mail issue/send/receipt `0/0/0`, пока не тестируется OTP transport;
- diagnostic page говорит о capabilities, а не просто «Яндекс не работает»;
- при доступном direct и недоступном Yandex relay итог вроде `CORE_AVAILABLE_DIRECT_YANDEX_DEGRADED`, а не полный outage.

Отдельно выполнить bounded Yandex OAuth round-trip из anonymous focus session после готовности C3. Не включать YDB, Postbox и Object Storage в один общий «Yandex status».

### P1-E. Остаточное hardening StaticSiteBuilder

После production-аудита, а не вместо него:

1. Протянуть bounded command-output tail в terminal builder status для диагностики раннего deterministic failure.
2. Добавить deterministic-failure circuit breaker для повторов с тем же `source SHA + failure fingerprint`, сохранив один coalesced pending build после изменения inputs.
3. Не выключать обычные Smart Update events.
4. Покрыть recovery/debounce/calendar rollover regression tests.

### P2-F. Weather producer и canary

Работа ведётся в `onedayonemasterpiece/cat-weather-new` и затем в consumer repo.

1. Проверить и обновить draft PR `cat-weather-new#4` от текущего `main`.
2. Не менять существующий Telegram weather product побочно.
3. Закрыть owner/operator gates:
   - точная production location binding и hashes;
   - одобренный provider usage plan;
   - versioned bucket/IAM/conditional writes;
   - single-writer lease;
   - non-public pointer-last smoke.
4. Запустить семь непрерывных дней наблюдения, включая выходной.
5. Собирать freshness, gaps, provider calls, retries, pointer conflicts, object/readback hashes.
6. Consumer flag в `events-bot-new` остаётся `0` до PASS.
7. После PASS выполнить отдельный bounded consumer canary на date/weekend surfaces и проверить browser cache/CLS/no-third-party-requests.

SVGRepo-набор и consumer UI уже сделаны; не пересобирать их без обнаруженной ошибки.

### P2-G. YDB compact data plane — live gates

Не переписывать typed queue/read model. Закрыть только честно оставшиеся пункты:

1. Проверить complete-producer coverage всех обязательных writers/finalizers; partial generation не может стать current.
2. Выполнить read-only/live YQL validation и получить server-side RU, а не только client ledger floor.
3. Настроить billing alert и auto-abort budget.
4. Подтвердить exact account/database guard.
5. Cutover:
   - freeze writers;
   - watermark/count/hash;
   - migration;
   - parity;
   - read-only canary;
   - manual no-publish run;
   - один scheduled slot;
   - 24-hour observation.
6. Scheduler/catch-up и RU throttle не включать до всех предыдущих PASS.
7. Большие тексты/векторы не возвращать в горячий KV.

Отчёт обязан содержать фактические queries/rows/bytes/server RU и стоимость canary.

## 6. PR hygiene

Перед финальным отчётом:

- `#287`, `#295`, `#270`, `#250`: сравнить с текущим `main`; если содержательная часть уже интегрирована через `#316`, закрыть как `superseded by #316` с перечислением сохранённых residual gates;
- `#314`: не закрывать до переноса актуального gastronomy data-prep либо явного replacement PR;
- `#322`: временный audit PR не сливать; закрыть после evidence и удаления workflow;
- `cat-weather-new#4`: оставить draft до operator/live gates;
- не закрывать PR только потому, что он старый; сначала дать exact compare/disposition.

## 7. Что не входит в эту итерацию

- P13N-01…P13N-06 и изменение модели персонализации;
- новые веса, remote profile writes или новая персонализированная `/dlya-menya/` generation scheme;
- семь визуальных типов festival pages;
- editorial collections и их UI/нарратив;
- полный raffle/draw/artifact programme;
- автоматическая public root promotion без отдельного owner gate;
- новый LLM extraction для `free`, если существующий admission contract достаточен;
- переписывание transport layer, если hosted failures закрываются точечными исправлениями.

## 8. Рекомендуемое разбиение PR

1. **PR-A — production health evidence and disposition**  
   Read-only audit, version chain, cleanup `#322`, без runtime mutation.
2. **PR-B — collection registry truth and route integrity**  
   Полный registry, menu/catalog/sitemap, free/kids/clubs reconciliation, route tests.
3. **PR-C — gastronomy data-prep integration**  
   Актуальная часть `#314`, production candidate queue, owner-review artifact.
4. **PR-D — focus anonymous MVP**  
   Anonymous session, feedback/outbox, eligibility false, fixed deadline, browser E2E.
5. **PR-E — hosted transport/Yandex acceptance**  
   Реальные fault cells и точечные runtime fixes.
6. **PR-F/G — Weather и YDB live gates**  
   Отдельные репозитории/операторские evidence; default-off до PASS.
7. **PR-H — builder residual hardening**  
   Output tail и deterministic circuit breaker после аудита.

`EventLayout.astro`, `docs/routes.yml`, global navigation и `CHANGELOG.md` изменять последовательно, чтобы не потерять независимые workstreams.

## 9. Обязательные проверки

Минимальный общий набор перед каждым merge:

- relevant unit/contract tests;
- real Astro production-profile build;
- generated route integrity;
- page runtime inventory;
- Playwright desktop `1440×900` и mobile `390×844` для затронутых UI;
- no horizontal overflow;
- no duplicate runtime/handlers;
- preview/base-path route checks;
- OTP/mail counters;
- sanitized network trace при transport changes;
- exact SHA и explicit list of live actions;
- `git diff --check`.

Нельзя ставить `Done` только на основании source assertion или mocked unit test, когда requirement явно требует hosted/live evidence.

## 10. Формат итогового отчёта кодового агента

Итог должен содержать:

1. таблицу `Done / Partial / Blocked / Owner gate` по разделам этого документа;
2. ветки, PR, commit SHA и merge disposition;
3. выполненные команды и результаты тестов;
4. точное количество real OTP/mail sends;
5. точное количество YDB live reads/writes/DDL/scheduler changes;
6. public root/candidate/build SHA и факт promotion;
7. ссылки на sanitized evidence artifacts;
8. список оставшихся gates без формулировок «в целом готово»;
9. подтверждение, что старые PR не были слиты целиком без compare.

Главный критерий завершения: пользовательская и операторская правда совпадает с labels в документации, registry и UI; заблокированный или непроверенный контур не выглядит опубликованным и готовым.