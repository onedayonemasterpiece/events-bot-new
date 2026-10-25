# Задача кодовому агенту: восстановить и зафиксировать Умный поиск

> **Приоритет:** P0, production incident.  
> **Текущий verdict:** `INCIDENT OPEN / NO-GO`.  
> **Канонический контракт:** [`README.md`](README.md).  
> **История реализации:** [`../../../unsigned-personalization/authorized-event-search.md`](../../../unsigned-personalization/authorized-event-search.md).  
> **Рабочая ветка:** `integration/smart-search-live-e2e-20260803`, draft PR #284.

## 1. Цель

Восстановить Умный поиск как единый авторизованный продуктовый путь и перевести его на постоянное сквозное автотестирование. Результат должен доказывать работу всей цепочки:

```text
опубликованная /poisk/
→ реальная Supabase-сессия после входа по email или через Яндекс
→ единый ResilientSupabaseTransport
→ один selected-once POST /functions/v1/event-search
→ cache / embedding / pgvector / optional LLM verifier
→ валидные большие EventCard
```

Не создавать параллельные Auth, transport, fault injector, Appium harness, quota ledger или отдельный реестр тестов.

## 2. Что уже доказано

1. На публичном origin `https://kenigevents.ru/poisk/` сейчас **HTTP 404**. Это подтверждённая поверхностная причина недоступности Search для пользователя.
2. В двух immutable transport-fault preview `/poisk/` существует, возвращает `200`, имеет `data-search-enabled=true` и JSON transport:
   - direct Supabase отключён: SHA `3e892bd510818c07b2e14b708db7d5f39e2ae845`;
   - Yandex relay отключён: SHA `592ffb2d5a68615b65481ee1acea65b728af8d8d`.
3. Public probe: GitHub Actions run `30792290043`.
4. Backend Search уже реализован: `gemini-embedding-2`, 768 dimensions → Supabase pgvector → optional Gemini Lite/Gemma verifier.
5. `functions.event-search` уже классифицирован в общем `backendOperationCatalog` как cost-bearing `selected-once`: маршрут выбирается до отправки; неоднозначный timeout не разрешает повтор через второй канал.
6. Focus OTP infrastructure уже терминально доказала оба направления отказа direct/relay на Android и iOS. Её нужно переиспользовать, а не переписывать.

Публичный `404` ещё не доказывает, что backend исправен. После восстановления маршрута требуется живой авторизованный прогон, который локализует возможный второй дефект по стадии.

## 3. Обязательные работы

### P0.1. Восстановить release surface

- Включить `/poisk/` в **предназначенный для текущего релиза** checked static artifact.
- Не публиковать production root в обход действующего immutable candidate / atomic promotion protocol.
- Связать evidence с exact static repo SHA, build ID и target URL.

### P0.2. Реализовать живой сценарий `search.transport_route_matrix`

Расширить существующие:

- `.github/workflows/external-focus-email-otp.yml`;
- `.github/workflows/static-site-qa-command.yml`;
- `site/e2e/focus-email/`;
- `docs/testing/static-site-autotest-scenarios.v1.yml`.

Сценарий использует настоящий вход по email OTP, затем переходит на `/poisk/` того же immutable preview и выполняет:

1. проверку exact SHA и восстановленной авторизованной сессии;
2. один cold Search по закрытому synthetic query ID;
3. проверку единственного `functions.event-search` outcome и появления минимум одной EventCard;
4. повтор того же запроса отдельным явным действием;
5. доказательство `served_from_cache=true` и `result_cache_status=hit`;
6. отсутствие дублирующего Search POST;
7. сохранение только обезличенного evidence.

### P0.3. Матрица транспорта

Прогнать последовательно browser → Android → iOS:

| Профиль | Обязательный результат |
|---|---|
| `normal` | Search работает через один выбранный здоровый маршрут. |
| `client_supabase_direct_unreachable` | Auth, registration и Search идут только через Yandex relay; direct Search outcomes = 0. |
| `client_yandex_relay_unreachable` | Auth, registration и Search идут только через direct Supabase; relay Search outcomes = 0. |
| `both_client_routes_unreachable` | `event-search` не отправляется; provider/pgvector side effects = 0; UI показывает честное degraded-состояние. Отдельный no-mail/no-search сценарий. |

Для каждой явной поисковой команды: **ровно один** cost-bearing POST. Никакого hedged request, rescue POST или автоматического переключения после неоднозначной отправки.

На Android Chrome и iOS Safari дополнительно проверить:

- настоящую системную клавиатуру;
- `enterkeyhint=search`;
- отправку системной клавишей поиска;
- отсутствие повторной отправки из-за Enter/IME/race;
- корректное восстановление UI после ошибки.

### P0.4. Добавить диагностическую идентичность ответа

В bounded Search response или service-only receipt добавить и сохранить в evidence:

- `request_id`;
- `edge_contract_revision`;
- static repo/build identity;
- `search_policy_version`;
- `catalog_revision`;
- `embedding_corpus_revision`;
- embedding model, dimension и document kind;
- cache status;
- достигнутые стадии и timings;
- LLM requested/used/status/model;
- выбранный transport route.

По raw query, email, token, OTP и названиям карточек evidence не строить.

### P0.5. Локализовать и исправить backend-дефект, если live run не проходит

Диагностика должна однозначно показать последнюю успешную стадию:

```text
auth → route selection → edge accepted → result cache → quota
→ query embedding → vector RPC → verifier/degraded → response decode → EventCard render
```

Исправить найденный root cause и добавить regression test с тем же failure domain. Не заменять восстановление текущего Gemini/pgvector baseline миграцией на BGE.

## 4. Обязательные исправления корректности после восстановления

1. Включить `catalog_revision` и `embedding_corpus_revision` в result-cache key. TTL остаётся только safety ceiling.
2. Устранить mismatch: cache signature сейчас читает legacy LLM env, а verifier — Lite/Gemma selectors. Ввести одну `SearchPolicyVersion` для execution, cache и telemetry.
3. Добавить corpus receipt и release gate: coverage eligible events = 100%, stale/orphan/wrong-dimension = 0.
4. При отказе LLM verifier возвращать vector-only degraded выдачу, а не blank/error; разрешить короткий маркированный degraded cache.
5. Сохранить серверные quotas, idempotency, one-in-flight per user и global provider circuit breaker. Авторизация необходима, но сама по себе не является полной anti-bot защитой.

## 5. Evidence и безопасность

Artifact обязан содержать:

- `qa-summary.json`, `result.json`, `junit.xml`;
- sanitized network/transport/fault outcomes;
- Search stage receipt;
- platform/runtime/SHA identity;
- redaction audit.

Запрещено сохранять query text, email, OTP, JWT, cookies, request body, EventCard content, HAR, trace, video, raw Appium log и полный native hierarchy после sensitive input.

## 6. Критерии завершения

Задача считается закрытой только при одновременном выполнении:

- [ ] предназначенный release target отдаёт `/poisk/` с `200` и exact compatible SHA;
- [ ] email- и Yandex-authenticated users получают один Search contract;
- [ ] cold query возвращает валидные EventCard;
- [ ] немедленный повтор — доказанный result-cache hit без нового provider/vector/cold-quota расхода;
- [ ] direct-down → relay PASS на browser, Android, iOS;
- [ ] relay-down → direct PASS на browser, Android, iOS;
- [ ] both-down даёт честный no-dispatch degraded UX;
- [ ] один gesture = один `event-search` POST;
- [ ] LLM outage даёт vector-only результат;
- [ ] corpus coverage/freshness gate PASS;
- [ ] все artifacts проходят redaction;
- [ ] registry, canonical Search README, incident record и CHANGELOG содержат terminal run IDs и exact SHAs;
- [ ] PR не содержит временных patch/source-export scaffolding.

## 7. Не входит в эту задачу

- перевод production на BGE;
- новый Search-specific transport или Auth client;
- CAPTCHA для всех пользователей;
- автоматический повтор cost-bearing Search через альтернативный origin;
- публикация production root вне общего release protocol;
- объявление PASS по mocked browser test без terminal live evidence.
