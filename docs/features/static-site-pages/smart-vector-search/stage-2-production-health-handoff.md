# Search production-health — handoff этапа 2

Канонический архитектурный контракт: [README, §16](README.md#16-search-production-health-архитектурная-коррекция-этап-1).

Текущее состояние после реализации кода, до live acceptance:

```text
STAGE2_IMPLEMENTED_LIVE_ACCEPTANCE_PENDING
PRODUCT_HEALTH_UNCONFIRMED
```

## Цель этапа 2

Подключить подготовленный pure contract к существующему current accepted target
resolver и получить два финальных acceptance proofs: browser + Android, затем
browser + iOS. Это не лимит diagnostic work: evidence-driven manual runs можно
повторять до качественного результата; ограничены ровно два автоматических
production-health запуска в сутки и один Search POST на platform cell. Не
возвращать cached-repeat, LLM,
pagination или полный scheduled mobile/release matrix в production-health;
минимальный Android/iOS user journey является обязательной частью health.

## Требуемые live-действия

1. Из clean `origin/main` подтвердить, что stage-1 workflows и deterministic CI
   зелёные; не запускать полную static build ради отладки health.
2. Подключить `search-production-health.yml` к существующему
   `resolve_current_secret_candidate` adapter. Разрешён только canonical current
   receipt; latest Kaggle job и public-root fallback запрещены.
3. После side-effect-free browser/device preflight получить отдельную no-mail
   session для каждой platform cell через существующий GitHub OIDC broker и
   `auth.session_fixture`; не передавать session между jobs, не отправлять OTP
   и не читать mailbox.
4. Выполнить минимальный UI journey: открыть pinned `/poisk/`, один vector-only
   query, ровно один POST с limit ≤5, получить 1–5 карточек, сделать настоящий
   wheel scroll в browser или native touch/swipe на mobile и открыть одну
   карточку с HTTP 200.
5. Собрать client-observed Auth/Edge/direct REST/RPC bytes, доказать target
   `≤48 KiB`, hard `≤96 KiB`, LLM/pagination/receipt-RPC/Storage-images `0`.
6. После journey повторно прочитать только pointer. При смене target записать
   `target_superseded=true`; Search не повторять и product incident не создавать.
7. Typed reporter уже подключён, но automatic schedule/repository dispatch
   закрыты variable `SEARCH_PRODUCTION_HEALTH_ENABLED`. Установить `true` лишь
   после двух ограниченных live доказательств. Release qualification не имеет
   schedule/direct repository trigger; только explicit `full` marker может
   запросить её один раз после standard platform PASS.

## Что уже реализовано

- единый `production-health-run.mjs` с current accepted target pin/reread;
- Playwright и real Appium Android/iOS preflight в той же session;
- platform-bound OIDC broker claim и admission до трёх разных platform personas;
- overlapping issue coalescing, 30-second memory replay и одинаковый
  encrypted durable result с двухминутным TTL для process/restart
  lost-response; plaintext credential, filesystem/cache/artifact escrow нет;
- one-query/one-POST vector-only journey, card ID parity, обязательный реальный
  scroll и exact candidate `/sobytiya/<slug>/` route 200;
- Auth getUser + один owner RLS proof; iOS выполняет этот Search-only proof в
  общей OTP/Search Safari session через WebDriver async callback с explicit
  bounded 15-second script timeout; callback landing одновременно даёт exact
  target 2xx receipt и не требует повторного перехода на тот же clean URL;
  общий OTP/Search network reducer нормализует Android/CDP `{method,params}` и
  официальный XCUITest `safariNetwork` `{method,event}`; 48/96 KiB meter и
  strict evidence;
- exact deployment marker `none|standard|full`, side-effect-free backend HEAD
  contract proof и pre-Search active release
  receipt и platform issue disposition, suppressing issue mutation when the
  pinned target was superseded.

Перед live остаются только production gates: применить broker migration,
добавить новые workflow refs и exact events
`workflow_dispatch,schedule,repository_dispatch` в broker allowlist, отдельно
deploy `supabase/functions/event-search` из exact merged main и подтвердить его
side-effect-free HEAD receipt, затем deploy Fly exact merged main,
выполнить два manual workflows и затем включить automatic variable.

## Production environment/secrets

Переиспользовать environment `search-e2e` и существующие роли без копирования:

- `SEARCH_E2E_FLY_APP_NAME` и `SEARCH_E2E_FLY_SSH_TOKEN` — read-only получение
  current accepted candidate receipt;
- `SEARCH_E2E_AUTH_BROKER_URL` и OIDC audience
  `kenigevents-static-search-broker`;
- `SEARCH_E2E_SUPABASE_URL` и `SEARCH_E2E_SUPABASE_PUBLISHABLE_KEY`;
- три отдельные no-mail health personas: browser, Android и iOS, плюс отдельная
  `search-cold-browser` persona для release qualification. Одна mutable session
  не передаётся между jobs. Health и legacy сериализованы общей concurrency
  group; qualification имеет отдельную group и всегда получает новую cold
  browser session, даже если вызвана из того же `full` GitHub run.

Secrets не выводятся в job output/artifacts. Opaque target URL маскируется до
первого log; iOS Safari network/console остаются только в driver buckets, а
Appium stdout работает на error-level с URL filter. Evidence хранит только
fingerprint и typed counters.

## Triggers, которые надо включить после live acceptance

| Trigger | Политика |
|---|---|
| `workflow_dispatch` | оставить |
| `17 6 * * *` | browser + Android, 08:17 Europe/Kaliningrad |
| `17 18 * * *` | browser + iOS, 20:17 Europe/Kaliningrad |
| repository dispatch после реального Search runtime/backend deploy | добавить с pinned deployment identity |
| Smart Update/static snapshot/data generation/corpus/index refresh | **никогда не добавлять** |

Release qualification остаётся manual/selective; никакого scheduled LLM или
расширенного mobile/release matrix.

## Два полных live workflows

1. **Browser + Android:** отдельные sessions и ровно один Search POST на каждую
   platform cell; browser wheel и Android native touch должны пройти.
2. **Browser + iOS:** новые отдельные sessions и ровно один Search POST на
   каждую cell; browser wheel и iOS XCUITest swipe должны пройти.

До включения repository variable необходимо машинно сравнить sanitized
receipts обоих успешных workflows: `target_url_sha256`, полный immutable target
tuple, `site_runtime_sha`, `search_backend_revision`, `content_generation_id`
и `search_index_generation_id` должны быть попарно равны. Любое отличие
означает, что продукт между proofs изменился; variable остаётся выключенной и
Stage 2 остаётся `PRODUCT_HEALTH_UNCONFIRMED`.

Автоматических scheduled запусков ровно два в сутки — утренний и вечерний
профили выше. Это ограничение не запрещает evidence-driven manual debugging до
достижения качественного результата: после каждого failure сначала читаются
sanitized evidence и deterministic fixtures, исправление проходит CI, и только
затем выполняется следующий bounded run. Слепой retry-loop в GitHub Actions
запрещён; каждый platform cell по-прежнему делает не больше одного Search POST.

## Возможные блокеры и типизация

- broker lease `409` / OIDC недоступен → `UNKNOWN_AUTH_BROKER`, без product issue;
- Playwright/runner/browser start failure → `UNKNOWN_RUNNER_BROWSER`;
- Android emulator/Appium failure → `UNKNOWN_ANDROID_INFRA`;
- iOS simulator/WDA/Appium failure → `UNKNOWN_IOS_INFRA`;
- current accepted receipt отсутствует или не активен →
  `BLOCKED_RELEASE_NOT_ACTIVE`;
- pointer сменился → `target_superseded=true`, без retry;
- bytes >96 KiB → `COST_GUARD_FAILED`;
- evidence содержит secret/URL/query/session → `EVIDENCE_REDACTION_FAILED`;
- только доказанный current-target `BROKEN_*` может стать product incident.

Repository хранит точную Edge identity как deterministic `sha256:<64>` digest
deployable `supabase/functions/event-search/` source tree. Generated constant
проверяется CI, публикуется через side-effect-free HEAD и обычный Search
response; response остаётся authoritative evidence, в том числе при смене Edge
между HEAD и единственным POST. Это намеренно не объявляется provider byte SHA
или git SHA и не требует новой таблицы/сервиса.

PR #436 не является зависимостью health activation. Его preclaim guard полезен
для immutable release qualification, но должен проходить отдельный review и не
возвращать `release_exact` в scheduled product health.

## Completion evidence этапа 2

- sanitized browser + Android и browser + iOS receipts с одним POST на cell;
- pinned target identity и superseded check;
- Auth/Edge/REST byte totals и 48/96 KiB verdict;
- card-open HTTP 200 и real-scroll receipt;
- enabled trigger diff без content-generation hooks;
- typed incident decision;
- состояние меняется на `HEALTHY` только если live evidence это доказало.
