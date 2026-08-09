# Search production-health — handoff этапа 2

Канонический архитектурный контракт: [README, §16](README.md#16-search-production-health-архитектурная-коррекция-этап-1).

Стартовое состояние:

```text
ARCHITECTURE_READY_FOR_LIVE_VALIDATION
PRODUCT_HEALTH_UNCONFIRMED
```

## Цель этапа 2

Подключить подготовленный pure contract к существующему current accepted target
resolver и выполнить не более двух полных live workflows за одну итерацию:
browser + Android, затем browser + iOS. Не возвращать cached-repeat, LLM,
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
7. Лишь после двух ограниченных live доказательств включить triggers и typed
   reporter. Release qualification подключать отдельно и оставлять manual/selective.

## Production environment/secrets

Переиспользовать environment `search-e2e` и существующие роли без копирования:

- `SEARCH_E2E_FLY_APP_NAME` и `SEARCH_E2E_FLY_SSH_TOKEN` — read-only получение
  current accepted candidate receipt;
- `SEARCH_E2E_AUTH_BROKER_URL` и OIDC audience
  `kenigevents-static-search-broker`;
- `SEARCH_E2E_SUPABASE_URL` и `SEARCH_E2E_SUPABASE_PUBLISHABLE_KEY`;
- три отдельные no-mail health personas: browser, Android и iOS. Одна mutable
  session/account не разделяется между platform jobs, manual legacy или
  qualification run.

Secrets не выводятся в job output/artifacts. Opaque target URL маскируется до
первого log; evidence хранит только redacted path/fingerprint и typed counters.

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

За одну итерацию запрещено выполнять больше двух live Search runs. При failure
сначала анализировать sanitized evidence и deterministic fixtures; GitHub
Actions нельзя использовать как цикл проб и ошибок.

## Возможные блокеры и типизация

- broker lease `409` / OIDC недоступен → `UNKNOWN_AUTH_BROKER`, без product issue;
- Playwright/runner/browser start failure → `UNKNOWN_RUNNER_BROWSER`;
- current accepted receipt отсутствует или не активен →
  `BLOCKED_RELEASE_NOT_ACTIVE`;
- pointer сменился → `target_superseded=true`, без retry;
- bytes >96 KiB → `COST_GUARD_FAILED`;
- evidence содержит secret/URL/query/session → `EVIDENCE_REDACTION_FAILED`;
- только доказанный current-target `BROKEN_*` может стать product incident.

Gap: repository пока не хранит точный deployed Edge Function byte/SHA. Сначала
проверить provider/deploy receipts; только при их отсутствии добавить минимальное
не-секретное поле в существующий deployment receipt, без новой таблицы/сервиса.

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
