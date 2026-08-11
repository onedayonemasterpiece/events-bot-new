# VK Auto Queue Import

Канонический контракт автоматического VK ingestion после
`INC-2026-08-10-smart-update-identity-terminal-loss`: максимальная полнота,
raw-first durability и строгая LLM-first семантика. Очередь не требует
операторского review и не имеет terminal technical failure.

## Граница покрытия

В обработку попадает каждый пост, который crawler фактически получил из
настроенного `vk_source` внутри технического crawl/backfill horizon. До
семантического LLM-решения разрешены только source allowlist, pagination,
horizon, raw persistence, загрузка вложений, OCR, exact replay и quota
admission. `no_keywords`, `no_date`, `past_event`, `too_far`, historical/admin,
`event_ts_hint` и cancellation regex — только hints/verification evidence.

Инвариант одного revision:

```text
VK API fetch
  -> vk_source_packet (commit)
  -> vk_inbox due state
  -> attachments/OCR + EvidenceManifest
  -> SourceParseDecision
  -> optional contradiction verifier
  -> Smart Update per event child / typed lifecycle action
  -> typed carrier terminal OR durable retry
```

Cursor не продвигается, пока не сохранены все полученные in-horizon packets.
Page/hard cap создаёт `vk_crawl_continuation`. Неизменившийся revision с тем же
payload/revision hash использует exact successful receipt без provider-вызова;
изменившийся пост получает новый immutable revision и снова становится due.
Blank/photo-only packets также сохраняются и идут через OCR и LLM.

Continuation consumer запускается автоматически и следует ожидаемой durable
state machine без зависимости от ручного UI:

```text
pending/retry due -> leased/running
  -> fetch one bounded page -> persist every raw packet/revision from that page
  -> advance continuation offset/cursor or mark done
```

Typed transport/provider/backpressure failure освобождает continuation в
`retry due` с причиной и следующим временем попытки. После restart stale lease
возвращается в due retry. Cursor/offset нельзя продвинуть при частично
сохранённой странице; exact replay должен быть idempotent. Имена внутренних
helper-функций не являются частью контракта — обязательны durable transitions и
read-back state.

## Durable schema

- `vk_source_packet` — immutable raw carrier revision: owner/post/revision,
  source URL, publication/fetch time, raw JSON/text, attachment metadata,
  payload/revision hashes, discovery/date hints, `event_ts_hint`, OCR/LLM
  state, lease, attempts, prompt/model/quota scope, `next_attempt_at`, typed
  reason and carrier outcome.
- `vk_source_packet_attempt` — append-only receipt каждого physical primary,
  repair, conditional-verification или exact-replay attempt: evidence manifest,
  request/response/finish metadata, input/output/thought/reserved tokens,
  disposition, child/action counts, Smart Update child outcomes and terminal.
  API keys, secrets и полный payload сюда не пишутся.
- `vk_crawl_continuation` — durable continuation для page/safety cap.
- `vk_inbox` — совместимая due/claim projection, связанная через
  `source_packet_id`; `vk_inbox_import_event` хранит все принятые Event ID.

`event_ts_hint` влияет только на приоритет. `NULL`, ошибочно прошлое или далёкое
значение не исключает carrier. Age-based ordering не даёт unknown-date rows
голодать.

## Evidence и source verdict

`vk_intake.build_event_drafts` перед primary parse формирует
`EvidenceManifest`: hash/длина полного source text, число вложений, доступные и
включённые OCR blocks, included chars, недоступные/omitted blocks и truncation.
Все доступные OCR blocks передаются независимо от длины текста и keyword regex.
Транспортный недоступный attachment явно делает evidence incomplete.

Typed `SourceParseDecision` допускает только:

- `EVENTS_FOUND`;
- `CONFIRMED_NO_EVENT`;
- `LIFECYCLE_ONLY`;
- `MIXED`;
- `RETRY_REQUIRED`.

Пустой ответ, malformed/schema mismatch, truncation, timeout, quota error,
неполный OCR или unresolved lifecycle action не равны no-event. Положительные
children из incomplete evidence можно провести через Smart Update, но carrier
остаётся `RETRY_SCHEDULED` для enrichment. `CONFIRMED_NO_EVENT` принимается
только при `llm_completed && structured_response_valid && evidence_complete`.

Обычный carrier выполняет один primary semantic parse. Второй вызов допустим
только как conditional verifier для закрытого набора противоречий: сильные
signals против no-event, date/OCR conflict, collapsed occurrences, generic
ungrounded title, mixed lifecycle conflict, impossible schema или incomplete
coverage. Технически недоступная/неоднозначная verification означает retry.

## Lifecycle и Smart Update

Cancellation/reschedule regex не изменяет Event до primary parse. LLM может
вернуть несколько `LifecycleAction` и одновременно новые event children.
Действия применяются независимо; no-match action сохраняется как durable retry
и не уничтожает siblings.

Каждый child проходит Smart Update. Downstream Telegraph/ICS/publication/month
rebuild запускаются только для typed accepted `CREATED`, `MERGED` или
`NOOP_EXACT_REPLAY`; `diagnostic_event_id` не считается успехом. Carrier-level
итоги: `EVENTS_RESOLVED`, `LIFECYCLE_RESOLVED`, `MIXED_RESOLVED`,
`CONFIRMED_NO_EVENT`, `CONFIRMED_PRODUCT_EXCLUSION`, `RETRY_SCHEDULED` или
`EXACT_REPLAY`.

## Backpressure

Quota/RPM/TPM/RPD/429, OCR/provider/schema/persist error, timeout, restart или
orphaned lease освобождают claim и записывают due retry. Они не переводят row в
terminal `failed`/`rejected`. Provider `retry_after` и `quota_scope` переносятся
из typed parse boundary в packet/inbox и append-only attempt. После быстрых
повторов применяется capped backoff, но row остаётся в automatic selection.
Worker не спит десятки минут на carrier и может взять другой due row/scope.

Prefetch загружает только transport evidence и не запускает второй LLM parse.
Успешный `(payload hash, source revision, evidence manifest, prompt version,
model)` receipt повторно используется.

## Запуск

Ручной E2E/операционный запуск:

```text
/vk_auto_import
/vk_auto_import --limit=25
/vk_auto_import --include-skipped
/vk_auto_import_stop
```

Scheduled entrypoint: `vk_auto_queue.vk_auto_import_scheduler`.
Основные ENV:

- `ENABLE_VK_AUTO_IMPORT=1`; production scheduler **default-on** в `fly.toml`
  (локальная/тестовая среда должна включать его явно, чтобы не получить
  неожиданные внешние вызовы);
- `VK_AUTO_IMPORT_TIMES_LOCAL` (default
  `06:15,10:15,12:00,15:30,18:30`);
- `VK_AUTO_IMPORT_TZ` (default `Europe/Kaliningrad`);
- `VK_AUTO_IMPORT_LIMIT` (default `15`);
- `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL` — существующий scoped model route;
- `VK_AUTO_IMPORT_PREFETCH=0` по умолчанию; даже при включении semantic parse
  принадлежит main worker;
- `VK_AUTO_IMPORT_ROW_TIMEOUT_SEC` — timeout становится typed retry;
- `VK_AUTO_IMPORT_INLINE_JOBS` / `VK_AUTO_IMPORT_INLINE_INCLUDE_ICS` управляют
  только ожиданием downstream receipts после accepted result.

Старые `VK_AUTO_IMPORT_PREFILTER_OBVIOUS_NON_EVENTS` и semantic prefilter API
удалены из production path. Старые photo/OCR semantic caps не являются
допустимым способом экономии TPM: если transport не дал полный материал,
negative outcome запрещён и планируется enrichment retry.

`/vk`, `/vk_queue`, `/vk_misses`, manual accept/reject/skip и editor actions
остаются legacy diagnostic/admin surfaces. Они могут помочь исследовать receipt
или выполнить отдельное редакционное действие, но автоматический carrier не
ждёт operator verdict и не обязан пройти manual terminal transition.

## Recovery и observability

Read-only inventory/census:

```bash
python scripts/ops/smart_update_loss_census.py \
  --db <snapshot.sqlite> --since 2026-08-04 --until 2026-08-12 --output -

python scripts/ops/recover_smart_update_identity_losses.py \
  --db <snapshot.sqlite> --since 2026-08-04 --until 2026-08-12 \
  --read-only --dry-run --include-discovery-misses --output -
```

В production snapshot новый schema может ещё отсутствовать; тогда отчёт обязан
показать `unavailable`, а не подменять отсутствие evidence нулём. Recovery не
делает прямых Event INSERT и в рамках incident разрешён только read-only dry-run.

Zero-инварианты: semantic terminal before LLM, deterministic post-LLM veto,
incomplete-evidence no-event, terminal technical failed и carrier/child balance
violation. Канонический incident и release gates:

- `docs/reports/incidents/INC-2026-08-10-smart-update-identity-terminal-loss.md`;
- `docs/operations/smart-update-prod-audit.md`;
- `docs/operations/release-smoke-smart-update.md`.

Эта реализация сама по себе **не означает deploy-ready**. Внешними release
blockers остаются все четыре независимых доказательства:

1. реальная provider quota/tier проверка на production route;
2. атомарная репетиция на свежем production snapshot;
3. явная disposition для FK orphan rows до apply;
4. replay model-derived recovery candidates через тот же typed Smart Update
   путь с проверяемыми receipts.

## Важные файлы и тесты

- `vk_intake.py` — raw-first crawl, evidence/OCR и source adapter;
- `vk_auto_queue.py` — claim, typed processing и downstream boundary;
- `vk_review.py` — durable state/attempt/retry receipts;
- `source_parse_contract.py` — typed source/lifecycle/evidence contract;
- `smart_event_update.py`, `smart_update_state.py` — child resolution;
- `tests/test_source_parse_contract.py`;
- `tests/test_vk_raw_first_llm_contract.py`;
- `tests/test_vk_auto_queue_import.py`;
- `tests/test_vk_auto_queue_rate_limit.py`;
- `tests/test_vk_intake_future.py`;
- `tests/test_vk_intake_history.py`;
- `tests/test_vk_intake_keywords_dates.py`.

Список содержит только существующие test modules. Continuation acceptance должна
дополнительно проверять описанную выше state machine: due claim, running lease,
raw-persist-before-advance, done, typed retry и stale-lease recovery — без
привязки документации к временному имени helper-функции.
