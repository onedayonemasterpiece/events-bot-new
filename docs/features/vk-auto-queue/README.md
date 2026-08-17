# VK Auto Queue Import

Канонический контракт автоматического VK ingestion после
`INC-2026-08-10-smart-update-identity-terminal-loss`: максимальная полнота,
raw-first durability и строгая LLM-first семантика. Очередь не требует
операторского semantic review. Решение владельца от 2026-08-15 заменяет часть
старого контракта про бесконечный background retry: строка, уже выбранная в
видимый `/vk_auto_import` batch, обязана завершиться в этом batch как accepted,
подтверждённое product exclusion или `FAILED_TECHNICAL` с причиной и receipt.

**Release status:** базовый linear-ingestion fix из PR #513 и остаточные
OCR/final-adjudication/lifecycle исправления из PR #514 работают в production
(`3458b549d3`, Fly v1983). Exact replay четырёх строк run 6020 закрыл два
исторических carrier как доказанный no-event, но выявил ещё две точные границы:
sub-minute provider Retry-After не ожидался внутри VK claim, а более поздний
carrier terminal скрывал уже сохранённый успешный parse receipt. Эти границы
остаются release-gate текущего открытого инцидента до повторного exact replay.

Дополнительный production-аудит ops 6085/6093 доказал ещё две visual-evidence
границы. Исчерпанный дневной бюджет primary poster OCR раньше не допускал уже
реализованный Google fallback, а `wall.getById` возвращал для VK/OK video
истёкшие preview URL (все варианты отвечали HTTP 404). Текущий linear contract
поэтому требует: при исчерпанном primary budget выполнить один independently
limited Google OCR в том же claim; для video выполнить один user-token
`video.get`, взять минимальный доступный MP4 и проанализировать короткий файл
Google multimodal inline. Видео не превращается в poster и не сохраняется как
event media; его распознанный текст/аудио входит только в EvidenceManifest и
LLM source decision. `VK_VIDEO_EVIDENCE_MAX_BYTES` ограничен 19 MiB hard cap
(default 18 MiB). Ошибка download/analysis остаётся видимым technical terminal,
а не ложным `CONFIRMED_NO_EVENT`.

## Граница покрытия

Каждый пост, который crawler фактически получил из настроенного `vk_source`
внутри технического crawl/backfill horizon, сначала без потерь сохраняется как
immutable `vk_source_packet`. Но в дорогую очередь `vk_inbox` попадает только
пост, прошедший отдельный **collection-time admission**:

1. Детерминистика пропускает только уверенный high-recall positive — есть
   event keyword, дата и будущий `event_ts_hint`; blank/photo-only также
   пропускается, потому что его смысл ещё скрыт в афише.
2. Если детерминистика не смогла уверенно подтвердить пост, небольшой LLM batch
   решает только `ADMIT | PAST_ONLY | NON_EVENT | UNCERTAIN`.
3. Grounded `PAST_ONLY/NON_EVENT` с confidence `>=0.90` и дословной цитатой
   получает terminal admission receipt и **не входит** в `vk_inbox`, даже если
   у поста есть media: приложенная картинка не отменяет доказательный текст.
   Если содержимое непроверенной афиши способно изменить решение, LLM обязан
   вернуть `UNCERTAIN`, и такой пост сохраняется в очереди.
4. `ADMIT`, `UNCERTAIN`, invalid schema, timeout/provider failure или реально
   неоднозначное visual evidence fail-open в `vk_inbox`, чтобы admission не мог
   потерять реальное событие.

Это не авторазбор: crawler лишь собирает, проверяет допуск и формирует очередь.
Отдельный scheduled `vk_auto_import` позднее берёт bounded batch из `vk_inbox`,
загружает media/OCR и выполняет полный source parse + Smart Update. Семантическое
решение остаётся LLM-first: keyword/date слой умеет только пропустить очевидный
positive, но не имеет права самостоятельно отклонить пост.

Инвариант одного revision:

```text
VK API fetch
  -> vk_source_packet (commit)
  -> deterministic positive OR LLM admission for unresolved post
       -> grounded PAST_ONLY/NON_EVENT: packet terminal, no vk_inbox row
       -> ADMIT/UNCERTAIN/technical: vk_inbox pending
  -> later scheduled bounded vk_auto_import
  -> attachments/OCR + bounded inline short-video evidence + EvidenceManifest
  -> SourceParseDecision
  -> one bounded sub-minute provider Retry-After inside the same claim
  -> optional contradiction verifier
  -> bounded final adjudication only if the VK decision is still technical
  -> Smart Update per event child / typed lifecycle action
  -> typed carrier terminal (no automatic durable retry from this batch)
```

Здесь важно разделять две очереди. `vk_crawl_continuation` — внутренняя
инфраструктурная доставка ещё не выбранных raw pages; её bounded durable retry
сохраняется. `vk_inbox`, уже claimed конкретным auto-import batch, не получает
`next_attempt_at`: даже timeout, fetch/evidence/provider/schema/persist error,
неполный evidence или исчерпанный provider/schema path закрываются как
`failed_technical`/`FAILED_TECHNICAL`. Unmatched lifecycle action получает
явный product no-op; он не превращает уже созданного event sibling в ошибку.
Повтор возможен только как явно
наблюдаемое операторское re-drive, а не как фоновый вечный цикл.

Cursor не продвигается, пока не сохранены все полученные in-horizon packets и
для каждого нового revision не записан admission receipt / fail-open enqueue.
Page/hard cap создаёт `vk_crawl_continuation`. Неизменившийся revision с тем же
payload/revision hash использует exact successful receipt без provider-вызова;
изменившийся пост получает новый immutable revision и снова становится due.
Blank/photo-only packets также сохраняются и fail-open идут через поздние OCR и
full source LLM: текстовый admission не имеет права угадывать содержимое афиши.

`VK_AUTO_IMPORT_RATE_LIMIT_MAX_WAIT_SEC` (default `60`, hard max `180`) задаёт
общий wall-clock budget для одного явного provider `Retry-After` внутри
текущего carrier claim. Это не background retry: после исчерпания budget строка
сразу получает видимый `FAILED_TECHNICAL`. Успешный immutable parse receipt не
перестаёт быть replayable, если последующий lifecycle/Smart этап завершил
carrier техническим terminal; failed attempt не имеет права перезаписать
identity/result уже принятого provider parse. Exact replay добавляет только
receipt с `parse_key=NULL`, поэтому не конфликтует с единственным physical
successful-parse key.

Idle backfill определяется по `vk_crawl_cursor.checked_at` — времени последнего
успешного scan, а не по `updated_at` последнего найденного поста. Иначе тихий,
но регулярно проверяемый источник каждые 24 часа повторно материализует полную
историю. Production crawl до provider fetch проверяет свободное место на
runtime volume и при остатке ниже `VK_CRAWL_MIN_FREE_MB` (default `512`) падает
в retry с `vk_crawl_storage_admission_blocked`; health critical floor не
является write admission сам по себе.

Continuation consumer запускается автоматически и следует ожидаемой durable
state machine без зависимости от ручного UI:

```text
pending/retry due -> leased/running
  -> fetch one bounded page -> persist every raw packet/revision from that page
  -> advance the persisted continuation offset
  -> mark done only on empty/short/horizon/original-cursor proof
```

Continuation workers deliberately do not rewrite the canonical
`vk_crawl_cursor`; only the primary crawl owns that cursor. A continuation is
bound to the original cursor boundary stored in its durable row and advances
only its own offset after the complete page reaches `vk_source_packet`.

Typed transport/provider/backpressure failure освобождает continuation в
`retry due` с причиной и следующим временем попытки. После restart stale lease
возвращается в due retry. Cursor/offset нельзя продвинуть при частично
сохранённой странице; exact replay должен быть idempotent. Имена внутренних
helper-функций не являются частью контракта — обязательны durable transitions и
read-back state.

Full page сам по себе **никогда** не доказывает окончание tail. Worker хранит
самую глубокую durable границу `(date, post_id)` и сравнивает её после каждого
полностью сохранённого page. Повторный fingerprint, page из одних уже durable
rows или full page без более глубокой границы получает `OFFSET_DRIFT` либо
`NO_PROGRESS`, увеличивает/rebases offset и остаётся `retry`. Это корректирует
VK offset относительно изменяемой головы. Terminal `done` разрешён только для:

- `EMPTY_PAGE`;
- `SHORT_PAGE`;
- `HORIZON_REACHED` в backfill;
- `ORIGINAL_CURSOR_OVERLAP` в incremental scan.

Legacy exact-full-page rows, ошибочно отмеченные `done`, reopen при schema init и
перед scheduling. Collision со старым offset-unique row остаётся наблюдаемым
`OFFSET_DRIFT_COLLISION` retry, а не `done`/stale-running.

## Durable schema

- `vk_source_packet` — immutable raw carrier revision: owner/post/revision,
  source URL, publication/fetch time, raw JSON/text, attachment metadata,
  payload/revision hashes, discovery/date hints, `event_ts_hint`, OCR/LLM
  state, lease, attempts, prompt/model/quota scope, `next_attempt_at`, typed
  reason and carrier outcome.
- `vk_source_packet_attempt` — append-only receipt каждого physical primary,
  repair, conditional-verification, terminal-adjudication или exact-replay
  attempt: evidence manifest,
  request/response/finish metadata, input/output/thought/reserved tokens,
  disposition, child/action counts, Smart Update child outcomes and terminal.
  API keys, secrets и полный payload сюда не пишутся.
- `vk_crawl_continuation` — durable continuation для page/safety cap.
- `vk_inbox` — совместимая due/claim projection, связанная через
  `source_packet_id`; `vk_inbox_import_event` хранит все принятые Event ID.

`event_ts_hint` влияет только на приоритет. `NULL`, ошибочно прошлое или далёкое
значение не исключает carrier. Age-based ordering не даёт unknown-date rows
голодать.

Обычный scheduled drain остаётся oldest-first при bounded due queue. Если due
backlog превышает `VK_AUTO_IMPORT_FRESH_FIRST_BACKLOG_THRESHOLD` (default
`150`), durable cursor чередует свежие carriers с одним самым старым через
каждые `VK_AUTO_IMPORT_HISTORY_EVERY` picks (default `5`). Поэтому текущий
intake не ждёт за многолетним replay, а история продолжает продвигаться даже
при непрерывном притоке новых posts и после рестарта. Исторические rows не
удаляются, не исключаются и не получают deterministic no-event verdict; после
снижения backlog снова действует oldest-first fairness.

## Raw source envelope v1

Каждый initial crawl, continuation page и успешный fresh `wall.getById` refresh
использует один `vk_source_envelope` schema version `1`. Durable packet хранит:

- canonical owner type/id, post id, publish/edit timestamps и canonical wall URL;
- sanitized `raw_item` целиком, outer text и рекурсивное `copy_history` tree;
- ordered `text_segments`/`revision_metadata` с JSON-like path и role;
- **все** attachment records, включая неизвестные/nonvisual types;
- link/doc/video/photo semantics и доступные link/doc/video preview candidates;
- all/selected/omitted/unavailable media inventories, counts и completeness;
- full payload hash и отдельный semantic revision hash, чувствительный к text,
  copy tree, edit metadata и attachment semantics, но не к volatile counters.

Упрощённый shape (поля внутри inventory показаны сокращённо):

```json
{
  "schema": "vk_source_envelope",
  "schema_version": 1,
  "source_type": "vk",
  "owner_id": 123,
  "owner_type": "group",
  "post_id": 456,
  "raw_item": {"text": "...", "attachments": [], "copy_history": []},
  "text_segments": [{"path": "$", "role": "outer", "text": "..."}],
  "revision_metadata": [{"path": "$", "id": 456, "date": 1780000000}],
  "attachment_inventory": [],
  "all_media_candidates": [],
  "media_candidates": [],
  "omitted_media_candidates": [],
  "unavailable_visual_attachments": [],
  "counts": {"attachment_inventory_count": 0},
  "completeness": {"capture_complete": true}
}
```

Recursive secret denylist removes request/auth/error material (`access_token`,
authorization, API/client secrets, generic token, captcha and provider error
payloads) and secret-like URL query parameters before persistence. Attachment
`access_key` is replay capability evidence inside the protected raw packet; it
must not enter logs, prompts or LLM receipts.

Replay matrix: a deleted/unavailable post may be parsed from its complete v1
envelope; a successful fresh refresh first persists its new immutable revision
and only then parses it; a legacy text/photos projection is explicitly
`replayable_legacy_incomplete` and closes the selected auto-import row as
technical `EVIDENCE_INCOMPLETE`, never as a semantic no-event. Media selection
limits OCR candidates, not the attachment inventory or capture-complete claim.

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

Пустой ответ, malformed/schema mismatch, truncation, timeout, quota error или
неполный OCR не равны no-event. Положительные
children из incomplete evidence можно провести через Smart Update, но carrier
закрывается видимым `FAILED_TECHNICAL`: уже принятые children сохраняются, а
неподтверждённый остаток не переигрывается автоматически. `CONFIRMED_NO_EVENT`
принимается только при
`llm_completed && structured_response_valid && evidence_complete`.

Кроме того, `CONFIRMED_NO_EVENT` обязан иметь ровно один
`SourceNoEventReason`: `NO_ATTENDABLE_EVENT`, `GIVEAWAY_ONLY`, `VAGUE_TEASER`,
`REFERRAL_ONLY`, `SERVICE_OR_RENTAL`, `RECAP_ONLY` или `OUT_OF_SCOPE`. У всех
остальных dispositions reason отсутствует. Missing/unknown/misplaced reason —
`SCHEMA_MISMATCH` и terminal `FAILED_TECHNICAL`, никогда product rejection.

Обычный carrier выполняет один primary semantic parse. Второй вызов допустим
только как conditional verifier для закрытого набора противоречий: сильные
signals против no-event, date/OCR conflict, collapsed occurrences, generic
ungrounded title, mixed lifecycle conflict, impossible schema или incomplete
coverage. Если primary/verifier всё ещё вернул malformed/schema/technical
результат, VK выполняет ровно один final adjudication в том же invocation на
отдельном schema-strict запросе, где `RETRY_REQUIRED` запрещён. Он обязан
вернуть events/lifecycle либо complete-evidence `CONFIRMED_NO_EVENT`; повторный
provider/schema failure остаётся видимым `FAILED_TECHNICAL`, а не скрытой
очередью.

Все эти trigger-факты вычисляет общий pure `source_contradiction_facts` collector
для shared main/VK/direct/parser callers; Telegram stages ровно тот же module.
Collector не выдаёт product verdict и не удаляет positive children. На carrier
разрешён максимум один verifier, а uncertain result закрывается technical. Тексты
prompts и provider examples не копируются сюда: см.
[`../../llm/prompts.md`](../../llm/prompts.md). Static audit проверяет mandatory
reason, закрытые enums/parity и terminal prompt gates.

## Lifecycle и Smart Update

Cancellation/reschedule regex не изменяет Event до primary parse. LLM может
вернуть несколько `LifecycleAction` и одновременно новые event children.
Действия применяются независимо. No-match action означает явный
`LIFECYCLE_NO_MATCH_NOOP`: lifecycle-only carrier закрывается как product
no-op, а mixed carrier остаётся `MIXED_RESOLVED` вместе с созданным/обновлённым
event child. Это не уничтожает sibling и не считается технической ошибкой.

Каждый child проходит Smart Update. Downstream Telegraph/ICS/publication/month
rebuild запускаются только для typed accepted `CREATED`, `MERGED` или
`NOOP_EXACT_REPLAY`; `diagnostic_event_id` не считается успехом. Carrier-level
итоги: `EVENTS_RESOLVED`, `LIFECYCLE_RESOLVED`, `MIXED_RESOLVED`,
`CONFIRMED_NO_EVENT`, `CONFIRMED_PRODUCT_EXCLUSION`, `FAILED_TECHNICAL`,
`LIFECYCLE_NO_MATCH_NOOP` или `EXACT_REPLAY`. Summary обязан балансировать
`processed = imported + rejected + failed`; `deferred=0`.

## Backpressure

`wall.getById` network/VK API failure допускает максимум два (configurable до
трёх) коротких transport attempts внутри текущего row invocation. Primary LLM
provider сохраняет собственный bounded physical-attempt contract; одинаковый
complete evidence не запускает semantic background retry. После исчерпания
текущего invocation quota/RPM/TPM/RPD/429, OCR/provider/schema/persist error,
timeout или orphaned claim закрывают row как `FAILED_TECHNICAL`, очищают lease и
selectable `vk_inbox.next_attempt_at`, сохраняют typed reason в packet/latest
attempt и попадают в summary/ops receipt. У immutable packet остаётся
schema-required timestamp, но terminal status делает его inert. Это не product
rejection и не автоматическая due queue.

Poster OCR отдельно делает до трёх transport attempts (hard max четыре) только
для timeout/connection и HTTP `408/409/429/5xx`, с коротким backoff и отдельным
`X-Client-Request-Id` на попытку. Ошибка одной картинки не стирает уже успешно
распознанные siblings; manifest остаётся честно incomplete для отсутствующей.

Только предшествующий crawl continuation остаётся durable retryable: он ещё
доставляет raw page до `vk_source_packet` и не является одной из 15
операторски видимых строк auto-import batch.

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
- `VK_AUTO_IMPORT_LIMIT` (application default `15`, production `25` so the
  four-fresh/one-history selector has measured headroom above fresh arrivals);
- `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL` — существующий scoped model route;
- `VK_AUTO_IMPORT_PREFETCH=0` по умолчанию; даже при включении semantic parse
  принадлежит main worker;
- `VK_AUTO_IMPORT_ROW_TIMEOUT_SEC` — timeout становится terminal
  `FAILED_TECHNICAL` без background retry;
- `VK_AUTO_IMPORT_FETCH_INLINE_ATTEMPTS` — bounded transport attempts в одном
  row invocation (default `2`, hard max `3`);
- `VK_AUTO_IMPORT_FRESH_FIRST_BACKLOG_THRESHOLD` — размер due backlog, после
  которого scheduled importer включает bounded fresh/history interleave;
- `VK_AUTO_IMPORT_HISTORY_EVERY` — не реже каждого N-го pick при большом
  backlog выбирается самый старый carrier (default `5`);
- `VK_CRAWL_MIN_FREE_MB` — production-volume admission floor перед VK fetch и
  packet persistence (default `512` MiB);
- `VK_CRAWL_ADMISSION_MODEL` — маленькая модель только для collection-time
  классификации deterministic failures (default `gemini-3.1-flash-lite`);
- `VK_CRAWL_ADMISSION_BATCH_SIZE` — число unresolved posts в одном bounded LLM
  запросе (default `8`, hard max `20`);
- `VK_CRAWL_ADMISSION_TIMEOUT_SEC` — wall-clock cap одного admission batch
  (default `75`, hard range `10..180` seconds);
- `VK_AUTO_IMPORT_INLINE_JOBS` / `VK_AUTO_IMPORT_INLINE_INCLUDE_ICS` управляют
  только ожиданием downstream receipts после accepted result.

Старые `VK_AUTO_IMPORT_PREFILTER_OBVIOUS_NON_EVENTS` и semantic prefilter API
удалены из production path. Старые photo/OCR semantic caps не являются
допустимым способом экономии TPM: если transport не дал полный материал,
negative outcome запрещён; строка завершается `FAILED_TECHNICAL` и требует
явного re-drive после восстановления evidence.

`/vk`, `/vk_queue`, `/vk_misses`, manual accept/reject/skip и editor actions
остаются legacy diagnostic/admin surfaces. Они могут помочь исследовать receipt
или выполнить отдельное редакционное действие, но автоматический carrier не
ждёт operator verdict и не обязан пройти manual terminal transition.

## Recovery и observability

Legacy pending rows, созданные raw-first crawler до collection-time gate, можно
проверить тем же контрактом, не запуская auto-import и не создавая Events:

```bash
# default is read-only dry-run; newest rows first
python scripts/ops/requalify_vk_inbox_admission.py --db /data/db.sqlite --limit 100

# explicit bounded apply after reviewing dry-run counters
python scripts/ops/requalify_vk_inbox_admission.py \
  --db /data/db.sqlite --limit 100 --apply
```

Команда меняет только admission receipt и связанный `vk_inbox.status`.
Grounded past/non-event становится `rejected`; admitted/fail-open остаётся
`pending` для отдельного расписания auto-import. Плохой provider/schema не
паркует пост в новой retry-очереди и не удаляет его — он fail-open. Строки,
уже claimed текущим auto-import (`status=locked`/`locked_by`), команда не
трогает. Старый `review_batch` на строке, которая снова `pending` и не имеет
lock, не считается активным claim: auto-import сам выбирает такие строки, и
requalifier также обязан их классифицировать. Обычный crawler не запускает
скрытую массовую переклассификацию старого backlog — для неё используется
только этот bounded entrypoint.

Read-only inventory/census:

```bash
python scripts/ops/smart_update_loss_census.py \
  --db <snapshot.sqlite> --since 2026-08-04 --until 2026-08-12 --output -

python scripts/ops/recover_smart_update_identity_losses.py \
  --db <snapshot.sqlite> --since 2026-08-04 --until 2026-08-12 \
  --read-only --include-discovery-misses --output -
```

В production snapshot новый schema может ещё отсутствовать; тогда отчёт обязан
показать `unavailable`, а не подменять отсутствие evidence нулём. Recovery не
делает прямых Event INSERT и в рамках incident разрешён только `--read-only`.
`--read-only`, `--dry-run` и `--apply` — mutually exclusive CLI modes; не
указывайте первые два вместе. Window всегда half-open `[since, until)`. Census и
recovery отдельно считают carrier revisions, event occurrences и lifecycle
actions; carrier count нельзя выдавать за число model-derived occurrences.

Zero-инварианты: semantic terminal before LLM, deterministic post-LLM veto,
incomplete-evidence no-event, новый `RETRY_SCHEDULED`/due row из
`/vk_auto_import` и carrier/child balance violation. `FAILED_TECHNICAL` теперь
обязательный наблюдаемый исход технической неопределённости; это явно
supersedes старый August-10 zero-terminal-technical пункт по решению владельца.
Канонический incident и release gates:

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
- `vk_source_envelope.py` — exact raw envelope v1, hashes и replayability;
- `source_contradiction_facts.py` — общий pure seven-reason collector;
- `vk_review.py` — durable carrier/attempt receipts и отдельные crawl
  continuation retry primitives;
- `source_parse_contract.py` — typed source/lifecycle/evidence contract;
- `smart_event_update.py`, `smart_update_state.py` — child resolution;
- `tests/test_source_parse_contract.py`;
- `tests/test_vk_raw_first_llm_contract.py`;
- `tests/test_vk_source_envelope.py`;
- `tests/test_vk_crawl_continuation.py`;
- `tests/test_vk_auto_queue_import.py`;
- `tests/test_vk_auto_queue_rate_limit.py`;
- `tests/test_vk_intake_future.py`;
- `tests/test_vk_intake_history.py`;
- `tests/test_vk_intake_keywords_dates.py`.

Список содержит только существующие test modules. Continuation acceptance должна
дополнительно проверять описанную выше state machine: due claim, running lease,
raw-persist-before-advance, done, typed retry и stale-lease recovery — без
привязки документации к временному имени helper-функции.
