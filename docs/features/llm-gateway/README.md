# LLM Limit Management Framework (LLM Gateway)

> **Linear Task:** [EVE-11](https://linear.app/events-bot-new/issue/EVE-11/llm-rate-limits)
> **Status:** ✅ Implemented
> **Component:** `google_ai.client.GoogleAIClient`

## 1. Цель
Обеспечить надежную работу с LLM (Gemma 2/3, Gemini) в условиях жестких ограничений API (RPM, TPM, Daily Limit), исключая "молчаливые" падения и превышения квот.

## 2. Архитектура
Фреймворк реализован как обертка над текущим SDK `google.genai` с централизованным контролем стейта через Supabase. Deprecated `google.generativeai` оставлен только ленивым fallback для старых локальных probe/скриптов, которые ещё не прошли миграцию.

### 2.1. Ключевые компоненты
*   **GoogleAIClient (`google_ai/client.py`)**: Единая точка входа. Управляет повторными попытками (Retries), логированием и вызовом RPC.
*   **Supabase Database**:
    *   Таблицы `google_ai_*` хранят лимиты/счётчики/аудит. Схема описана в `docs/architecture/eve-arch-phase-1.md`.
    *   *Примечание:* Сами ключи хранятся в ENV, а Supabase возвращает имя переменной окружения для выбранного ключа.
*   **Supabase RPC (`google_ai_reserve`)**: Атомарное резервирование лимитов. Возвращает `env_var_name` (какую переменную среды читать).
    *   По умолчанию reserve теперь **scope-ится к `default_env_var_name` клиента**: если вызывающий consumer не передал явные `candidate_key_ids`, клиент сначала резолвит metadata только для своего ENV-ключа (`GOOGLE_API_KEY` для обычных bot-потоков, `GOOGLE_API_KEY2` для guide-only runtimes). Это защищает общие пайплайны от случайного “перетекания” на чужой ключ только потому, что в `google_ai_api_keys` появилась новая активная строка.
    *   Если metadata для scoped ENV-ключа отсутствует в Supabase registry (например `GOOGLE_API_KEY3` в Telegram Monitoring canary), клиент **не** снимает scope и не берёт общий key pool. Он переходит на process-local limiter fallback с тем же `default_env_var_name`, чтобы provider call всё равно шёл через выбранный runtime key.
    *   Empty scoped-key cache is sticky: an empty result is cached as `[]` and remains local-limiter fallback on later calls in the same process, instead of widening back to an unscoped reserve.
*   **Supabase RPC (`google_ai_mark_sent`)**: Помечает, что запрос реально отправлен провайдеру (для диагностики/восстановления).
*   **Supabase RPC (`google_ai_finalize`)**: Фиксирует фактическое потребление токенов и статус провайдера.
*   **Reserve fallback (защита от “вечного fallback в Smart Update”)**:
    * если `google_ai_reserve` временно недоступен из-за миграционного/схемного рассинхрона (`PGRST202`, `Route ... not found`), клиент может перейти в bypass-режим и использовать `GOOGLE_API_KEY` напрямую;
    * управляется ENV `GOOGLE_AI_ALLOW_RESERVE_FALLBACK` (`1` по умолчанию, `0` для strict-режима).
    * fallback больше не “залипает” навсегда в процессе: клиент периодически перепроверяет RPC и автоматически возвращается к Supabase-limiter после восстановления.
    * интервал перепроверки: `GOOGLE_AI_RESERVE_RPC_RECHECK_SECONDS` (по умолчанию `600` сек).
    * при transient сетевых сбоях (`SSL handshake timeout`, `server disconnected`, `EOF`) reserve RPC сначала делает короткие retry и только затем переключается в local fallback:
      - `GOOGLE_AI_RESERVE_RPC_RETRY_ATTEMPTS` (по умолчанию `2`);
      - `GOOGLE_AI_RESERVE_RPC_RETRY_BASE_DELAY_MS` (по умолчанию `350` мс, exponential backoff + jitter).
    * если клиент создан без Supabase (`supabase_client=None`), это больше не
      считается unlimited local-dev режимом: по умолчанию включается тот же
      process-local fail-fast limiter. Его дефолтный RPM равен `15`, чтобы
      случайный server-side fallback не пробивал Gemma 4 free-tier лимит; для
      локальных probes можно явно переопределить `GOOGLE_AI_LOCAL_RPM`.
*   **Совместимость с legacy Supabase-проектами (без миграций):**
    * если отсутствует `google_ai_finalize`, клиент автоматически переключается на `finalize_google_ai_usage`;
    * fallback на legacy finalize применяется не только для первого запроса, а для всех следующих в процессе;
    * это изменение только в коде клиента, без изменения RPC/таблиц в проде.
*   **Stale reservation recovery:**
    * transient RPC errors на `google_ai_mark_sent` / `google_ai_finalize` теперь имеют короткие retry (тот же backoff-профиль, что и reserve RPC);
*   **Provider timeout guard:**
    * `GOOGLE_AI_PROVIDER_TIMEOUT_SEC` (default `0`, disabled unless set by a caller) wraps the underlying Google AI provider call with `asyncio.wait_for`;
    * timed-out calls are finalized as failed attempts and surfaced as `TimeoutError`, so feature-level code can fail-open or opt into its own retry policy without waiting for provider-side 10-minute deadlines.
    * для уже накопившихся записей доступен RPC `google_ai_sweep_stale(p_older_than_minutes, p_limit)`, который компенсирует counters только для безопасного окна `status='reserved' AND sent_at IS NULL`, затем помечает записи как `stale`;
    * ручной запуск из репозитория: `python scripts/inspect/sweep_google_ai_stale.py --use-service --older-than-minutes 30 --limit 500`.

### 2.3. Диагностика PGRST202 (RPC not found / schema cache)

Если локально/в CI вы видите `PGRST202` по `google_ai_reserve`/`google_ai_finalize`, это означает, что PostgREST не видит RPC в текущей схеме или роль не имеет прав на выполнение функции.

Важно: в таком состоянии межсервисный лимитер не работает как единый атомарный контроль (можно превысить общий RPM/TPM/RPD при параллельной нагрузке нескольких сервисов).

Что проверить:

*   Убедитесь, что задан `SUPABASE_SERVICE_KEY` (а не только `SUPABASE_KEY`). Для rate-limit RPC обычно нужен service role.
*   Если RPC лежит не в `public`, выставьте `SUPABASE_SCHEMA` в нужную схему (и проверьте, что PostgREST эту схему экспонирует).
*   Если в проекте есть только `finalize_google_ai_usage`, это допустимо: клиент продолжит работать в режиме legacy finalize без DDL-изменений.

Быстрый probe из репозитория (не печатает секреты, только статус/первые 400 символов ответа):

```bash
python scripts/inspect/probe_supabase_rpc.py google_ai_reserve --schema public
python scripts/inspect/probe_supabase_rpc.py google_ai_reserve --schema public --use-service
```

Если `--use-service` даёт 200/2xx, а без него 404/PGRST202, проблема в правах (нужен service key).

### 2.4. Включение межсервисного лимитера (rollout)

Для проектов, где есть только legacy `finalize_google_ai_usage`, нужно один раз применить SQL-миграцию:

```sql
-- файл из репозитория:
-- migrations/002_google_ai_rpc_rollout.sql
```

Что делает миграция:
*   добавляет недостающие таблицы счётчиков/аудита `google_ai_usage_counters`, `google_ai_requests`, `google_ai_request_attempts`;
*   создаёт RPC `google_ai_reserve`, `google_ai_mark_sent`, `google_ai_finalize`;
*   добавляет только недостающие колонки в уже существующие `google_ai_model_limits/google_ai_api_keys` (без destructive-изменений).

Проверка после применения:

```bash
python scripts/inspect/probe_supabase_rpc.py google_ai_reserve --schema public
python scripts/inspect/probe_supabase_rpc.py google_ai_mark_sent --schema public
python scripts/inspect/probe_supabase_rpc.py google_ai_finalize --schema public
```

Ожидание:
*   больше нет `404/PGRST202` по `google_ai_reserve/google_ai_mark_sent/google_ai_finalize`;
*   `google_ai_reserve` отвечает JSON-объектом (даже если `ok:false` по причине `rpd/rpm/tpm/no_keys`).

### 2.5. Канонические model-id и текущие лимиты

Лимитер работает по нормализованным `model` из `google_ai_model_limits`, а не по raw provider name.
Для Gemma 4 это важно, потому что в провайдера уходит `...-it`, а в Supabase quota-table хранится base id.

Актуальные seed-значения ниже были проверены в quota UI Google AI Studio для этого проекта `2026-04-06`:

*   `gemma-3-27b` -> `30 RPM / 15000 TPM / 14400 RPD`
*   `gemma-4-31b` -> `15 RPM / Unlimited TPM / 1500 RPD`
*   `gemma-4-26b-a4b` -> `15 RPM / Unlimited TPM / 1500 RPD`

Нормализация в клиенте:

*   `models/gemma-4-31b-it` -> `gemma-4-31b`
*   `models/gemma-4-26b-a4b-it` -> `gemma-4-26b-a4b`

Техническая деталь: в таблице `google_ai_model_limits.tpm` "Unlimited TPM" хранится как `2147483647`, потому что схема лимитера требует целочисленный cap.

### 2.6. Structured output и thought filtering для Gemma 4

Для `Gemma 4` клиент теперь различает два runtime-контракта:

*   `Gemma 3` / старые Gemma-path по-прежнему fail-open работают через prompt-only JSON contract: `response_mime_type` / `response_schema` снимаются на клиенте, потому что эти модели часто отвергали native JSON knobs.
*   `Gemma 4` (`gemma-4-31b`, `gemma-4-26b-a4b`) теперь сохраняет native `response_mime_type=application/json` и `response_schema`, если вызывающий stage их передал. Это нужно для structured extract / classify / dedup stages, где `lollipop g4` уже показал реальный practical uplift именно от native schema discipline.
*   `generate_content_async()` теперь принимает не только plain string, но и multimodal prompt parts (`text` + `inline_data` blobs). Это позволяет guide/Telegram Kaggle runtimes использовать общий gateway и для image+text OCR/vision paths, а не обходить лимитер отдельным direct SDK-вызовом.
*   Provider call для новых Gemma stages идёт через `google.genai.Client(...).aio.models.generate_content(...)`; legacy `google.generativeai.GenerativeModel.generate_content_async(...)` используется только если новый SDK отсутствует или тест явно подставил legacy fake.

Дополнительное правило transport hygiene:

*   при чтении `candidates[].content.parts[]` клиент отбрасывает `parts[].thought = true`, чтобы Gemma 4 thought-channel не утекал в parsed JSON, persisted history или operator-facing surfaces.

### 2.2. Алгоритм работы
1.  **Reserve**: Клиент запрашивает резерв (примерно `max_output_tokens + 1000`).
    *   Для длинных текстовых prompt’ов используется консервативная оценка по байтам **и** символам; это особенно важно для русскоязычных/OCR-heavy запросов, где простой `bytes/4` может занизить реальный input TPM.
    *   Для multimodal prompt parts текст оценивается отдельно от binary blobs, а каждый image/blob получает дополнительный safety reserve, чтобы raw bytes не раздували estimate строковым `repr`, но image-heavy OCR calls всё равно не уходили в систематическое under-reserve.
    *   *Успех:* Получает `api_key` и разрешение.
    *   *Отказ:* Получает `RateLimitError` (Fail Fast, NO_WAIT).
2.  **Execute**: Вызов API провайдера (Google AI Studio).
    *   *Ошибка:* Если 5xx — ретрай. Если 429 — немедленный проброс ошибки
        наверх без sleep; единственное исключение — явно объявленный normal
        pool, где клиент сразу резервирует следующего ещё не использованного
        участника того же пула и не меняет модель.
    *   *Пустой ответ:* трактуется как `ProviderError(empty_response)` и ретраится.
3.  **Finalize**: Клиент отправляет реальную статистику (`input_tokens`, `output_tokens`) в БД для корректировки квот.

## 3. Возможности
*   **Multi-Account Sharding**: Поддержка ротации ключей/аккаунтов через переменную `GOOGLE_API_LOCALNAME`.
*   **Atomic Counting**: Исключает Race Conditions при параллельных запросах.
*   **Fail Fast**: Не ждет в очереди (чтобы не вешать воркера), а сразу падает, позволяя планировщику (JobOutbox) перезапустить задачу позже.
    * Для provider-side `429` unpooled consumer по-прежнему fail-fast. В
      объявленном normal pool клиент без sleep исключает перегруженный key id и
      пробует следующий pool member; после исчерпания пула ошибка уходит
      вызывающему workflow. Emergency overflow сам по себе этого права не даёт.
*   **Structured Logging**: Все вызовы логируются в формате JSON Lines для анализа.
*   **Operational visibility**: bypass reserve логируется отдельным событием `google_ai.reserve_fallback_no_rpc` — это сигнал, что RPC-схему нужно починить.
    * `google_ai.reserve_ok` в production не должен иметь `api_key_id=null` /
      `used_after=null`, кроме явно локальных probes; это regression signal для
      `INC-2026-06-28-google-ai-gemma4-rpm-overrun`.
*   **Incident alerts**: критические сбои LLM отправляются в админ-чат как инцидент (`notify_llm_incident`).
    * ENV `GOOGLE_AI_INCIDENT_NOTIFICATIONS=0` — выключить инцидент-алерты.
    * ENV `GOOGLE_AI_INCIDENT_COOLDOWN_SECONDS` — антиспам/дедуп уведомлений (по умолчанию 900 сек).
*   **Model fallback chain**: при финальном провале основной модели клиент переключается на запасные модели из `GOOGLE_AI_FALLBACK_MODELS` (через запятую) и логирует `google_ai.model_fallback`.
    * Gateway уважает `requested_model`: первой в цепочке всегда идёт запрошенная модель, а запасные модели остаются только fallback-хвостом.
    * Gemma-модели меньше `12b` (`1b/4b`) автоматически исключаются из цепочки и не используются для текста.
*   **Emergency overflow keys**: `GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS` может дать scoped consumer запасные ключи только при
    daily-budget отказах (`rpd`/`no_keys`). Per-minute (`rpm`/`tpm`) не расширяется, чтобы параллельные задачи не пробивали
    минутные лимиты. Каждый env в этом списке должен существовать и как runtime secret, и как активная строка
    `google_ai_api_keys.env_var_name`; env-only ключ не виден атомарному reserve RPC. Успешный borrow запасного ключа
    пишет `google_ai.reserve_overflow_used` в structured log, но не отправляет operator-facing LLM incident: алерт нужен
    только когда рабочий ключ/модель не найдены или provider call реально завершился ошибкой.
*   **Normal rotating pool**: consumer может передать
    `GoogleAIClient(reserve_key_envs=[...])`. В отличие от emergency overflow,
    это его обычный candidate set с самого первого reserve: process-wide cursor
    меняет стартовый env на каждом запросе, а существующий atomic RPC проверяет
    кандидатов по одному и при `rpm`/`tpm`/`rpd` пробует следующего участника той
    же нормальной allocation. Каждый env обязан существовать и в runtime, и как
    active `google_ai_api_keys.env_var_name`; отсутствие shared Supabase limiter
    или registry member завершает pool fail-closed, без local/bypass fallback.
    Provider-side `429`, возникший из-за дрейфа внешнего provider quota
    относительно shared ledger, также остаётся внутри этой allocation: текущий
    key id исключается, следующий участник резервируется атомарно, событие
    `google_ai.provider_key_rotation` сохраняет причину и размер остатка пула.
    Явный `candidate_key_ids` никогда не расширяется за пределы caller scope.
    API-key rotation распределяет только наши ledger counters: по контракту
    Google AI квоты принадлежат Cloud project, поэтому несколько ключей одного
    project не создают несколько независимых RPD. Feature-specific total caps
    всё равно обязательны.
*   **Smart Update 4o fallback budget**: 4o остаётся аварийным fallback после Gemma/Gemini ошибок, но массовые Smart Update
    переливы можно ограничить `SMART_UPDATE_4O_FALLBACK_MAX_PER_HOUR=N`. `SMART_UPDATE_4O_FALLBACK=0` — временный
    incident kill-switch, не steady-state policy.
*   **Smart Update provider retry cap**: `SMART_UPDATE_GOOGLE_AI_MAX_RETRIES` отдельно ограничивает внутренние
    `GoogleAIClient` retry для массового Smart Update (default `1`). Это не запрещает 4o fallback: оно предотвращает
    умножение Gemma RPM/RPD на provider `500/504`, после чего маленький 4o budget остаётся последним аварийным хвостом.

### 3.1. Логирование конкретной модели (обязательно)
В JSON-логах клиента теперь фиксируются **оба** имени модели:

*   `model` — модель для лимитера (rate-limit model, нормализованная для RPC Supabase).
*   `requested_model` — модель, которую запросил вызывающий код.
*   `provider_model` — короткое имя модели у провайдера (например, `gemma-3-27b-it`).
*   `provider_model_name` — фактическое имя, отправленное в API провайдера (например, `models/gemma-3-27b-it`).
*   `invoked_model` — поле для быстрой проверки в логах: фактически вызванная модель.

Это нужно для пост-фактум проверки, что запрос ушёл именно в ожидаемую модель, а не только в её нормализованный alias для квот.

## 4. Использование
```python
from google_ai.client import GoogleAIClient

client = GoogleAIClient(supabase_client=db)
try:
    text, usage = await client.generate_content_async(
        model="gemma-3-27b",
        prompt="Analyze this event..."
    )
except RateLimitError:
    print("Limits exceeded, try again later")
```

### 4.1. Kaggle smoke-probe для конкретного ключа

Для live-проверки отдельного ключа Gemma через Kaggle добавлен приватный kernel
`kaggle/GemmaKey2Probe/gemma_key2_probe.ipynb` и launcher
`kaggle/execute_gemma_key2_probe.py`.

Что делает launcher:
* best-effort подхватывает `.env` для `KAGGLE_USERNAME`/`KAGGLE_KEY` и общих env;
* может дополнительно читать другой env-файл c целевым ключом (например `.env copy`);
* шифрует только выбранный ключ в `secrets.enc`, кладёт `fernet.key` в отдельный dataset
  и запускает Kaggle kernel через тот же split-secrets паттерн, что используется в
  Telegram Monitoring и `/3di`;
* скачивает `output.json` в `artifacts/codex/kaggle/gemma-key2-probe/<run_id>/`.

Базовый запуск:

```bash
python kaggle/execute_gemma_key2_probe.py --env-file ".env copy"
```

Полезные опции:
* `--secret-var GOOGLE_API_KEY2` — имя проверяемого ключа (по умолчанию уже `GOOGLE_API_KEY2`);
* `--model models/gemma-3-27b-it` — модель для smoke-call;
* `--keep-datasets` — не удалять временные private datasets после прогона.

Важно:
* launcher не печатает значение ключа;
* если в `.env` ключа нет, можно передать второй env-файл через `--env-file`;
* Kaggle output фиксирует только `ok/status_code/model/response_excerpt` и диагностические excerpts,
  без секрета.

### 4.2. Gemma 4 structured-output caveat

Практический вывод из live guide-monitoring smoke `2026-04-19`:

* для `Gemma 4` нельзя считать поддержкой "весь JSON Schema";
* provider contract успешно принимает `response_schema`, но может отвергать отдельные поля schema-слоя;
* в нашем runtime подтверждённый несовместимый ключ — `additionalProperties`.

Следствие:

* structured `Gemma 4` stages должны использовать упрощённое schema-подмножество;
* prompt-level contract `Return only JSON` остаётся обязательным, но сам по себе не заменяет native schema;
* любые новые `Gemma 4` structured stages нужно smoke-проверять именно на реальном provider, а не только по локальным unit-тестам.

### 4.3. Gemini TTS: обязательный общий лимитер

Google TTS доступен только через `google_ai.tts.GoogleTTSClient` и проектный
skill `.codex/skills/google-tts-generation/`. Прямые REST/SDK-вызовы с raw key
запрещены: они не создают atomic reserve/audit и могут незаметно сжечь дневную
квоту.

Поддерживаемые exact model IDs:

* `gemini-2.5-flash-preview-tts` — default;
* `gemini-3.1-flash-tts-preview` — только по явному выбору, не fallback.

Обе модели используют один quota bucket `google-tts`: переключение alias не
удваивает бюджет. В `google_ai_model_limits` задан project guard
`1 RPM / 10 RPD`; TPM в локальном ledger не используется как утверждение о
provider audio quota. Счётчик day bucket соответствует существующему контракту
gateway — UTC.

Строгий runtime-контракт:

1. `--check` проверяет без reserve/provider-вызова:
   Supabase, model row, `quota_scope`, все env-name pool members, active registry
   rows, наличие secrets и текущие daily counters.
2. `google_ai_reserve` под advisory transaction lock атомарно выбирает один
   доступный key lane и увеличивает общий scope counter.
3. `google_ai_mark_sent` обязан завершиться успешно **до** обращения к Google.
4. Выполняется ровно один provider-вызов. Нет provider retry, model fallback,
   emergency overflow или перехода на второй key после sent failure.
5. `google_ai_finalize` сохраняет success/provider failure и token metadata.
   Уже отправленный failed call остаётся потраченным.

Отсутствие Supabase, RPC, model row, полного registry pool или secret всегда
заканчивается fail-closed до provider-вызова. Process-local limiter и direct-env
fallback для TTS запрещены.

Multi-key pool задаётся только именами env-переменных:

```bash
GOOGLE_TTS_KEY_ENVS=GOOGLE_API_KEY,GOOGLE_API_KEY2,GOOGLE_API_KEY3,GOOGLE_API_KEY5
python .codex/skills/google-tts-generation/scripts/generate_tts.py \
  --env-file .env \
  --check
```

Google применяет Gemini rate limits к проекту, а не к API key. Поэтому несколько
ключей одного Google AI project не создают независимый provider RPD; включать в
pool следует только понятные operator lanes. Наш per-registry-key ledger —
conservative routing/accounting слой, а не способ обойти project quota.

Результат single-speaker TTS — mono PCM `audio/L16`, обычно `24000 Hz`,
упакованный CLI в WAV. Артефакты и redacted receipts хранятся только в
`artifacts/codex/google-tts/`.

Миграция `migrations/006_google_ai_tts_limits.sql` также идемпотентно учитывает
один успешный direct-запрос от `2026-07-29` для registry lane
`GOOGLE_API_KEY`: synthetic audit имеет финальный `succeeded`, а daily
`google-tts` counter увеличивается только при первой вставке. Пользователь
подтвердил расход `1`; наблюдавшийся перед успехом provider `503` сохранён как
операционный факт, но не добавлен вторым RPD вопреки quota UI.

## TODO / Risks
- Дочистить оставшиеся direct-SDK inspect/probe scripts после Smart Update G4 rollout, чтобы удалить legacy `google.generativeai` fallback из runtime dependencies.

## Event-media scoped vision consumer

`event_media_review` uses the shared `GoogleAIClient`, but deliberately has a
stricter envelope: one image pair per call, KEY4 only, no reserve overflow or
local limiter fallback, 100 primary + 25 escalation calls per UTC day and at
most three automatic attempts. Primary budget exhaustion does not consume the
escalation allowance. Pair/result cache and daily counters live in Fly SQLite;
unresolved media stays non-public. Full contract: [Event media](../event-media/README.md).

`smart_update_image_geometry` — отдельный event-media consumer для face/value
metadata. Он с первого запроса использует normal pool KEY4+KEY5 (не overflow),
не видит KEY1–KEY3, не имеет model/local fallback, делает максимум одну provider
попытку на durable item и по умолчанию ограничен `100` calls/UTC day. External
backfill дополнительно capped на `400` total calls/day и paced `>=6s`, поэтому
не предполагает, что пять env keys автоматически означают пять provider quotas.
