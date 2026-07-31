# LLM Limit Management Framework (LLM Gateway)

> **Linear Task:** [EVE-11](https://linear.app/events-bot-new/issue/EVE-11/llm-rate-limits)
> **Status:** canonical project-scoped database contract is applied; runtime
> cutover and post-deploy reconciliation are tracked by
> `INC-2026-07-31-google-ai-parallel-limiter-bypass`.
> **Component:** `google_ai.client.GoogleAIClient`

## 1. Цель
Обеспечить надежную работу с LLM (Gemma 2/3, Gemini) в условиях жестких ограничений API (RPM, TPM, Daily Limit), исключая "молчаливые" падения и превышения квот.

## 2. Архитектура
Фреймворк реализован как обертка над текущим SDK `google.genai` с
централизованным контролем через отдельный Supabase ledger. Legacy SDK может
использоваться только внутри `GoogleAIClient`; прямые probe/скрипты удалены или
переведены на тот же gateway.

### 2.1. Ключевые компоненты
*   **GoogleAIClient (`google_ai/client.py`)**: Единая точка входа. Управляет повторными попытками (Retries), логированием и вызовом RPC.
*   **Dedicated Supabase Database**:
    *   Таблицы `google_ai_*` хранят лимиты/счётчики/аудит. Схема описана в `docs/architecture/eve-arch-phase-1.md`.
    *   *Примечание:* Сами ключи хранятся в ENV, а Supabase возвращает имя переменной окружения для выбранного ключа.
*   **Supabase RPC (`google_ai_reserve`)**: Резервирование лимитов. Оно является
    атомарным между процессами только для версии
    `google_ai_project_model_atomic_v1`. Каноническая self-contained схема —
    `supabase/migrations/20260731170000_google_ai_canonical_limiter_bootstrap.sql`;
    наличие старой RPC с тем же именем не доказывает этот контракт. Успешный
    ответ обязательно содержит `limiter_contract`, `quota_scope` и
    `env_var_name`; клиент отвергает старый или неполный ответ до чтения ключа.
    *   По умолчанию reserve теперь **scope-ится к `default_env_var_name` клиента**: если вызывающий consumer не передал явные `candidate_key_ids`, клиент сначала резолвит metadata только для своего ENV-ключа (`GOOGLE_API_KEY` для обычных bot-потоков, `GOOGLE_API_KEY2` для guide-only runtimes). Это защищает общие пайплайны от случайного “перетекания” на чужой ключ только потому, что в `google_ai_api_keys` появилась новая активная строка.
    *   Если metadata для scoped ENV-ключа отсутствует, клиент **не** снимает
        scope и не берёт общий key pool: remote runtime fail-closed завершает
        вызов без provider send.
*   **Supabase RPC (`google_ai_mark_sent`)**: Помечает, что запрос реально отправлен провайдеру (для диагностики/восстановления).
*   **Supabase RPC (`google_ai_finalize`)**: Фиксирует фактическое потребление токенов и статус провайдера.
*   **Reserve fallback (только изолированная локальная разработка)**:
    * production/agent/Kaggle/Fly default — fail closed. При недоступном
      `google_ai_reserve`, Supabase или key metadata provider-вызов запрещён;
    * `GOOGLE_AI_ALLOW_RESERVE_FALLBACK`,
      `GOOGLE_AI_LOCAL_LIMITER_FALLBACK` и
      `GOOGLE_AI_LOCAL_LIMITER_ON_RESERVE_ERROR` по умолчанию равны `0`;
    * process-local limiter разрешается только явным opt-in в одном
      изолированном dev-процессе. Он не является контролем для параллельных
      Codex/Fly/Kaggle процессов и не должен включаться в remote runtime;
    * fallback больше не “залипает” навсегда в процессе: клиент периодически перепроверяет RPC и автоматически возвращается к Supabase-limiter после восстановления.
    * интервал перепроверки: `GOOGLE_AI_RESERVE_RPC_RECHECK_SECONDS` (по умолчанию `600` сек).
    * при transient сетевых сбоях (`SSL handshake timeout`, `server disconnected`, `EOF`) reserve RPC сначала делает короткие retry и только затем переключается в local fallback:
      - `GOOGLE_AI_RESERVE_RPC_RETRY_ATTEMPTS` (по умолчанию `2`);
      - `GOOGLE_AI_RESERVE_RPC_RETRY_BASE_DELAY_MS` (по умолчанию `350` мс, exponential backoff + jitter).
    * если клиент создан без Supabase (`supabase_client=None`), по умолчанию он
      возвращает `shared_limiter_unavailable`, не env-key. Для локального probe
      оператор должен явно включить local fallback и может задать
      `GOOGLE_AI_LOCAL_RPM`.
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

То же относится к старой версии `google_ai_reserve` без advisory lock. Для
параллельного rollout недостаточно получить HTTP 2xx от RPC: должна быть
применена migration 008 и зафиксирована capability/version-проверка.

Что проверить:

*   Убедитесь, что атомарной парой заданы
    `GOOGLE_AI_LIMITER_SUPABASE_URL` и
    `GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY`. Общие `SUPABASE_*` не являются
    steady-state fallback для limiter.
*   Если RPC лежит не в `public`, выставьте `SUPABASE_SCHEMA` в нужную схему (и проверьте, что PostgREST эту схему экспонирует).
*   Если в проекте есть только `finalize_google_ai_usage`, это допустимо: клиент продолжит работать в режиме legacy finalize без DDL-изменений.

Быстрый probe из репозитория (не печатает секреты, только статус/первые 400 символов ответа):

```bash
python scripts/inspect/probe_supabase_rpc.py google_ai_reserve --schema public
python scripts/inspect/probe_supabase_rpc.py google_ai_reserve --schema public --use-service
```

Если `--use-service` даёт 200/2xx, а без него 404/PGRST202, проблема в правах (нужен service key).

### 2.4. Включение межсервисного лимитера (rollout)

Текущий rollout создаёт отдельный канонический ledger двумя миграциями:

```text
supabase/migrations/20260731170000_google_ai_canonical_limiter_bootstrap.sql
supabase/migrations/20260731170100_google_ai_limiter_registry_seed.sql
```

После применения обязательны:

1. `google_ai_limiter_capabilities().limiter_contract ==
   google_ai_project_model_atomic_v1`;
2. пять активных redacted key metadata rows и полный model registry;
3. reserve-smoke внутри транзакции с `ROLLBACK` — без provider send;
4. Supabase advisors без замечаний по `google_ai_*`;
5. dedicated URL/service-key pair во всех Fly/Kaggle consumers.

Старые migrations 002/008 ниже остаются только для истории существующего
legacy-проекта и не являются целевым production deployment.

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

После базовой migration 002 для существующей БД обязательно применить также:

```sql
-- migrations/008_google_ai_atomic_reserve.sql
```

Она сериализует check+increment по key/model. Пока применение не подтверждено,
одновременные provider consumers должны оставаться выключенными.

### 2.4.1. Provider quota scope и прямые обходы

Google AI Studio показывает квоту на уровне Cloud project/model. Новый reserve
берёт advisory lock и суммирует counters по `quota_scope/model`, а не по id
ключа.
Поэтому до инвентаризации `API key -> Cloud project` нельзя считать два разных
ключа независимыми quota lanes. Если они принадлежат одному проекту, их расход
должен суммироваться перед admission. Это открытый rollout blocker из
`INC-2026-07-31-google-ai-parallel-limiter-bypass`.

Запрещены production-вызовы `generativelanguage.googleapis.com`,
`google.genai` или `google.generativeai` в обход shared reserve/mark/finalize.
Edge search, Universal Festival Parser, AfishaThumb, benchmarks и runtime
consumers переведены на gateway; legacy GemmaKey2 raw-key notebook выведен из
эксплуатации без manual override. Офлайн-аудит
`scripts/inspect/audit_google_ai_provider_paths.py` обязан показывать
`allowlisted_debt=0` и `unapproved=0`.

Локальные `agy`/Gemini CLI используют отдельную OAuth-аутентификацию и не
входят в API-key ledger; их нельзя запускать как будто они защищены этим
limiter.

Production event-vector sync с этого изменения использует
`GoogleAIClient.embed_content_async()`; скрытый direct REST retry-loop удалён,
каждая реальная попытка проходит отдельный shared reserve/finalize.

### 2.5. Канонические model-id и текущие лимиты

Лимитер работает по нормализованным `model` из `google_ai_model_limits`, а не по raw provider name.
Для Gemma 4 это важно, потому что в провайдера уходит `...-it`, а в Supabase quota-table хранится base id.

Консервативные seed-значения Gemma 4 исправлены по quota UI от `2026-07-31`:

*   `gemma-3-27b` -> `30 RPM / 15000 TPM / 14400 RPD`
*   `gemma-4-31b` -> `15 RPM / 15000 TPM / 14000 RPD`
*   `gemma-4-26b-a4b` -> `15 RPM / 15000 TPM / 14000 RPD`

Нормализация в клиенте:

*   `models/gemma-4-31b-it` -> `gemma-4-31b`
*   `models/gemma-4-26b-a4b-it` -> `gemma-4-26b-a4b`

Предыдущее значение `2147483647` ошибочно трактовало TPM как unlimited и не
могло остановить показанное на dashboard превышение. Оно запрещено в
канонической bootstrap migration.

#### Antigravity managed agent

`antigravity-preview-05-2026` — это agent code для Gemini Interactions API, а
не модель для `generate_content`. Google предоставляет ему управляемую Linux
песочницу, code execution, Google Search, URL context и сохраняемую между
interaction файловую систему.

Quota UI проекта проверен `2026-07-29`:

* provider quota: `60 RPM / 100000 TPM / 100 RPD`;
* shared-limiter safe cap:
  `54 RPM / 96000 TPM / 90 RPD` (`migrations/006_google_ai_antigravity_limits.sql`).

`100000 TPM` — минутная квота, а не документированный жёсткий предел одного
запроса. Для одного interaction нужно отдельно задавать
`agent_config.max_total_tokens`; Google описывает этот бюджет как best-effort.
Structured output у Antigravity preview не поддерживается, поэтому JSON
задаётся prompt-контрактом и обязательно валидируется локально.

Запись в `google_ai_model_limits` сама по себе не делает agent call через
`GoogleAIClient.generate_content_async()`. Новый production consumer обязан
явно reserve/finalize-ить interaction под canonical id
`antigravity-preview-05-2026`, учитывать все `usage.total_tokens` и
fail-closed завершаться при недоступном shared limiter.

Ограниченный live probe `2026-07-29` прошёл через `GOOGLE_API_KEY5`:

* API создал remote environment и агент реально использовал Bash/Python,
  Google Search и web fetching;
* исследован свежий `festival_queue.id=1291` — выпуск 2026 фестиваля
  «Территория мира — Территория музыки»;
* первый interaction с `max_total_tokens=45000` завершился `incomplete` после
  `78690` total tokens, continuation с `20000` — после `29704`;
* итоговый JSON агент записал и проверил в sandbox, но из-за исчерпания
  best-effort budget финальный `output_text` не был возвращён.

Практические правила после probe:

1. Просить агента сохранять результат под `/workspace/...`, а не `/tmp/...`:
   environment snapshot Files API экспортирует workspace, но не временный
   `/tmp`.
2. Считать `status=incomplete` отдельным нормальным исходом budget guard и
   скачивать workspace snapshot до continuation.
3. Не принимать агентный JSON напрямую в production. В этом probe агент
   корректно нашёл официальный `festtm.ru` и программу трёх дней, но также
   назвал неподтверждённый номер выпуска, сослался на прошлогоднюю страницу
   `sobor39.ru/news/nashi-novosti/3930/` как на 2026 год и подставил абонемент
   вместо билета первого дня. Нужен deterministic provenance/date/URL review.
4. Канонический evidence-first prompt pack и fail-closed pipeline для
   фестивального исследования описаны в
   `docs/llm/antigravity-festival-research.md`. Монолитный research+merge
   prompt не является допустимым production contract.

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
