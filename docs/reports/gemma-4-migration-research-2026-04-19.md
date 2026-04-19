# Gemma 4 Migration Research 2026-04-19

Статус: research memo, без переключения runtime.

## Зачем этот отчёт

Цель исследования:

- понять, укладывается ли текущая живая нагрузка в `Gemma 4` free tier;
- зафиксировать фактическое распределение Gemma-нагрузки по фичам и ключам;
- отделить `production/runtime` use от `guide/Kaggle` и `research/lab`;
- подготовить реалистичный migration plan без преждевременного switch.

Важно:

- для квот Google главный разрез здесь не `30d total`, а `daily per key per feature`;
- все выводы ниже помечены либо как `confirmed`, либо как `estimate/inference`;
- отчёт учитывает не только `google_ai` gateway, но и найденные direct `google.generativeai` path.

## Confirmed facts

### Доступные Gemma-модели и лимиты

Подтверждено по `docs/features/llm-gateway/README.md` и по live-таблице `google_ai_model_limits`:

- `gemma-3-27b` -> `30 RPM / 15000 TPM / 14400 RPD`
- `gemma-4-31b` -> `15 RPM / Unlimited TPM / 1500 RPD`
- `gemma-4-26b-a4b` -> `15 RPM / Unlimited TPM / 1500 RPD`

### Ключи: production secrets vs live limiter metadata

Подтверждено по Fly production secrets у app `events-bot-new-wngqia`:

- `GOOGLE_API_KEY`
- `GOOGLE_API_KEY2`
- `GOOGLE_API_KEY3`

Подтверждено по `google_ai_api_keys` live metadata лимитера:

- `GOOGLE_API_KEY`
- `GOOGLE_API_KEY2`

Важно:

- `GOOGLE_API_KEY3` подтверждён как production secret;
- `GOOGLE_API_KEY3` при этом на момент этого исследования не виден в `google_ai_api_keys`;
- это означает, что production secret уже существует, но current live metadata лимитера пока знает только про два ключа.

### Где точно используется Gemma через `google_ai` gateway

`production/runtime`:

- `smart_update`
- `event_parse`
- `event_topics`
- `admin_assist`
- `bot` non-core path

`guide / Kaggle / related`:

- `guide_occurrence_enrich`
- `guide_profile_enrich`
- `guide_excursions_digest_batch`
- `guide_excursions_dedup`
- `guide_scout_*`
- `route_weaver_enrich`
- `consumer='kaggle'` как основной guide Kaggle stage

`research/lab`:

- `codex_limit_probe` on `Gemma 4` (`2026-04-06`, stale probes)

### Найденные direct `google.generativeai` path вне gateway

Подтверждено по коду:

- `kaggle/TelegramMonitor/telegram_monitor.ipynb`
- `kaggle/UniversalFestivalParser/src/enrich.py`
- `kaggle/UniversalFestivalParser/src/reason.py`

Это важно, потому что такие вызовы не попадают в `google_ai_requests` / `google_ai_usage_counters` и не учитываются в quota-аналитике gateway.

## Live quota view: что важно для Gemma 4

Источник:

- exact quota counters: `google_ai_usage_counters`
- feature/key attribution: `google_ai_requests`

### Daily load per key

За последние 30 дней по `gemma-3-27b`:

- `GOOGLE_API_KEY`: average `548.16/day`, peak `1031/day` on `2026-04-10`
- `GOOGLE_API_KEY2`: average `499.03/day`, peak `990/day` on `2026-04-10`
- combined: average `1047.19/day`, peak `2021/day` on `2026-04-10`

Вывод:

- один общий `Gemma 4` bucket на всё текущее observed usage не проходит по `1500 RPD`;
- два раздельных daily bucket сейчас проходят по observed peaks.

### RPM / TPM

За последние 30 дней по `gemma-3-27b`:

- `GOOGLE_API_KEY`: peak `12 RPM`, peak `14999 TPM`
- `GOOGLE_API_KEY2`: peak `13 RPM`, peak `14990 TPM`
- combined: peak `15 RPM`, peak `29663 TPM`
- minute buckets above `15 RPM`: `0`

Вывод:

- главный риск `Gemma 4` сейчас не `RPM`, а `RPD` и потеря правильного key split;
- `Unlimited TPM` у `Gemma 4` снимает текущий TPM pressure.

## Feature view: кто сколько тратит и на каком ключе

Ниже приведён продуктовый разрез, а не просто сырой `consumer`-лист.

### Production/runtime

| Feature bucket | Intended key | Observed key usage | 30d volume | Peak day |
| --- | --- | --- | ---: | --- |
| `smart_update` | `GOOGLE_API_KEY` | `KEY=10037`, `KEY2=1111` | `11148` | `767/day` on `KEY`, `510/day` on `KEY2` |
| `event_parse` | `GOOGLE_API_KEY` | `KEY=1366`, `KEY2=135` | `1501` | `85/day` on `KEY`, `50/day` on `KEY2` |
| `event_topics` | `GOOGLE_API_KEY` | `KEY=1080`, `KEY2=104` | `1184` | `94/day` on `KEY`, `68/day` on `KEY2` |
| `bot_misc` | `GOOGLE_API_KEY` | `KEY=390`, `KEY2=130` | `520` | `48/day` on `KEY`, `36/day` on `KEY2` |
| `admin_assist` | `GOOGLE_API_KEY` | no live traffic in last `30d` | `0` | n/a |

Confirmed fact:

- bot/runtime traffic сейчас не изолирован на одном ключе;
- часть runtime-нагрузки уже ходила в `GOOGLE_API_KEY2`, что нарушает желаемый contract `runtime -> one key`.

### Guide excursions

| Feature bucket | Intended key | Observed key usage | 30d volume | Peak day |
| --- | --- | --- | ---: | --- |
| `guide_kaggle_main` | `GOOGLE_API_KEY2` | `KEY2=12600`, `KEY=3` | `12603` | `968/day` |
| `guide_extract_family` | `GOOGLE_API_KEY2` by product intent | `KEY=4145` | `4145` | `454/day` on `2026-04-05` |
| `guide_server_family` | `GOOGLE_API_KEY2` | `KEY2=1389`, `KEY=22` | `1411` | `239/day` |

Где есть routing drift:

- `guide_kaggle_main` в целом уже сидит на `KEY2`;
- `guide_extract_family` заметно течёт в `KEY`, хотя продуктово это guide-only surface;
- `guide_server_family` в основном сидит на `KEY2`, но не идеально чисто.

### Research/lab

`Gemma 4` в live usage почти не использовалась:

- `gemma-4-31b`: `1` stale probe
- `gemma-4-26b-a4b`: `1` stale probe

Это не production load и не должно искажать migration estimate.

## Что оказалось неполным в первом проходе

Подтверждённый пробел первого исследования:

- quota-анализ по `google_ai_requests` был корректен только для gateway-tracked traffic;
- direct `google.generativeai` path не были инвентаризированы в первой версии отчёта;
- значит первый проход нельзя было считать полным quota-исследованием.

Сейчас этот пробел закрыт на уровне inventory:

- `Telegram monitoring` и `UniversalFestivalParser` найдены как direct SDK callers;
- их нужно учитывать отдельно от `google_ai` gateway.

Ограничение всё ещё остаётся:

- по direct-SDK path нет такого же готового daily accounting в `google_ai_usage_counters`;
- значит для них нужна отдельная операционная фиксация ключа и отдельная телеметрия.

## Key topology: рекомендуемая целевая схема

### Production/runtime

- `GOOGLE_API_KEY`
- сюда должны идти: `smart_update`, `event_parse`, `event_topics`, `admin_assist`, `bot_misc`

### Guide excursions

- `GOOGLE_API_KEY2`
- сюда должны идти весь guide-only runtime и guide Kaggle monitoring

### Telegram monitoring Kaggle

- `GOOGLE_API_KEY3`
- выделенный ключ для `Telegram monitoring` как отдельного Kaggle surface

Почему это разумно:

- bot/runtime не должен разъезжаться по нескольким ключам;
- guide already has a natural isolation boundary;
- Telegram monitoring живёт в Kaggle и сейчас вообще идёт direct SDK path, поэтому отдельный ключ даёт самую чистую operational isolation.

Важное уточнение:

- сам по себе production secret `GOOGLE_API_KEY3` уже существует;
- но это ещё не означает, что какая-либо фича начала его использовать;
- для gateway-tracked consumers нужен явный routing через `default_env_var_name` / `candidate_key_ids` / metadata;
- для `Telegram monitoring` этого тоже недостаточно само по себе, потому что текущий Kaggle notebook читает именно `GOOGLE_API_KEY`, а не `GOOGLE_API_KEY3`.

Подтверждённый факт по коду:

- `source_parsing/telegram/service.py` уже прокидывает в Kaggle payload все переменные, начинающиеся на `GOOGLE_API_KEY*`;
- но `kaggle/TelegramMonitor/telegram_monitor.ipynb` в текущем виде берёт `os.getenv('GOOGLE_API_KEY')` и делает `genai.configure(api_key=GOOGLE_API_KEY)`;
- следовательно, добавление секрета на Fly уже подготовило platform-side secret boundary, но не переключило сам `Telegram monitoring` на `GOOGLE_API_KEY3`.

## Gemma 4 model split

Это `recommendation / estimate`, а не уже подтверждённый quality fact.

### `gemma-4-26b-a4b`

Кандидаты для более лёгких стадий:

- `guide_scout_screen`
- `guide_scout_status_claim_extract`
- `event_topics`
- `geo_region` fallback
- lightweight `bot` checks

### `gemma-4-31b`

Кандидаты для более тяжёлых стадий:

- `smart_update`
- `event_parse`
- `route_weaver_enrich`
- `guide_occurrence_enrich`
- `guide_profile_enrich`
- `guide_excursions_digest_batch`

## Recommended migration order

### Wave 1

`Guide excursions monitoring` first on `Gemma 4`.

Почему именно он:

- отдельная фича;
- уже есть intended отдельный ключ `GOOGLE_API_KEY2`;
- достаточно большой объём для meaningful canary;
- blast radius ниже, чем у `smart_update`;
- внутри фичи есть естественный split между простыми и сложными LLM stages.

### Wave 2

`Telegram monitoring` on dedicated `GOOGLE_API_KEY3`.

Почему не раньше:

- сначала нужно зафиксировать guide canary на gateway-tracked path;
- Telegram monitoring сейчас direct-SDK path и требует отдельного контроля ключа и telemetry story.

### Wave 3

Core production/runtime:

- `event_topics`
- `event_parse`
- `smart_update` last

## Biggest quota risks

### Confirmed

- единый `Gemma 4` free-tier bucket на весь observed traffic не проходит по `1500 RPD`;
- runtime traffic уже drift-ил в `KEY2`;
- часть guide extraction traffic drift-ила в `KEY`;
- `Telegram monitoring` bypasses `google_ai` gateway accounting.

### Estimate / inference

- `GOOGLE_API_KEY3` как production secret уже есть и этого достаточно для отдельного key boundary на уровне platform secrets;
- отдельно всё ещё нужно проверить, что runtime path `Telegram monitoring` действительно использует именно `GOOGLE_API_KEY3`;
- двух `Gemma 4` ключей достаточно для observed `runtime + guide` load только при чистом routing contract;
- трёх ключей достаточно для схемы `runtime / guide / tg-monitoring`, если Telegram monitoring действительно будет изолирован и не смешается с runtime.

## Будет ли один ключ замедлять фичу

Это был отдельный проверочный вопрос исследования.

### Что подтверждено по коду

- `GoogleAIClient` не реализует “умное ожидание свободного ключа” сам по себе;
- он либо резервирует лимит через конкретный набор `candidate_key_ids`, либо сразу возвращает `RateLimitError`;
- если consumer не передал `candidate_key_ids`, reserve scope-ится к `default_env_var_name` этого клиента.

Практический смысл:

- простое наличие нескольких ключей в проекте не означает автоматический cross-key spillover;
- чтобы одна и та же фича реально переливалась между ключами, routing должен быть задан явно.

### Что подтверждено по live данным за 30 дней

По `google_ai_request_attempts`:

- `blocked_attempts = 0`
- `retry_after_attempts = 0`
- `requests_with_multiple_keys = 0`
- только `1` request имел retry, и ни один request не переключался между ключами

Вывод:

- нет live-доказательства, что текущая схема выигрывала в latency за счёт того, что один и тот же request “не ждал” и уходил на другой ключ;
- observed multi-key картина выглядит как routing drift между разными вызовами/потоками, а не как осознанный per-request anti-throttling.

### Если схлопнуть фичу в один ключ, вырастет ли minute pressure

Observed peak RPM per feature, если смотреть как будто вся фича жила бы на одном ключе:

- `smart_update`: peak `11 RPM`
- `event_parse`: peak `2 RPM`
- `event_topics`: peak `2 RPM`
- `bot_misc`: peak `12 RPM`
- весь `runtime` вместе: peak `13 RPM`
- весь `guide` вместе: peak `15 RPM`, минут выше `15` не было

Observed daily pressure:

- весь `runtime` вместе: average `453/day`, peak `958/day`
- весь `guide` вместе: average `582.26/day`, peak `1123/day`

Вывод:

- для `runtime` изоляция на один ключ по observed данным не должна сама по себе тормозить систему относительно `Gemma 4` free tier;
- для `guide` ситуация пограничная по minute peak, но observed history всё ещё не показала минут выше `15 RPM`;
- главный риск остаётся не latency от ожидания, а неверный routing contract и случайное смешивание чужих feature-bucket'ов.

### Дополнительный смягчающий фактор при переходе на Gemma 4

Лимитер считает квоты по паре `api_key_id + model`.

Практический смысл:

- `gemma-4-31b` и `gemma-4-26b-a4b` имеют отдельные quota-bucket'ы;
- если внутри одной фичи разделить простые и сложные стадии по двум моделям, они не обязаны конкурировать за один и тот же model bucket;
- это снижает риск self-throttling для `guide excursions monitoring` при staged migration.

## Что ещё нужно проверить перед фактическим switch

- подключён ли `GOOGLE_API_KEY3` не только как platform secret, но и в реальном runtime path `Telegram monitoring`;
- как именно будет фиксироваться quota/accounting для direct-SDK `Telegram monitoring`;
- почему `guide_extract_family` уходил в `GOOGLE_API_KEY`, а часть runtime-нагрузки уходила в `GOOGLE_API_KEY2`;
- quality/latency на `gemma-4-26b-a4b` против `gemma-4-31b` по реальным guide stages;
- финальный 7-day recheck `RPD/RPM` после нормализации key routing.

## Итог

Главный practical вывод исследования:

- переход на `Gemma 4` реалистичен не как один общий switch, а как staged rollout с жёстким разделением ключей;
- первым кандидатом на миграцию должен быть `guide excursions monitoring`;
- `Telegram monitoring` стоит вынести на отдельный `GOOGLE_API_KEY3`;
- до switch нужно убрать текущий routing drift и не путать gateway-tracked quota picture с direct-SDK Kaggle path.

## Update: implementation + smoke 2026-04-19

После research-фазы был выполнен первый реальный migration pass для `guide excursions monitoring` и серия live smoke-run'ов на локальном коде с Kaggle transport.

### Что уже подтверждено

- Kaggle guide path действительно запускается с `Gemma 4` split:
  - `trail_scout.screen.v1` -> `models/gemma-4-26b-a4b-it`
  - `trail_scout.*extract*` -> `models/gemma-4-31b-it`
- server-side guide path больше не должен неявно откатываться на `Gemma 3` из-за gateway model-chain policy: найден и исправлен баг, где `GoogleAIClient` переставлял любой Gemma text request так, что первой попыткой становилась `gemma-3-27b`;
- native `Gemma 4 response_schema` теперь реально доезжает до provider;
- provider-compatible schema subset для `Gemma 4` уже уточнён: из structured guide stages убран `additionalProperties`, потому что live provider возвращал `Unknown field for Schema: additionalProperties`.

### Что показали smoke-run'ы

- smoke v1:
  - Kaggle path и import доходят до конца;
  - обнаружен gateway bug: server-side `guide_occurrence_enrich` / `guide_profile_enrich` фактически шли в `gemma-3-27b`;
  - локальный import на dev-машине также упирался в `GUIDE_MEDIA_STORE_ROOT=/data`.
- smoke v2:
  - после фикса model-chain и writable media root исчезли `Gemma 3` fallback и local `/data` permission issue;
  - остался provider schema reject `Unknown field for Schema: additionalProperties`.
- smoke v3:
  - schema reject устранён;
  - итог: `llm_ok=2`, `llm_deferred=5`, `llm_error=0`;
  - run остался `partial`, но уже не из-за schema mismatch, а из-за `llm_deferred_timeout`.
- точечный timeout probe (`@tanja_from_koenigsberg`, только один source, `GUIDE_MONITORING_LLM_TIMEOUT_SEC=240`):
  - source всё равно завершился как `llm_deferred_timeout`;
  - значит простое повышение timeout с `120s` до `240s` само по себе не гарантирует устранение latency risk.

### Скорректированный rollout вывод

- first-stage migration для `guide monitoring` уже технически начат и основные transport/runtime blockers сняты;
- текущий открытый риск перед production switch — не квоты и не schema contract, а latency/timeout surface у `trail_scout.screen.v1` на части реальных постов;
- поэтому `guide excursions monitoring` остаётся правильным первым canary, но не выглядит готовым к безоговорочному full switch без дополнительного stage-level tuning.
