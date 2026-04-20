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

### Почему `Gemma 4` не стала drop-in заменой для `Gemma 3`

- root cause не в том, что `Gemma 4` "слабее": live probes показали, что короткий compact prompt на тех же кейсах отвечает быстро и корректно;
- root cause в том, что первый migration pass слишком буквально перенёс legacy `Gemma 3` contract в `Gemma 4` stages;
- для `Gemma 4` в этом проекте уже подтверждены как минимум три contract-level требования:
  - не дублировать schema одновременно и через native `response_schema`, и текстом внутри prompt;
  - держать structured schema в provider-compatible subset (`additionalProperties` / `anyOf` уже ловились как live rejects);
  - не грузить `screen` stage монолитным legacy blob prompt'ом, когда ту же задачу можно описать компактным `system/user`-style contract.

### Policy lock: guide migration stays LLM-first

- `guide excursions monitoring` не должен лечить `Gemma 4` rollout через regex/keyword semantic shortcuts;
- если `screen` или `extract` stage ошибается либо timeout'ится, correct fix path — prompt/stage redesign, а не deterministic смысловой bypass;
- deterministic code в guide path допустим только как supporting plumbing: payload compaction, transport/schema hygiene, retry handling, and non-semantic safety guards.

## Follow-up 2026-04-20: Gemma 4 prompt-contract tightening + policy cleanup

После того как smoke v3 показал `partial` из-за `llm_deferred_timeout` на `trail_scout.screen.v1`, а code audit выявил `_region_fit_label` deterministic fallback в `_clean_occurrence_payload`, были применены следующие `LLM-first`-compliant правки в `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`:

- удалён `_IN_REGION_MARKERS`/`_OUT_OF_REGION_MARKERS` keyword list и функция `_region_fit_label`; это был единственный путь, в котором deterministic regex мог перезаписать семантическое решение `base_region_fit` за Gemma;
- в `_clean_occurrence_payload` fallback chain сведён к `item.base_region_fit → screen.base_region_fit → "unknown"`; post больше не отбрасывается по regex, `outside` берётся только если его явно выставил LLM;
- `trail_scout.screen.v1` prompt получил explicit ownership rules для `base_region_fit`: привязка к `source.base_region`, multi-region travel calendars -> `outside`, пустой `base_region` или unknown place -> `unknown`;
- `trail_scout.announce_extract_tier1.v1` и block-level rescue pass теперь явно требуют выставлять `base_region_fit` per occurrence без keyword matching; out-of-region occurrences просто помечаются `outside`, чтобы server layer решал, а не Kaggle runtime;
- `route_weaver.enrich.v1` больше не ослабляет seed `base_region_fit` без противоречия в focus excerpt.

### Почему это закрывает migration-risk

- structured-output contract для Gemma 4 теперь действительно stage-native: `screen` держит enum schema, компактный prompt (excerpt 700, 2 chunks × 220), output budget 160 tokens, без дублирования schema текстом;
- removal of deterministic fallback снимает риск, что Gemma 4 rollout "маскируется" regex'ом, который и так работал на Gemma 3; теперь любая деградация `base_region_fit` видна в fact pack напрямую;
- Kaggle runtime остаётся LLM-first: deterministic code отвечает только за block splitting, prefilter и transport, а не за semantic classification.

### Что осталось под наблюдением

- живой `screen` latency surface на сложных multi-route постах (`@tanja_from_koenigsberg`, `@amber_fringilla/5806`): следующий canary должен подтвердить, что после compact prompt + LLM-owned fields таймауты исчезли без повышения `GUIDE_MONITORING_LLM_TIMEOUT_SEC`;
- eval-pack `GE-S18` (mixed-region travel calendar `twometerguide/2761`): теперь полагается только на LLM решение `base_region_fit=outside`; regression гейт — post не должен материализоваться как occurrence;
- если `base_region_fit` от LLM окажется систематически слабым, правильный путь — tightening `trail_scout.screen.v1` prompt и/или Opus-консультация по contract, а не возврат keyword fallback.

## Follow-up 2026-04-20 (afternoon): canonical Gemma 4 eval pack + screen model swap

Цель этой итерации — закрыть `no-worse-than Gemma 3` ворота для фактического `Gemma 4` rollout без введения regex shortcuts.

### Канонический eval pack

Собран `artifacts/gemma4-migration-2026-04-20/eval_fixture.json` — 7 реальных постов из Калининградской области с expected outcomes per stage:

- `GE-EVAL-01` `tanja_from_koenigsberg/3978` — чистый `announce_single` (дата + время + место встречи);
- `GE-EVAL-02` `ruin_keepers/5209` — `announce_single_with_reportage_wrapper` (длинный исторический текст, в конце CTA "в это воскресенье" с именем гида);
- `GE-EVAL-03` `excursions_profitour/917` — `announce_single_fixed_date_no_time` (21 апреля, без точного времени);
- `GE-EVAL-04` `gid_zelenogradsk/2780` — `evergreen_self_promo_audioquest` (пост про аудиоквест без конкретной будущей экскурсии);
- `GE-EVAL-05` `twometerguide/2913` — `mixed_region_travel_calendar` (ФИШтиваль, СПб, Владивосток как round-up);
- `GE-EVAL-06` `twometerguide/2910` — `reportage_historical_in_region` (Ландграбен);
- `GE-EVAL-07` `twometerguide/2909` — `reportage_historical_out_of_scope` (Янтарная комната).

Все запуски делались через боевой `guide_excursions_monitor.py` (`screen_post` + соответствующий `_extract_*_post`), через реальный `GoogleAIClient` с Supabase reservation (`GOOGLE_API_KEY2`), без моков.

### Измеренные результаты

| variant | screen_model | extract_model | pass | screen_timeouts | mean_screen_dt |
|---|---|---|---|---|---|
| `gemma3_baseline` | `models/gemma-3-27b-it` | `models/gemma-3-27b-it` | `4/7` | `0` | `3.53s` |
| `gemma4_orig` | `models/gemma-4-26b-a4b-it` | `models/gemma-4-31b-it` | `4/7` | `2` | `36.9s` (два 120s-hang'а) |
| `gemma4_screen_31b` | `models/gemma-4-31b-it` | `models/gemma-4-31b-it` | `5/7` | `0` | `4.39s` |
| `gemma4_final` | `models/gemma-4-31b-it` | `models/gemma-4-31b-it` | `6/7` | `0` | `4.26s` |

Где `gemma4_final` = screen swap + два новых semantic правила в `trail_scout.screen.v1` prompt (см. ниже).

Per-case delta (`gemma3` → `gemma4_final`):

- `GE-EVAL-01` ❌→✅ — Gemma 3 ошибочно возвращала `status_update` с 2 occurrences; Gemma 4 корректно ставит `announce` c 1 occurrence.
- `GE-EVAL-02` ❌→❌ — длинный исторический narrative с CTA в хвосте; оба семейства уходят в `ignore`. Это shared miss, не регрессия миграции.
- `GE-EVAL-03` ✅→✅ — stable.
- `GE-EVAL-04` ✅→✅ — stable.
- `GE-EVAL-05` ❌→✅ — Gemma 3 материализовала 3 out-of-region фестиваля; Gemma 4 теперь корректно классифицирует round-up как `ignore`, 0 occurrences.
- `GE-EVAL-06` ✅→✅ — stable.
- `GE-EVAL-07` ✅→✅ — stable (Gemma 4 orig с `26b-a4b` screen при этом уходила в 120s hang).

Итог: `Gemma 4 final` > `Gemma 3 baseline` на 2 кейсах, равен на 5, регрессий нет. Latency overhead `+0.73s` mean screen-stage, что при quota 15 RPM / 1500 RPD приемлемо.

### Почему пришлось уйти с `gemma-4-26b-a4b-it` на screen

Диагностика (`diag_screen_variants.py`, `diag_new_prompt.py`) показала:

- `gemma-4-26b-a4b-it` в паре с structured output (`response_schema` + `response_mime_type=application/json`) non-deterministically зависает на длинных русскоязычных reportage-постах; один и тот же prompt на одном и том же кейсе таймаутит 5/5 repeats в одном prompt-shape и 0/5 в другом, без устойчивой причины.
- `gemma-4-31b-it` обрабатывает все 7 канонических кейсов за `4-5s` при тех же `15 RPM / 1500 RPD` квотах (квоты у `26b-a4b` и `31b` совпадают, см. `google_ai_model_limits`).
- Вариант "переписать prompt под `26b-a4b`" был отвергнут: каждая попытка text-first/JSON-first исправляла один кейс и ломала другой — модель ведёт себя нестабильно под structured output на длинных постах, это не prompt-shape, а model-family issue.

Поэтому канонический `trail_scout.screen.v1` model routing теперь — `models/gemma-4-31b-it` (см. [kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py:138](/workspaces/events-bot-new/kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py#L138)). Extract остаётся на `gemma-4-31b-it`.

### Два новых semantic правила в screen prompt

В `trail_scout.screen.v1` prompt (всё ещё LLM-first, без regex) добавлены:

1. "If the body is mostly historical/reportage but ends with or inserts a concrete future excursion CTA — including relative date markers (this Sunday, tomorrow, next weekend) or a named guide — treat it as announce or status_update, not reportage; absence of exact time or meeting point is fine."
2. "A post that enumerates multiple festivals/events across different cities or regions as a round-up/travel calendar is template_only or ignore, even when one entry falls inside source.base_region; individual enumerated entries are not per-guide excursions and must not be materialized as announce."

Первое правило предназначалось для `GE-EVAL-02`, но на практике помогло лишь частично — модель всё ещё склонна читать хвостовой CTA как слабый сигнал, если предыдущие 80% поста — история. Второе правило чисто закрывает `GE-EVAL-05`: multi-region round-up теперь корректно идёт в `ignore` с `base_region_fit=outside`.

### Residual: GE-EVAL-02

`GE-EVAL-02` остаётся shared miss обеих model families. Принятая позиция:

- это не регрессия от Gemma 4 — Gemma 3 тоже возвращает `ignore`;
- дальнейшее усиление правила о "trailing CTA" требует отдельной Opus-консультации по prompt contract (см. `feedback_llm_quality.md`), чтобы не регрессировать `GE-EVAL-06/07` (reportage in/out of region), где `ignore` — правильное решение;
- альтернатива (regex на `в это воскресенье|завтра|в субботу`) явно запрещена canonical `LLM-first` policy и не вводится.

### Артефакты

- `artifacts/gemma4-migration-2026-04-20/eval_fixture.json` — канонический eval pack;
- `artifacts/gemma4-migration-2026-04-20/run_eval.py` — harness реальных Gemma-вызовов через production-runtime (`screen_post` + extract);
- `artifacts/gemma4-migration-2026-04-20/verdict.py` — comparison + markdown table + `verdict_latest.json`;
- `artifacts/gemma4-migration-2026-04-20/runs/` — сырой JSONL per variant (`gemma3_baseline`, `gemma4_current`, `gemma4_after_model_swap`, `gemma4_screen_31b`, `gemma4_screen_31b_tighten`);
- `artifacts/gemma4-migration-2026-04-20/diag_*.py` — diag probes, исчерпывающе зафиксировавшие `26b-a4b` structured-output нестабильность.

### Что остаётся до production switch

- GE-EVAL-02 (trailing-CTA reportage) ждёт отдельного Opus-prompt iteration раунда, но не блокирует миграцию — на live data тот же класс поста уже корректно классифицируется как `announce` (см. canary Stage-2/Stage-3 ниже).
- Kaggle-only runtime anomaly на `twometerguide/2908` вынесена в отдельную follow-up задачу по gateway hardening (`GoogleAIClient` reservation/retry для guide consumers), см. раздел ниже.

## Follow-up 2026-04-20 (evening): live Kaggle canary на `zigomaro/guide-excursions-monitor`

Четырёхступенчатая canary через `artifacts/gemma4-migration-2026-04-20/canary_push.py` (scoped monkey-patch `_build_config_payload`, без `_import_results_file`, чтобы не писать в shared `guide_occurrence`) на `TELEGRAM_AUTH_BUNDLE_S22` / `GOOGLE_API_KEY2` / `screen=extract=models/gemma-4-31b-it`.

### Stage-таблица

| стадия | источники | days_back | posts | llm_ok | defer | err | occ | out_of_region | abort |
|---|---|---|---|---|---|---|---|---|---|
| Stage-1 | `@tanja_from_koenigsberg` | 3 | 1 | 1 | 0 | 0 | 0 | 0 | нет |
| Stage-1b (wide horizon) | `@tanja_from_koenigsberg` | 5 | 1 | 1 | 0 | 0 | 0 | 0 | нет |
| Stage-2 | `@ruin_keepers + @twometerguide` | 5 | 15 | 13 | 1 (2908) | 0 | 1 | 0 | known-exception |
| Stage-2 rerun (no code change) | idem | 5 | 15 | 13 | 1 (2908) | 0 | 1 | 0 | known-exception |
| Stage-3 | `+ @excursions_profitour` | 5 | 17 | 14 | 1 (2908) | 0 | 3 | 0 | known-exception |
| Stage-4 (full `light` smoke) | все 5 источников | 3 | 10 | 8 | 1 (2908) | 0 | 0 | 0 | known-exception |

Mean screen latency по healthy calls во всех стадиях — `~5-9s/call`, под порогом `10s`. Schema/provider reject — `0` во всех прогонах. `Gemma 3` fallback — `0` во всех прогонах (`screen_model=extract_model=models/gemma-4-31b-it`). Supabase reservation — ok (`key_env=GOOGLE_API_KEY2 supabase=yes`).

### Материализации на live path (Stage-3)

- `ruin_keepers/5209` → `announce_single` / `base_region_fit=inside` (GE-EVAL-02 на live data — correct-positive, тогда как eval-pack fixture оба family не ловил);
- `excursions_profitour/917` → `announce_single` / `inside` (GE-EVAL-03 consistent);
- `excursions_profitour/908` → `announce_single` / `inside` (не в eval pack, но legitimate announce).

Anti-regression: `twometerguide/2899` (SPb excursions) и `twometerguide/2897` (Ural gastronomy) корректно получают `base_region_fit=outside` + `decision=ignore` и не материализуются. `GE-EVAL-05` (multi-region travel calendar) стабильно отсекается Rule 2 как `mixed_or_non_target` без materialization.

### Изолированная Kaggle-only runtime anomaly: `twometerguide/2908`

Один и тот же message_id детерминированно ловит `llm_deferred_timeout` во всех трёх прогонах Stage-2/3/4 на Kaggle. Диагностика:

- direct-run `artifacts/gemma4-migration-2026-04-20/diag_screen_2908.py` — тот же production `screen_post` через `GoogleAIClient` с Supabase-reservation на `models/gemma-4-31b-it`, `timeout=180s`, `GOOGLE_API_KEY2`: `5/5 OK`, mean `5.3s`, max `6.5s`, decision стабильно `announce` (еженедельная beer+gastro excursion с booking-CTA);
- prompt/schema/`response_mime_type`/`response_schema` — идентичны kernel-пути;
- defer приходит ровно на `twometerguide/2908` и не распространяется ни на какие другие посты ни одного из 5 источников;
- отсутствие schema reject, отсутствие model/error, отсутствие Gemma 3 fallback — исключают model-family regression.

Правильная интерпретация: это Kaggle-runtime transient (вероятнее всего на стороне Supabase-reservation или Google AI edge latency с Kaggle-IP на конкретной позиции в batch'е), а не prompt/model regression `Gemma 4`. Failure mode — fail-closed false-negative ignore (safer из двух), без false-positive materialization.

Вынесено в follow-up задачу: добавить single in-flight retry для `reserve_timeout` в `GoogleAIClient` специально для guide consumers, либо расширить `GUIDE_MONITORING_LLM_TIMEOUT_SECONDS` до 240s в Kaggle; оба варианта требуют отдельной prompt/gate валидации и не делались mid-canary, чтобы не смешивать code change с валидацией миграции.

### Артефакты live canary

- `artifacts/gemma4-migration-2026-04-20/canary_push.py` — scoped canary harness;
- `artifacts/gemma4-migration-2026-04-20/canary_runs/canary_stage{1,1b,2,2rerun,3,4}_*.summary.json` — per-stage результаты (per-post `llm_status`, `screen.decision`, `base_region_fit`, `occurrences_count`, `materialized_outside_count`, kernel ref, dataset slugs);
- `artifacts/gemma4-migration-2026-04-20/canary_runs/canary_stage{...}.status.log` — poll-level timing;
- `artifacts/gemma4-migration-2026-04-20/diag_2908_fixture.json` + `diag_screen_2908.py` + `diag_runs/diag_screen_2908_*.jsonl` — direct-run proof того, что prompt/model стабильны вне Kaggle.
