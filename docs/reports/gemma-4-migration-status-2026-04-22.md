# Gemma 4 Migration Status 2026-04-22

Статус: current repo-wide inventory

Назначение: зафиксировать полный на текущий момент обзор миграции на `Gemma 4` по репозиторию, включая уже существующую документацию, реально найденные Gemma-surface'ы в коде, текущий статус по каждой поверхности и пробелы, где каноника отсутствует или неполна.

Важно:

- этот документ появился потому, что до `2026-04-22` в репозитории уже были сильные частные документы по `guide-excursions` и `Smart Update / lollipop g4`, но не было одного канонического repo-wide инвентаря всех поверхностей, где всё ещё живёт `Gemma 3` или уже включён `Gemma 4`;
- ниже разделяется:
  - `есть документация по миграции / поверхности`;
  - `поверхность реально найдена в коде`;
  - `поверхность уже мигрирована или нет`.

## Какая документация по миграции уже есть

Найденные канонические документы:

- [docs/features/llm-gateway/README.md](/workspaces/events-bot-new/docs/features/llm-gateway/README.md) — общий runtime/gateway contract, лимиты, Gemma 4 structured output и known caveats;
- [docs/reports/gemma-4-migration-research-2026-04-19.md](/workspaces/events-bot-new/docs/reports/gemma-4-migration-research-2026-04-19.md) — research memo по ключам, квотам, migration order и guide canary/eval;
- [docs/features/guide-excursions-monitoring/README.md](/workspaces/events-bot-new/docs/features/guide-excursions-monitoring/README.md) — самая полная production-facing каноника по уже прошедшей миграции;
- [docs/reports/incidents/INC-2026-04-21-guide-gemma4-partial-monitoring.md](/workspaces/events-bot-new/docs/reports/incidents/INC-2026-04-21-guide-gemma4-partial-monitoring.md) — incident hardening после live rollout;
- [docs/llm/smart-update-lollipop-gemma-4-migration.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-migration.md) — canonical target architecture для `lollipop g4`;
- [docs/llm/smart-update-lollipop-gemma-4-eval.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-eval.md) — evaluation log по `lollipop g4`.

Практический вывод:

- документация по миграции в проекте была;
- но она была не repo-wide, а фрагментированная по двум главным трекам:
  - `guide-excursions`;
  - `Smart Update / lollipop g4`.

## Что реально использует Gemma в коде

Ниже перечислены product/runtime surfaces, найденные по коду на `2026-04-22`.

| Surface | Entry points / code | Current default | Transport | Status |
| --- | --- | --- | --- | --- |
| Guide excursions Kaggle monitor | `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py` | `models/gemma-4-31b-it` | `GoogleAIClient` | migrated to `Gemma 4` |
| Guide server enrich/dedup/digest | `guide_excursions/enrich.py`, `dedup.py`, `digest_writer.py` | `gemma-4-31b` | `GoogleAIClient` | migrated to `Gemma 4` |
| Smart Update core | `smart_event_update.py` | `gemma-3-27b-it` | `GoogleAIClient` | not migrated |
| Event parse (`/parse`, VK draft extraction, universal parser helper) | `main.py`, `vk_intake.py` | `gemma-3-27b-it` | `GoogleAIClient` | not migrated |
| Event topics | `main.py` | inherits `TG_MONITORING_TEXT_MODEL`, default `gemma-3-27b-it` | `GoogleAIClient` | not migrated |
| Admin action assistant | `handlers/admin_assist_cmd.py` | `gemma-3-27b` | `GoogleAIClient` | not migrated |
| Geo region fallback | `geo_region.py` | `gemma-3-27b` | bot Gemma client | not migrated |
| Telegram Monitoring Kaggle text/vision | `kaggle/TelegramMonitor/telegram_monitor.py`, `telegram_monitor.ipynb` | `models/gemma-4-31b-it` | `GoogleAIClient` | migrated; subset live Kaggle canary passed |
| Universal Festival Parser | `kaggle/UniversalFestivalParser/src/*` | `gemma-3-27b` / `models/gemma-3-27b-it` | direct `google.generativeai` | not migrated |
| Video announce poster completeness check | `video_announce/poster_overlay.py` | `gemma-3-27b` | `GoogleAIClient` | not migrated |

## Что уже сделано

### 1. Guide excursions — фактически завершённый first-wave rollout

Подтверждено по канонике и коду:

- Kaggle `screen/extract` уже на `Gemma 4` по умолчанию: [kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py](/workspaces/events-bot-new/kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py:137);
- server-side `enrich/dedup/digest_writer` уже на `Gemma 4`: [guide_excursions/enrich.py](/workspaces/events-bot-new/guide_excursions/enrich.py:19), `guide_excursions/dedup.py`, `guide_excursions/digest_writer.py`;
- gateway уже умеет `Gemma 4` structured output и filtering `parts[].thought=true`: [docs/features/llm-gateway/README.md](/workspaces/events-bot-new/docs/features/llm-gateway/README.md:104), [tests/test_google_ai_client.py](/workspaces/events-bot-new/tests/test_google_ai_client.py:31);
- schema-совместимость и timeout-bounded contract покрыты регрессиями: [tests/test_guide_kaggle_schema_contract.py](/workspaces/events-bot-new/tests/test_guide_kaggle_schema_contract.py:1), [tests/test_guide_local_llm_timeout_contract.py](/workspaces/events-bot-new/tests/test_guide_local_llm_timeout_contract.py:1), [tests/test_scheduling_guide_digest.py](/workspaces/events-bot-new/tests/test_scheduling_guide_digest.py:52).

Итог по этому треку:

- `guide-excursions` — единственная поверхность, где миграция на `Gemma 4` не только задокументирована, но и реально включена по умолчанию в коде, прошла eval/canary и потом incident hardening.

### 2. LLM gateway — готов как инфраструктурная база

Сделано:

- добавлены лимиты для `gemma-4-31b` и `gemma-4-26b-a4b`;
- gateway больше не переставляет requested Gemma model так, что первый hop тихо идёт в `gemma-3-27b`;
- `Gemma 4` caller может передавать native `response_schema` / `response_mime_type`;
- hidden thought-channel фильтруется до JSON parsing.

Это не означает, что весь проект мигрирован; это означает, что общая transport/runtime база уже не блокирует staged migration.

### 3. Smart Update / lollipop g4 — есть research и target architecture

Сделано в плане документации и lab-work:

- есть канонический migration contract для `Gemma 4 upstream + final 4o`;
- есть eval docs и benchmark harness;
- есть `CHANGELOG` и lab artifacts для `lollipop g4`.

Но важно:

- это ещё не migration фактического продового `smart_update.py`;
- production default в коде остаётся `gemma-3-27b-it`: [smart_event_update.py](/workspaces/events-bot-new/smart_event_update.py:146).

### 4. Telegram Monitoring — migrated, subset live canary passed

Сделано в текущем hardening wave:

- Kaggle producer переведён на `models/gemma-4-31b-it` для text/vision stages и shared `GoogleAIClient` с native `response_schema`;
- source metadata prompt запрещает сохранять social/profile links как `suggested_website_url`;
- extract prompt явно требует мерджить OCR/date/time/venue facts, не возвращать whitespace-only strings и не придумывать `end_date` для single-date events;
- generated Kaggle notebook embed-ит `google_ai` sources и запускает `main()` через `nest_asyncio`, что исправило два live-failure класса: `ModuleNotFoundError: google_ai` и `telegram_monitor.py should not be imported while an event loop is already running`;
- key isolation tightened: если `GOOGLE_API_KEY3` отсутствует в Supabase quota registry, gateway uses process-local limiter with `GOOGLE_API_KEY3` instead of silently falling through to the shared key pool;
- post-canary hardening added: Telegram Monitoring Kaggle secrets no longer ship unrelated `GOOGLE_API_KEY*` pools, empty scoped-key cache no longer widens to unscoped reserve on later calls, and Gemma 4 provider calls are bounded by `TG_MONITORING_LLM_TIMEOUT_SECONDS` / `GOOGLE_AI_PROVIDER_TIMEOUT_SEC` (default `45s` after full-run evidence showed successful calls stay below ~34s while stalled calls waste the schedule window).

Live evidence (`2026-04-22`):

- Kaggle run `tg_g4_live_smoke_subset_20260422g` produced `telegram_results.json` with `schema_version=2`, `sources_total=3`, `messages_scanned=2`, `messages_with_events=1`, `events_extracted=4`;
- Kaggle log confirms `requested_model/provider_model/invoked_model=models/gemma-4-31b-it`; no Gemma 3 model fallback was observed;
- server recovery import `ops_run id=797` finished `success`, `errors_count=0`; repeat import-only `id=798` also finished `success`, `errors_count=0`.
- scheduled full run `48fa98294333486d94dd0e14785d774f` produced full Kaggle output on 45 sources (`messages_scanned=177`, `messages_with_events=69`, `events_extracted=84`) and recovery import `ops_run id=803` finished `success`, `errors_count=0`, `events_imported=14`; log evidence shows `GOOGLE_API_KEY3`, `GOOGLE_API_KEY2=0`, `gemma-3=0`, and `models/gemma-4-31b-it` for requested/provider/invoked model.
- post-`45s` smoke `tg_g4_45s_smoke_20260423a` finished through primary `ops_run id=807` as `success` without recovery (`sources_scanned=3`, `messages_processed=3`, `messages_with_events=2`, `errors_count=0`, `duration_sec=279.22`) and confirmed fast fail-open `45s` source-metadata timeouts with `GOOGLE_API_KEY2=0`, `gemma-3=0`, `Traceback=0`.

Оставшийся caveat:

- full scheduled run succeeded through recovery, and the post-`45s` manual smoke proves the primary `ops_run` can finish `success` without recovery. Remaining production watch item: the next natural scheduled all-source run should be observed once with the `45s` default to confirm the full schedule also stays inside the primary poll window.

## Что ещё не сделано

### 1. Smart Update core не переведён

Факты:

- код по умолчанию всё ещё использует `SMART_UPDATE_MODEL=gemma-3-27b-it`: [smart_event_update.py](/workspaces/events-bot-new/smart_event_update.py:146);
- docs для migration target есть, но это planning/research track, а не delivered runtime switch: [docs/llm/smart-update-lollipop-gemma-4-migration.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-migration.md:1).

### 2. Event parse и все его потребители не переведены

Сюда входят:

- `/parse` и универсальный event parse path в `main.py`: `EVENT_PARSE_GEMMA_MODEL=gemma-3-27b-it` по умолчанию: [main.py](/workspaces/events-bot-new/main.py:8914);
- `vk_intake.build_event_drafts()` использует тот же parser/backend и потому остаётся в том же migration bucket;
- source parsing docs описывают Gemma-path и fallback, но не содержат отдельного Gemma 4 rollout plan для этого surface.

### 3. Event topics не переведён

Факты:

- `EVENT_TOPICS_MODEL` по умолчанию наследует `TG_MONITORING_TEXT_MODEL`, который в Kaggle всё ещё `gemma-3-27b-it`: [main.py](/workspaces/events-bot-new/main.py:2201), [kaggle/TelegramMonitor/telegram_monitor.ipynb](/workspaces/events-bot-new/kaggle/TelegramMonitor/telegram_monitor.ipynb:169);
- docs по темам есть, но migration-specific каноники нет: [docs/llm/topics.md](/workspaces/events-bot-new/docs/llm/topics.md:37).

### 4. Telegram Monitoring full scheduled rollout ещё не закрыт

Факты:

- subset live canary passed (см. раздел выше);
- full scheduled run на всех источниках после hardening ещё нужно прогнать и сравнить с recent Gemma 3 baseline по import volume/ошибкам;
- Supabase quota registry нужно синхронизировать с `GOOGLE_API_KEY3`, чтобы межсервисный лимитер видел primary key row; код уже защищён от silent shared-pool fallback, но registry sync вернёт централизованный accounting.

### 5. Universal Festival Parser не переведён

Факты:

- каноническая дока явно говорит `Playwright + Gemma 3-27B`: [docs/features/source-parsing/sources/festival-parser/README.md](/workspaces/events-bot-new/docs/features/source-parsing/sources/festival-parser/README.md:5);
- runtime config default остаётся `gemma-3-27b`: [kaggle/UniversalFestivalParser/src/config.py](/workspaces/events-bot-new/kaggle/UniversalFestivalParser/src/config.py:35);
- reason step использует direct `google.generativeai` с `models/gemma-3-27b-it`: [kaggle/UniversalFestivalParser/src/reason.py](/workspaces/events-bot-new/kaggle/UniversalFestivalParser/src/reason.py:279).

### 6. Admin assistant, geo-region fallback и video-announce poster check не переведены

Факты:

- admin assistant: `gemma-3-27b`: [handlers/admin_assist_cmd.py](/workspaces/events-bot-new/handlers/admin_assist_cmd.py:1161), docs: [docs/features/admin-action-assistant/README.md](/workspaces/events-bot-new/docs/features/admin-action-assistant/README.md:85);
- geo-region fallback: `gemma-3-27b`: [geo_region.py](/workspaces/events-bot-new/geo_region.py:220), docs: [docs/features/geo-region-filter/README.md](/workspaces/events-bot-new/docs/features/geo-region-filter/README.md:10);
- video announce poster completeness: `gemma-3-27b`: [video_announce/poster_overlay.py](/workspaces/events-bot-new/video_announce/poster_overlay.py:24), docs: [docs/features/crumple-video/README.md](/workspaces/events-bot-new/docs/features/crumple-video/README.md:111).

## Где документации не хватало

На момент этой фиксации в проекте не было одного канонического документа, который одновременно отвечал бы на четыре вопроса:

1. Какие именно документы по Gemma 4 migration уже существуют.
2. Какие product/runtime surfaces реально используют Gemma сейчас.
3. Какие из них уже переведены на `Gemma 4`, а какие нет.
4. Какие поверхности сидят на `GoogleAIClient`, а какие всё ещё идут direct `google.generativeai`.

Из-за этого было легко ошибочно читать `guide-excursions` migration как будто это и есть весь repo-wide migration status.

## Что нужно делать дальше

### Wave 1: закончить inventory-to-contract по оставшимся поверхностям

Нужно оформить отдельные migration decisions хотя бы для этих bucket'ов:

- `Smart Update core` / `event_parse` / `vk_intake` как один большой runtime family на `GOOGLE_API_KEY`;
- `Telegram Monitoring Kaggle` как отдельный direct-SDK surface;
- `Universal Festival Parser` как отдельный Kaggle direct-SDK surface;
- `small bot surfaces`: `admin_assist`, `geo_region`, `video_announce poster check`.

### Wave 2: мигрировать direct-SDK surfaces или явно зафиксировать, что они остаются на Gemma 3

Сейчас главный недокументированный operational risk не в `guide`, а в том, что несколько Kaggle/auxiliary surfaces вообще обходят `google_ai` gateway accounting:

- `TelegramMonitor`;
- `UniversalFestivalParser`;
- отдельные lab/benchmark harnesses.

По ним нужно отдельно решить:

- перевод на `GoogleAIClient`;
- либо явная фиксация, что surface пока остаётся на `Gemma 3` и не входит в current Gemma 4 rollout.

### Wave 3: Smart Update только staged, не “общим switch”

Для `Smart Update` канонический следующий шаг остаётся прежним:

- не переводить final writer на Gemma 4;
- двигаться только по схеме `Gemma 4 upstream + final 4o`;
- принимать rollout only after real eval/canary на продовых family stages, а не по writer-swap experiments.

## Короткий итог

- документация по миграции в проекте была, но не полная repo-wide;
- реально завершён и production-proven только `guide-excursions`;
- `Smart Update`, `event_parse`, `event_topics`, `Universal Festival Parser`, `admin_assist`, `geo_region`, `video_announce` всё ещё не переведены на `Gemma 4` по умолчанию;
- `Telegram Monitoring` уже переведён в коде, покрыт локальными контрактами и прошёл subset live Kaggle canary; full scheduled rollout ещё требует отдельного all-source прогона;
- этот документ фиксирует текущий repo-wide status, которого раньше в канонике не было.
