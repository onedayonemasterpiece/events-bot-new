# Guide Excursions Monitoring

Статус: fact-first MVP in progress

Канонический surface для мониторинга экскурсионных анонсов гидов в Telegram. Трек живёт в отдельных `guide_*` таблицах и не попадает в обычные `event`/`/daily`/month/weekend surfaces.

## Каноническая runtime boundary

Текущая каноническая граница совпадает с backlog-доками:

- `Kaggle notebook` делает Telegram fetch, grouped albums, deterministic prefilter и `Tier 1` extraction;
- multi-announce posts inside Kaggle сначала режутся на `occurrence_blocks`, после чего Gemma extraction обязана вернуть несколько отдельных occurrences по разным датам/маршрутам, а uncovered schedule blocks добираются block-level rescue pass'ом;
- `trail_scout.screen.v1` оценивает пост целиком и не получает `occurrence_blocks` как вход; block split используется только на extraction stage, чтобы screen не подхватывал детерминированное мнение сплиттера;
- guide extraction идёт Opus/lollipop-style семействами, а не одним тяжёлым универсальным prompt'ом: `trail_scout.announce_extract_tier1.v1` вытаскивает только occurrence skeleton, `trail_scout.status_claim_extract.v1` обрабатывает update-посты, `trail_scout.template_extract.v1` собирает template-only сигналы, а `route_weaver.enrich.v1` отдельным коротким запросом дозаполняет семантические поля по уже найденному occurrence;
- TPM-профилирование должно лечиться именно таким stage split'ом; просто “ужимать payload, пока проходит” не считается каноническим решением, если при этом теряется ранее доступная семантика;
- live Gemma output может формально вернуть bare JSON array вместо обёртки `{"occurrences":[...]}`; prompt всё равно требует object-wrapper, но runtime обязан считать bare array валидным extraction contract и не терять из-за этого найденные excursions;
- серверный runtime импортирует результат notebook в `GuideProfile / ExcursionTemplate / ExcursionOccurrence / GuideFactClaim`;
- digest preview/publish строятся уже на сервере из materialized fact pack;
- Kaggle scan materialize-ит source media в output bundle, server import копирует файлы в persistent store `GUIDE_MEDIA_STORE_ROOT` (по умолчанию `/data/guide_media`), а publish использует только эти сохранённые assets;
- локальный Telethon scan остаётся только аварийным fallback и включается через `GUIDE_EXCURSIONS_LOCAL_FALLBACK_ENABLED=1`;
- live E2E намеренно ставит `GUIDE_EXCURSIONS_LOCAL_FALLBACK_ENABLED=0`, чтобы падение Kaggle/Gemma path не маскировалось `partial`-успехом.
- daily horizon для уже прогретого guide-track остаётся коротким: `GUIDE_DAYS_BACK_FULL=5`, `GUIDE_DAYS_BACK_LIGHT=3`, но первый `full` run на пустой базе теперь автоматически расширяет post horizon до `GUIDE_DAYS_BACK_BOOTSTRAP=14`, чтобы bootstrap digest не терял будущие экскурсии из постов, опубликованных несколько дней назад;

Для guide-track LLM path должен быть только Gemma-only:

- Kaggle extraction использует `GoogleAIClient` + Supabase-backed limiter с primary secret `GOOGLE_API_KEY2` и guide account label `GOOGLE_API_LOCALNAME2`;
- default model split для первого production migration на `Gemma 4` такой:
  - `trail_scout.screen.v1` -> `models/gemma-4-31b-it` (канонический screen с `2026-04-20` eval; `26b-a4b-it` был признан нестабильным на длинных русскоязычных reportage-постах — non-deterministic hang ≥120s под structured output)
  - `trail_scout.*extract*` + `route_weaver.enrich.v1` -> `models/gemma-4-31b-it`
  - server-side `guide_occurrence_enrich`, `guide_profile_enrich`, `guide_excursions_dedup`, `guide_excursions_digest_batch` -> `gemma-4-31b`
- для Telegram auth Kaggle guide path по умолчанию использует только `TELEGRAM_AUTH_BUNDLE_S22`; локальная `TELEGRAM_AUTH_BUNDLE_E2E` не считается допустимым автоматическим fallback и может быть использована только через явный аварийный override;
- перед запуском guide Kaggle kernel сервер обязан проверить общий `kaggle_registry`: если другой remote Telegram job (`tg_monitoring`, `guide_monitoring`, `telegraph_cache_probe`) ещё жив или его Kaggle status не удалось надёжно прочитать, guide run должен завершиться `skipped` с явной диагностикой `remote_telegram_session_busy`, а не запускать вторую Telethon session;
- guide digest publish-time fallback для media запрещён: bot-side `forward -> file_id`, Telethon download и public-web scraping не считаются каноническим путём; если materialized assets не доехали из Kaggle/import path, publish должен останавливаться явно, а не деградировать до text-only поста.
- server-side guide Gemma path (`enrich`, `dedup`, `digest_writer`) тоже обязан идти как fixed-key consumer: runtime резолвит `candidate_key_ids` из `google_ai_api_keys.env_var_name` и сначала пытается зарезервировать именно `GOOGLE_API_KEY2` / `GOOGLE_API_KEY_2`, а на `GOOGLE_API_KEY` откатывается только если для primary guide key metadata ещё не заведена в Supabase;
- server-side dedup/editorial тоже сидят на Gemma-конфигах;
- `4o` для guide pipeline не используется.
- mixed-region sources не дают “автоматического доверия по региону”: generic/out-of-region travel calendars должны отсеиваться ещё в Kaggle extraction и не materialize'иться как guide occurrence.
- first-pass `base_region_fit` теперь относится к screen/extraction LLM layer, а не к смысловым regex в local fallback.
- `base_region_fit` считается полностью Gemma-owned semantic decision: Kaggle runtime больше не держит deterministic keyword fallback по городам; если LLM не заполнил поле, результат остаётся `unknown` и post не отбрасывается по regex, а обрабатывается server-side enrichment стадиями с LLM-first ownership.
- `Gemma 4` structured stages в guide path должны использовать native `response_schema` / `response_mime_type`, а не только prompt-level "верни JSON" contract.
- runtime обязан фильтровать `parts[].thought = true` до JSON parsing и materialization, чтобы guide fact-pack и admin surfaces не протаскивали hidden reasoning text.
- guide migration остаётся строго `LLM-first`: semantic screen/extract decisions не должны переезжать в regex/keyword shortcuts даже если конкретный `Gemma 4` stage ведёт себя хуже baseline; в таких случаях исправляется prompt/stage contract, а не вводится deterministic bypass по смыслу текста.

## Что уже мигрировано

- отдельный guide-track в основной SQLite;
- seed-пак Telegram-источников из casebook;
- seed-пак guide-источников теперь также включает `@art_from_the_Baltic` как provisional `guide_project` source и `@jeeptours39` как branded off-road / jeep-tour source;
- guide-specific Kaggle runtime: [kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py](/workspaces/events-bot-new/kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py);
- secure Kaggle push/poll/download через тот же split-secrets pattern, что и в Telegram Monitoring;
- guide Kaggle transport теперь повторяет продовый Telegram Monitoring pattern: kernel push содержит нужный `google_ai/` код сразу, а secrets по-прежнему идут только через два отдельных datasets (`cipher + key`), без третьего payload dataset;
- канонический guide kernel должен оставаться одним и тем же (`zigomaro/guide-excursions-monitor`) и жить через Kaggle versioning; `GUIDE_MONITORING_KERNEL_SLUG` оставлен только как ручной аварийный override, а не как обычный E2E path;
- server import с materialization `fact_pack_json` и `GuideFactClaim.claim_role/provenance/observed_at/last_confirmed_at`;
- server-side digest eligibility теперь остаётся fact-first: undated / cancelled / private occurrences fail-closed, но `limited`-объявления с реальной будущей датой и достаточным набором публичных фактов (`time/city/meeting/route/price/booking/summary`) могут быть повышены до digest-ready вместо немого выпадения из daily выпуска;
- digest/editorial, где `fact_pack` считается primary truth source, а исходный post text используется только как secondary evidence;
- guide-specific `Lollipop Trails` fork для digest copy: Gemma batch-пишет `title`, `digest_blurb` и короткие public lines (`Гид` / `Организатор`, `Цена`, `Что в маршруте`, `Запись`, house-line про local-vs-tourist fit) из materialized fact pack + guide profile rollup;
- migration `2026-04-19`: guide path переведён на `Gemma 4` defaults с quality-first split (`26b-a4b` для screen, `31b` для extract/enrich/writer), а Kaggle/server structured stages теперь передают native `response_schema` вместо чисто prompt-only JSON режима;
- live smoke `2026-04-19` подтвердил, что guide Kaggle path реально стартует и доходит до `Gemma 4` stages на `GOOGLE_API_KEY2`, а server import больше не откатывается на `gemma-3-27b` из-за gateway model-chain bug;
- тот же smoke выявил текущий rollout risk: часть posts на `trail_scout.screen.v1` всё ещё уходит в `llm_deferred_timeout` даже при `GUIDE_MONITORING_LLM_TIMEOUT_SEC=240`, поэтому migration уже технически поднят, но production switch должен оставаться staged/canary, пока не будет снижен screen-stage latency surface;
- canonical interpretation этого риска: проблема не в том, что `Gemma 4` "хуже `Gemma 3`", а в том, что legacy `Gemma 3` prompt-contract не оказался drop-in совместимым с `Gemma 4`; structured output у `Gemma 4` рабочий, но требует более нативного stage contract (`system/user` split, компактный payload, provider-compatible schema subset, без дублирования schema текстом в prompt);
- follow-up `2026-04-20`: canonical `Gemma 4` eval pack `GE-EVAL-01..07` на 7 реальных постах (Калининградская область, базовые классы: announce_single, announce_with_reportage_wrapper, announce_fixed_date_no_time, evergreen_self_promo, mixed_region_travel_calendar, reportage_in_region, reportage_out_of_scope) подтвердил, что `Gemma 4` после правок строго `no-worse` относительно `Gemma 3` baseline: Gemma 3 — `4/7` pass, Gemma 4 final (`screen=31b` + два новых semantic правила в `trail_scout.screen.v1`) — `6/7` pass, `0` timeouts, mean screen latency `4.26s` (Gemma 3 — `3.53s`); единственный shared miss — `GE-EVAL-02` (announce спрятан в хвост длинного исторического reportage), который также не ловит `Gemma 3`;
- оба улучшения чисто LLM-first: добавлены семантические правила "trailing-CTA в reportage — не reportage, а announce/status_update" и "multi-region festival round-up не материализуется как announce даже если один из пунктов внутри base_region"; в runtime Kaggle code не появилось ни одного нового regex/keyword shortcut;
- live Kaggle canary `2026-04-20` на `zigomaro/guide-excursions-monitor` (`TELEGRAM_AUTH_BUNDLE_S22`, `GOOGLE_API_KEY2`, screen/extract = `models/gemma-4-31b-it`) прошла четыре стадии — positive-control, borderline negative, mixed, full `light` smoke. Во всех прогонах: `0 llm_error`, `0 schema/provider reject`, `0 Gemma 3 fallback`, `0 out-of-region materializations`, mean screen latency ~5-9s; на Stage-3 live path корректно материализовал `GE-EVAL-02` (ruin_keepers/5209 announce/inside) и `GE-EVAL-03` (excursions_profitour/917 announce/inside). Единственное наблюдаемое отклонение — изолированный `llm_deferred_timeout` на `twometerguide/2908`, который детерминированно повторяется ровно на этом message_id только в Kaggle runtime, тогда как direct-run того же `screen_post` через production `GoogleAIClient` / Supabase reservation на `models/gemma-4-31b-it` стабилен `5/5` @ ~`5.3s` со стабильным решением `announce`; это задокументировано как Kaggle-only runtime transient (fail-closed false-negative ignore), а не как prompt/model regression `Gemma 4`, и вынесено в follow-up задачу на Kaggle gateway/runtime hardening;
- materialized fact pack теперь явно несёт `duration_text`, `route_summary`, `group_format` и `base_region_fit`, чтобы оператор видел не только title/date/booking, но и что именно было извлечено про маршрут, длительность и формат участия;
- эти richer semantic fields считаются Gemma-owned: local fallback parser может резать пост на блоки и вытаскивать базовые operational facts, но не должен эвристически материализовывать `duration_text`, `route_summary` или `group_format` без LLM extraction;
- inspectability фактов через `/guide_facts <occurrence_id>`;
- Smart Update-style operator reporting через `ops_run` + `/guide_report [ops_run_id]` + `/guide_runs [hours]`;
- source-log analogue `/guide_log <occurrence_id>` с source posts и occurrence-level claim provenance;
- исключение past occurrences в MVP;
- preview/publish digest в канал(ы) из `GUIDE_DIGEST_TARGET_CHATS` (legacy-primary остаётся в `GUIDE_DIGEST_TARGET_CHAT`); текущие runtime targets: `@wheretogo39`, `@youwillsee39`;
- runtime semantic dedup перед render/publish;
- отдельный блок в `/general_stats`;
- env-gated scheduler для `light` и `full` прогонов.

## Что ещё остаётся MVP-ограничением

- OCR в guide Kaggle runtime пока не доведён до backlog-parity;
- `status_bind / reschedule / same-occurrence` merge уже fact-first, но ещё не полный `Route Weaver v1`;
- profile enrichment теперь отдельно materialize-ится Gemma-pass'ом из `guide_source.about_text` + sample occurrence titles/hooks, чтобы `guide_profile` копил публичное имя, регалии и области экспертизы;
- template rollup по-прежнему строится в основном из occurrence-linked hints/facts; отдельного template-only harvesting pipeline пока нет;
- local fallback специально сохранён для обратной совместимости, но он не считается каноническим путём.

Аудит расхождения старого regex-heavy MVP и текущего migration plan: [guide-excursions-fact-first-audit-2026-03-15.md](/workspaces/events-bot-new/docs/reports/guide-excursions-fact-first-audit-2026-03-15.md)

## Команды

Guide admin surface подключается в основной bot runtime через `guide_excursions.commands.guide_excursions_router`; отсутствие этого router import/registration в `create_app()` считается startup-blocking prod regression, а не допустимой деградацией feature-surface.

- `/guide_excursions` — основное меню управления;
- `/guide_sources` — список источников и текущее покрытие;
- `/guide_events [page]` — список всех будущих occurrences с inline delete/facts/log actions;
- `/guide_templates [page]` — список `GuideTemplate` / типовых экскурсий с возможностью удалить устаревший template;
- `/guide_template <id>` — детальный просмотр одного `GuideTemplate`: accumulated route facts, hooks, locals/tourists/mixed rollup и связанные occurrences;
- `/guide_recent` — preview `new_occurrences` и быстрый список `occurrence_id`;
- `/guide_recent_changes [hours]` — какие occurrences были созданы, а какие обновлены за недавнее окно;
- `/guide_runs [hours]` — последние guide monitoring runs с `ops_run_id`;
- `/guide_report [ops_run_id]` — детальный run report: transport, источники, посты, created/updated occurrence ids, ошибки;
- `/guide_facts <occurrence_id>` — materialized fact pack и `GuideFactClaim` по конкретной карточке;
- `/guide_log <occurrence_id>` — source-post / claim log для конкретной карточки, аналог `/log` у Smart Update;
- `/guide_digest` — publish текущего digest во все каналы из `GUIDE_DIGEST_TARGET_CHATS`.

## Admin observability

Guide track должен быть проверяемым так же, как Smart Update:

- каждый scan пишет `ops_run(kind='guide_monitoring')` с `details_json.source_reports[]` и `details_json.occurrence_changes[]`;
- если scheduled run ломается ещё до входа в основной scan/import path (например, на bootstrap/import слое scheduler), это всё равно должно materialize-иться как `ops_run(kind='guide_monitoring', status='error')` с явным `details_json.transport='bootstrap_error'`, а не выглядеть как “пропущенный без следа слот”;
- `/guide_report` показывает `ops_run_id`, transport-path, источники, посты, `llm_status`, created/updated occurrence ids и source post labels вида `@channel/1234`;
- `/guide_report` и `/guide_runs` дополнительно показывают `llm_ok / llm_deferred / llm_error`, чтобы оператор видел реальный объём Gemma-вызовов и deferred по лимитам;
- `/guide_runs` даёт короткий список последних прогонов и команду-переход `/guide_report <ops_run_id>`;
- `/guide_events` даёт оператору отдельный future inventory guide-track, а не только digest-preview; из списка можно сразу удалить occurrence или открыть её facts/log;
- в `/guide_events` рядом с source label показывается отдельная строка `🔗 https://t.me/...`, чтобы исходный post URL был реально кликабелен в Telegram UI, а не только как текстовый `@channel/123`;
- `/guide_templates` даёт отдельный inventory типовых экскурсий (`GuideTemplate`) с количеством связанных/future occurrences;
- `/guide_template <id>` показывает, как именно копится типовая информация по маршруту: `facts_rollup_json`, main hooks, route summaries, locals/tourists/mixed vote rollup и связанные occurrences;
- `/guide_recent_changes` показывает created vs updated occurrences за окно, чтобы можно было быстро проверить, что мониторинг действительно добавил новое, а что только обновил;
- `/guide_facts` показывает materialized fact pack и occurrence-level claims;
- `/guide_log` показывает связанные source posts и provenance каждого claim, чтобы руками проверить, из какого поста и когда пришёл конкретный факт.

## Надёжность Kaggle polling

- import-time guide Kaggle config должен быть blank-safe: пустые numeric env overrides (`GUIDE_MONITORING_*`) не должны валить импорт guide runtime, а обязаны откатываться к documented defaults с warning в логах;
- Статус guide kernel опрашивается с интервалом `GUIDE_MONITORING_POLL_INTERVAL` до динамического лимита ожидания по числу источников.
- Транзиентные ошибки Kaggle API на polling (`SSL`, сеть, timeout) не должны валить guide run сразу: бот продолжает опрос и показывает в status-update, что это временная ошибка сети.
- При скачивании output сервер дополнительно валидирует `run_id` внутри `guide_excursions_results.json`; stale output от предыдущей версии kernel не должен импортироваться как свежий scan.
- Перед polling сервер теперь дополнительно проверяет shape канонического Kaggle kernel: `zigomaro/guide-excursions-monitor` обязан оставаться `kernel_type=notebook` с notebook `code_file`. Если remote kernel внезапно стал `script`, run должен падать сразу с явной инструкцией пересоздать канонический notebook, а не зависать на stale output.
- Сам guide notebook runner теперь тоже fail-closed по auth boundary: если `TELEGRAM_AUTH_BUNDLE_S22` отсутствует, а в окружении есть только `TELEGRAM_AUTH_BUNDLE_E2E`, `_resolve_auth_bundle()` обязан упасть с явной ошибкой вместо тихого borrow чужой сессии; non-`S22` auth допустим только через явный low-level override `GUIDE_MONITORING_ALLOW_NON_S22_AUTH=1`.

## Recovery после рестарта бота

- `guide_monitoring` регистрирует pushed kernel в общем `kaggle_registry` сразу после успешного `push`.
- После успешного download сервер копирует весь output bundle в persistent store `GUIDE_MONITORING_RESULTS_STORE_ROOT` (по умолчанию `/data/guide_monitoring_results`) и пишет `results_path` в recovery meta, чтобы рестарт во время server-import или scheduled auto-publish не терял уже готовый результат.
- Scheduler `kaggle_recovery` проверяет и `guide_monitoring`, так же как остальные Kaggle jobs:
  - если kernel ещё работает, запись остаётся в реестре;
  - если output уже был скачан до рестарта, recovery сначала поднимает import из сохранённого `results_path` без повторного запроса в Kaggle;
  - если kernel завершился `complete`, а локального persisted bundle ещё нет, бот заново скачивает `guide_excursions_results.json` из Kaggle и запускает обычный server-import;
  - для scheduled `full` run с `ENABLE_GUIDE_DIGEST_SCHEDULED=1` recovery должен дотягивать не только import, но и тот же auto-publish `new_occurrences`, если процесс упал между этими фазами;
  - если kernel завершился `failed/error/cancelled`, запись удаляется из реестра и оператор получает уведомление.
- Источником истины для recovery по-прежнему остаётся Kaggle output; локальный persisted bundle считается лишь durable-копией уже скачанного canonical output и нужен только для того, чтобы рестарт не обнулял фазу import/publish.

`/general_stats` для guide-track теперь должен показывать не только источники/прогоны, но и:

- `occurrences_new` и `occurrences_updated` за окно;
- `occurrences_future_now` как текущий future inventory;
- `templates_total` как текущий объём типовых экскурсий;
- published digest count и guide-monitoring runs.

## Digest copy policy

Guide digest не должен скатываться ни в regex-heavy шаблонизатор, ни в свободный rewrite raw source text.

Текущая каноника для live MVP:

- server-side digest writer реализован как guide-specific fork `Lollipop Trails v1`;
- перед writer batch сервер теперь делает два маленьких Gemma-only enrich pass'а поверх materialized fact pack:
  - `main_hook`
  - `audience_region_fit` (`locals | tourists | mixed`);
- runtime нормализует `audience_region_fit` score fields к процентной шкале даже если Gemma в ответе сжимает их до `0..10`, чтобы сигнал не пропадал из fact pack/admin surfaces;
- enrich-batches intentionally kept small and retry one explicit provider `retry after ... ms` hint, so `main_hook` и `audience_region_fit` не расходятся из-за второго подряд TPM-удара;
- Gemma batch-call получает только materialized `fact_pack` + короткие precomputed hints, без полного raw source prose как primary input;
- Gemma пишет authorial части карточки из materialized facts:
  - `title`
  - `digest_blurb`
  - короткие public lines для `Гид`, `Цена`, `Что в маршруте`, `Кому больше`;
- `audience_region_line` теперь пишется как самостоятельная house-line без префикса `Кому больше:` и должна описывать именно local-vs-tourist fit по региону, а не дублировать возрастную/групповую `👥 Кому подойдёт` строку;
- date/city/meeting-point/booking/seats по-прежнему рендерятся из сохранённых фактов, но public phrasing для human-facing semantic lines больше не должна сваливаться в raw regex-copy;
- если human-facing `price_line` не был нормально переписан Gemma, public shell должен скрыть сырой slash-copy вида `500/300 руб взрослые/дети,пенсионеры`, а не публиковать его как будто это clean LLM output;
- writer теперь может вернуть отдельный `lead_emoji`, и если он grounded в теме маршрута (`🐦`, `🏛️`, `🌲`, `🧱` и т.п.), digest использует именно его, а не generic engagement-mark;
- `Lollipop Trails` writer режет publish на более мелкие fixed-key batch'и и готов переждать несколько явных provider `retry after ... ms` в пределах bounded wait window, чтобы preview/publish не срывались из-за одной тяжёлой TPM-минуты сразу после scan;
- shell теперь рендерит не только logistics, но и richer fact lines:
  - `Гид`
  - `Локация`
  - standalone house-line про fit для местных/гостей без префикса `Кому больше:`
  - `Кому подойдёт`
  - `Формат`
  - `Что в маршруте`
  - `Продолжительность`
  - `Место сбора`
- placeholder и мусорные pseudo-facts вроде `одна дата` не должны попадать в публичную карточку;
- если в `guide_profile` уже накоплена Gemma-derived строка `guide_line`, digest должен брать её как preferred public profile surface вместо channel brand/username;
- для non-personal source kinds (`guide_project`, `organization_with_tours`, `excursion_operator`, `aggregator`) даже такая `guide_profile.guide_line` может публиковаться только если тот же человек подтверждён occurrence-level facts (`guide_names`) или явно присутствует в grounded excerpt конкретного поста; profile/about сами по себе не дают права публиковать личную строку `Гид`;
- если occurrence уже несёт несколько `guide_names`, public shell должен показывать plural line `👥 Гиды: ...`, а не схлопывать карточку обратно до одного primary guide profile;
- если надёжного конкретного гида нет, public shell не должен подменять организацию полем `Гид`; допускается отдельная строка `🏢 Организатор: ...`, а при слабой идентичности line лучше скрыть совсем;
- если Gemma или profile-rollup всё же вернули operator-like строку (`Профи-тур`, `команда`, `организация экскурсий`) в `guide_line`, public render обязан деградировать её до `🏢 Организатор`, а не публиковать как персонального гида;
- при такой деградации public shell должен предпочитать короткое grounded имя бренда/организатора (`marketing_name` / channel brand), а не публиковать длинную operator-bio строку под меткой `Организатор`;
- перед рендером digest делает lightweight public-identity resolution для явно упомянутых в source post `@username`: если occurrence уже знает имя co-guide, но публичный Telegram-профиль даёт более полное ФИО по тому же человеку, для public line нужно показывать resolved version;
- public-identity resolution смотрит только на guide-like контекст (`мы с @...`, `гид @...`, `экскурсию проведёт @...`) и не должен автоматически превращать username из блока `запись / бронь / лс` в ещё одного гида;
- если `guide_profile` уже знает canonical public имя, а occurrence-level `guide_names` хранит marketing alias / username того же человека (`Amber Fringilla` vs `Юлия Гришанова`), preview должен схлопывать это в одно public имя, а не показывать ложную plural-пару;
- public `Локация` line теперь проходит через guide-specific alias table [guide-place-aliases.md](/workspaces/events-bot-new/docs/reference/guide-place-aliases.md), чтобы исторические или разговорные топонимы вроде `Роминта` не выходили в digest как будто это современный город;
- если `Запись` в facts свелась к одному телефонному номеру, digest должен публиковать его как plain compact number (`+79217101161`) без форматирующих пробелов: Telegram нативно делает такой номер tap-target, а HTML `tel:` ссылка в карточке может не давать ожидаемый UX;
- если у occurrence нет даты, он может materialize-иться для inventory/template layer, но не считается digest-ready public card;
- длина `digest_blurb` выбирается по плотности фактов (`1..3` предложения), а не по “богатству” исходного поста;
- формулировка должна быть живой и интересной, но строго grounded:
  - без hype и рекламных усилителей;
  - без выдуманных преимуществ;
  - без дублирования логистики, которая уже вынесена в shell.

## Live UI и E2E

Manual/live scan через `/guide_excursions` теперь должен явно показывать transport-path:

- стартовое сообщение содержит `transport=kaggle`, если активен канонический путь;
- если `GUIDE_EXCURSIONS_LOCAL_FALLBACK_ENABLED=1`, при сбое Kaggle оператор получает явное сообщение про переход на local fallback;
- по завершении scan UI отправляет ссылку на отчёт `/guide_report <ops_run_id>` и сам run report;
- preview header содержит подсказки `facts=/guide_facts <id>` и `log=/guide_log <id>`, чтобы можно было вручную проверить извлечённые факты до публикации.
- Kaggle notebook log должен явно показывать `Guide monitor llm_gateway=google_ai ... key_env=GOOGLE_API_KEY2 account_env=GOOGLE_API_LOCALNAME2`, `[gemma:client] consumer=...` и итоговую строку `Guide monitor stats posts_total=... prefilter_true=... llm_ok=... llm_deferred=...`, чтобы оператор видел, что guide-path идёт через общий limiter, а не через прямой SDK вызов.
- guide Kaggle runtime должен переживать transient `Cannot send requests while disconnected` и явные Gemma `retry after ... ms`: на source-scan path клиент обязан переподключаться перед следующим источником, а Gemma calls должны один-два раза пережидать provider hint вместо мгновенного source-level partial;
- guide Kaggle Gemma wrapper дополнительно делает bounded retry для `asyncio.TimeoutError` (`GUIDE_MONITORING_LLM_TIMEOUT_RETRIES`, default `1`) и provider `5xx` (`GUIDE_MONITORING_LLM_PROVIDER_5XX_RETRIES`, default `1`); schema/config errors вроде unsupported `response_schema` не ретраятся и остаются blocking diagnostics.
- Канонический live pass 15 марта 2026 года подтвердил success-path `transport=kaggle -> status=success` без hidden local fallback, с `llm_deferred=0`, materialized occurrences, рабочими `/guide_facts` и `/guide_log`, и публикацией digest/media в `@keniggpt`.
- Mass rerun 16 марта 2026 года на чистой guide DB (`run_id=8a01ff760d1e`, канонический kernel `zigomaro/guide-excursions-monitor`) подтвердил, что path работает не только на одном ручном кейсе: `sources=10`, `posts=36`, `prefilter=21`, `llm_ok=21`, `created=10`, `past_skipped=3`, `errors=0`; в итоговый published digest после runtime dedup попали `7` карточек из нескольких источников, включая multi-occurrence post `@amber_fringilla/5806` и excursion `@excursions_profitour/863`, которую раньше ложно терял overly-strict eligibility gate.

Для полного live E2E через Telegram UI локальный бот должен быть единственным `getUpdates` consumer на токене. Если на том же токене параллельно работает другой polling/runtime process, команды может обрабатывать не локальный код, а preview/publish будут смотреть в чужое состояние БД.

Target channels для manual/scheduled publish задаются через `GUIDE_DIGEST_TARGET_CHATS` (comma-separated list). `GUIDE_DIGEST_TARGET_CHAT` остаётся как legacy-primary / fallback для single-target runtime. В текущем runtime guide digest уходит в `@wheretogo39` и `@youwillsee39`; для изолированного live E2E допускается временно переопределить targets на безопасный тестовый канал или список каналов, чтобы не публиковать служебный прогон в боевую ленту.

Канонический live scenario: `tests/e2e/features/guide_excursions.feature`

Сценарий обязан проходить полный operator path:

- `/start` -> `/guide_excursions` -> full scan;
- фиксация `ops_run_id` и проверка `/guide_report` + `/guide_runs`;
- проверка completion/report на `LLM ok/deferred/error` и `llm_ok/llm_deferred`, чтобы подтвердить реальный Gemma path;
- success считается только по `✅ Мониторинг экскурсий завершён` и `/guide_report ... status=success`; `⚠️ ... завершён с ошибками` не считается E2E pass;
- preview с capture нескольких control occurrences;
- ручная проверка `/guide_events`, `/guide_recent_changes`, `/guide_facts <id>`, `/guide_log <id>` по нескольким карточкам;
- только после этого publish digest во все `GUIDE_DIGEST_TARGET_CHATS` (в текущем runtime `@wheretogo39`, `@youwillsee39`).

## Формат digest-карточки

- заголовок экскурсии кликается и ведёт на исходный Telegram-пост;
- строка с каналом остаётся plain text без ссылки, чтобы не было двух соседних tap-target;
- отдельная строка `🔗 Анонс: исходный пост` больше не публикуется: source link живёт только в title, чтобы карточка не дублировала один и тот же tap-target дважды;
- между карточками ставится пустая строка + горизонтальный разделитель, чтобы длинный digest легче сканировался глазами в Telegram;
- booking link, если он извлечён, публикуется как кликаемая ссылка;
- placeholder-значения вроде `Не определено` в публичной карточке не показываются;
- booking/contact normalization предпочитает один лучший contact endpoint: сначала явный booking факт, затем мобильный телефон, затем `@username`, затем сайт/форма; публичная строка не должна копировать raw-instruction prose вроде `по телефону с 08:30...`, а `tel:` контакт должен быть кликабельным;
- media delivery публикует album только из materialized assets, которые приехали из Kaggle scan/import и сохранены в `guide_monitor_post.media_assets_json`;
- multi-target publish должен отправлять один и тот же digest payload в каждый configured target channel; issue-level storage при этом хранит legacy primary target в `target_chat` и per-target message maps в `published_targets_json`, чтобы можно было безопасно backfill-ить новый канал без text-only костылей;
- backfill нового target channel должен копировать исторические media albums как album-group, а затем перевязывать caption первого media-сообщения на текстовые части уже в целевом канале, чтобы copied album ссылался на локальные digest posts, а не на исходный канал;
- caption media album должен компактно показывать временной охват найденных экскурсий: одна дата (`12 апреля`), короткий список редких дат (`12, 14 и 16 апреля`) или диапазон (`11-15 апреля`), после чего той же строкой можно указывать `карточки 1-8`; если валидных дат нет, runtime откатывается к старому нейтральному `В альбоме карточки ...`;
- если весь digest укладывается в один безопасный caption (`<=1000` символов), runtime публикует materialized album одним сообщением без отдельной текстовой части;
- после публикации текстовых частей digest runtime редактирует caption первого сообщения media album и добавляет короткие понятные down-links вида `Подробнее: Описание` или `Подробнее: Часть 1 · Часть 2` на все связанные текстовые посты ниже в том же канале, чтобы пересланная медиагруппа оставалась связанной с описаниями экскурсий без односимвольных tap-targets;
- если несколько published occurrences приходят из одного multi-announce source post, digest должен распределять разные `media_refs` по карточкам этого поста, а не повторять одну и ту же фотографию 3-4 раза подряд;
- media selection не должна останавливаться на первых нескольких карточках без фото: runtime добирает media дальше по digest rows, пока не соберёт доступный album pack (до Telegram cap).
- если preview выбрал карточки с `media_refs`, но usable materialized files для них отсутствуют, publish обязан завершаться с явной ошибкой; silent text-only fallback для guide digest не допускается.
- service-only фразы вроде `Новых экскурсионных находок пока нет.` или `Сигналов last call пока нет.` не считаются публичным digest payload: при пустом наборе candidates runtime не публикует их в target channels и оставляет такие сообщения только для operator-facing surfaces.

## Terminology Policy

Guide digest не должен произвольно смешивать `прогулка` и `экскурсия`.

Текущая policy:

- если source title / grounded facts явно задают `прогулка`, writer сохраняет эту семью слов и не переименовывает её в `экскурсию`;
- если source title / facts явно задают `экскурсия`, writer не размывает её в `прогулку`;
- если source title / about / grounded facts явно задают `джип-тур`, внедорожный выезд или off-road формат, writer сохраняет `джип-тур` или нейтральные слова `поездка` / `выезд`, но не переименовывает такой формат в `экскурсию`;
- если тип неочевиден или формат ближе к выезду/поездке, writer предпочитает нейтральные слова `маршрут`, `выход`, `поездка`, а не неверный термин.
- тот же dominant term обязан сохраняться не только в финальном `digest_blurb`, но и в server-side `main_hook`, чтобы прогулка не превращалась в экскурсию уже на enrich-слое.

Критерии различения для guide-track:

- `экскурсия`: есть явно выраженный route + показ/рассказ + познавательная цель + структурированный guided format;
- `прогулка`: акцент на walking experience, ритме, наблюдении, природной/городской атмосфере и менее формальной форме прохождения маршрута, даже если прогулка при этом остаётся guided;
- `джип-тур`: моторизованный внедорожный выезд / поездка на внедорожнике, где существенен сам off-road формат, рельеф и проезд по труднодоступным точкам; даже если маршрут остаётся экскурсионно насыщенным, public copy не должна схлопывать его обратно в generic `экскурсию`;
- `прогулка-экскурсия`: в source может встречаться смешанная формулировка, но public copy всё равно должна выбрать один dominant term по title/facts, а не скакать между обоими словами внутри одной карточки.

Исследовательская опора для policy:

- БРЭ: `экскурсия` как коллективное посещение достопримечательных мест / объектов с образовательной, научной и культурно-просветительной целью — https://bigenc.ru/c/ekskursiia-91d446
- методика экскурсоведения: экскурсия как методически продуманный показ объектов на местности с анализом и рассказом — https://cyberleninka.ru/article/n/osnovy-ekskursionnoy-deyatelnosti-ponyatie-suschnost-priznaki-i-funktsii-ekskursii
- `джип-тур` как отдельный формат активного отдыха / поездки на внедорожнике с акцентом на бездорожье и труднодоступные точки — https://travel.rambler.ru/local/50751173-dzhip-tur-po-rossii/ , https://travelask.ru/excursions/t_27012

## Repeat Policy

Текущая repeat-логика для ежедневных digest'ов намеренно консервативная:

- `new_occurrences` публикует только те future occurrences, у которых `published_new_digest_issue_id IS NULL`;
- после публикации карточка считается уже покрытой в family `new_occurrences` и на следующий день туда повторно не попадёт;
  При этом published-mark можно ставить только тем occurrences, которые реально вошли в опубликованный digest, плюс их dedup-cluster siblings, схлопнутым в ту же canonical card;
- occurrences, которые были выкинуты editorial fallback'ом и не дошли до финального digest text/caption, не считаются опубликованными и должны оставаться кандидатами для следующего `full` run;
- `last_call` — отдельная family: туда попадают только occurrences с `is_last_call=1`, у которых ещё нет `published_last_call_digest_issue_id`;
- простое служебное обновление `updated_at` или повторный импорт тех же фактов не должны приводить к повторной публикации в `new_occurrences`;
- существенные update-digest'ы (`new route facts`, `резко изменились цена/место сбора`, `добавился booking`, `перенос`, `last seats`) пока не выделены в отдельную auto-family и в каноническом MVP считаются следующим этапом.

Практический вывод для daily automation сейчас такой:

- утренний/дневной auto-digest безопасно собирать как `new_occurrences`;
- отдельный auto-digest можно строить по family `last_call`;
- для будущего `updates_digest` нужен отдельный fact-diff слой поверх public fact pack, а не переиспользование `updated_at` как суррогата “важного изменения”.

## Scheduler

Включается только через `ENABLE_GUIDE_EXCURSIONS_SCHEDULED=1`.

Тайминги по умолчанию:

- `GUIDE_EXCURSIONS_LIGHT_TIMES_LOCAL=09:05,13:20`
- `GUIDE_EXCURSIONS_FULL_TIME_LOCAL=20:10`
- `GUIDE_EXCURSIONS_TZ=Europe/Kaliningrad`
- `ENABLE_GUIDE_DIGEST_SCHEDULED=1` включает автопубликацию `new_occurrences` сразу после scheduled `full` scan/import; отдельный cron для digest здесь намеренно не используется, чтобы не гадать длительность Kaggle run и не занимать ещё одно heavy-job окно.
- post-level Kaggle `partial` из-за отдельных `llm_deferred_timeout` / provider `5xx` считается non-blocking warning для scheduled digest: свежие eligible occurrences должны публиковаться, а предупреждения остаются в operator surfaces (`/guide_report`, completion message, `/guide_runs`). Blocking failures (`Kaggle path failed`, import errors, missing results, remote session busy) по-прежнему останавливают auto-publish.
- если после scheduled `full` scan у `new_occurrences` нет candidates, scheduled publish должен завершаться bot-only служебным сообщением оператору (`новых экскурсионных находок нет`) без публикации пустого поста в каналы;
- scheduled `full` slot считается critical daily slot: если первичный APScheduler fire пропущен или записался как `ops_run(... status='skipped', skip_reason='heavy_busy')`, startup catch-up и live watchdog обязаны догонять тот же scheduled `full` path в пределах lookback окна, а catch-up-dispatch ждёт освобождения heavy gate вместо тихого пропуска дня;
- same-day `light` runs не считаются подтверждением доставки daily `full` slot: recovery должен искать materialized `guide_monitoring` именно с `details.mode='full'`, иначе вечерняя автопубликация может быть ложно признана “уже выполненной”.
- если catch-up `full` run снова завершается `status='skipped'` только из-за занятого shared remote Telegram/Kaggle session (`remote_telegram_session_busy`), watchdog не должен считать такой dispatch завершением суточного слота: тот же scheduled `full` path обязан пробоваться снова на следующем watchdog tick, пока не materialize-ится не-skipped `full` run или не истечёт lookback окно.

## Основные entrypoints

- [guide_excursions/commands.py](/workspaces/events-bot-new/guide_excursions/commands.py)
- [guide_excursions/service.py](/workspaces/events-bot-new/guide_excursions/service.py)
- [guide_excursions/kaggle_service.py](/workspaces/events-bot-new/guide_excursions/kaggle_service.py)
- [kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py](/workspaces/events-bot-new/kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py)
- [guide_excursions/dedup.py](/workspaces/events-bot-new/guide_excursions/dedup.py)
- [guide_excursions/editorial.py](/workspaces/events-bot-new/guide_excursions/editorial.py)
- [guide_excursions/seed.py](/workspaces/events-bot-new/guide_excursions/seed.py)
- [db.py](/workspaces/events-bot-new/db.py)
- [general_stats.py](/workspaces/events-bot-new/general_stats.py)
- [scheduling.py](/workspaces/events-bot-new/scheduling.py)
- [main.py](/workspaces/events-bot-new/main.py)
- [main_part2.py](/workspaces/events-bot-new/main_part2.py)

## Связанные документы

- backlog overview: `docs/backlog/features/guide-excursions-monitoring/README.md`
- architecture: `docs/backlog/features/guide-excursions-monitoring/architecture.md`
- MVP: `docs/backlog/features/guide-excursions-monitoring/mvp.md`
- digest spec: `docs/backlog/features/guide-excursions-monitoring/digest-spec.md`
- eval pack: `docs/backlog/features/guide-excursions-monitoring/eval-pack.md`
- live E2E plan: `docs/backlog/features/guide-excursions-monitoring/e2e.md`
Kaggle runtime alignment:
- `GuideExcursionsMonitor` now uses a Kaggle notebook entrypoint (`guide_excursions_monitor.ipynb`) generated from the canonical Python runner `guide_excursions_monitor.py` at push time.
- This matches the execution model used by production Telegram Monitoring more closely and improves live log visibility in the Kaggle UI while preserving a single fact-first Python source of truth.
