# Guide Excursions Monitoring

Статус: fact-first MVP in progress

Канонический surface для мониторинга экскурсионных анонсов гидов в Telegram. Трек живёт в отдельных `guide_*` таблицах и не попадает в обычные `event`/`/daily`/month/weekend surfaces.

## Каноническая runtime boundary

Текущая каноническая граница совпадает с backlog-доками:

- `Kaggle notebook` делает Telegram/VK fetch, Telegram grouped albums, OCR/vision pass по Telegram candidate images, deterministic prefilter и `Tier 1` extraction;
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
- generated Guide notebook является самостоятельной import-boundary: server
  embed-ит детерминированно все `google_ai/**/*.py` с сохранением относительных
  путей, а acceptance test запускает собранный notebook code с isolated Python.
  Ручной allowlist модулей запрещён, потому что новый внутренний import
  (`limiter_supabase`, `interactions` или будущий nested module) иначе превращает
  все prefiltered posts в `llm_error:ModuleNotFoundError`;
- default model split для первого production migration на `Gemma 4` такой:
  - `trail_scout.screen.v1` -> `models/gemma-4-31b-it` (канонический screen с `2026-04-20` eval; `26b-a4b-it` был признан нестабильным на длинных русскоязычных reportage-постах — non-deterministic hang ≥120s под structured output)
  - `trail_scout.announce_extract_tier1.v1` + `tier1_extract_block` rescue -> `models/gemini-3.1-flash-lite` (по аналогии со Smart Update facts_extract миграцией; extraction-полнота — известное bottleneck'о, а Lite RPD-бюджет `500/day` комфортно перекрывает текущие ~20-30 extract-вызовов/сутки на `GOOGLE_API_KEY2`)
  - `trail_scout.status_claim_extract.v1`, `trail_scout.template_extract.v1`, `route_weaver.enrich.v1` -> `models/gemma-4-31b-it`
  - server-side `guide_occurrence_enrich`, `guide_profile_enrich`, `guide_excursions_dedup` -> `gemma-4-31b`
  - server-side `guide_excursions_digest_batch` (Lollipop Trails writer) -> `gemini-3.1-flash-lite` (writer-twin Smart Update; per-day объём ~1-3 batched calls на digest publish)
  - `GOOGLE_AI_FALLBACK_MODELS` должен включать `gemma-4-31b-it`, чтобы Lite outage прозрачно откатывался на Gemma 4 через канонический `GoogleAIClient` model chain;
- для Telegram auth Kaggle guide path по умолчанию использует только `TELEGRAM_AUTH_BUNDLE_S22`; локальная `TELEGRAM_AUTH_BUNDLE_E2E` не считается допустимым автоматическим fallback и может быть использована только через явный аварийный override;
- VK sources in the same Kaggle guide path use `GUIDE_MONITORING_VK_TOKEN`; server-side encrypted secrets populate it from `GUIDE_MONITORING_VK_TOKEN` or from the env named by `GUIDE_MONITORING_VK_TOKEN_ENV` (default `VK_ACCESS_TOKEN5`). This token is scoped to Kaggle guide monitoring and is not exposed in config datasets.
- перед запуском guide Kaggle kernel сервер обязан проверить общий `kaggle_registry`: если другой remote Telegram job (`tg_monitoring`, `guide_monitoring`, `telegraph_cache_probe`, `kenigsberg_story`) с тем же `remote_telegram_auth_scope` ещё жив или его Kaggle status не удалось надёжно прочитать, guide run должен завершиться `skipped` с явной диагностикой `remote_telegram_session_busy`, а не запускать вторую Telethon session поверх той же auth key. Jobs с другим explicit scope (например story publishing на `TELEGRAM_AUTH_BUNDLE_STORY`) могут идти параллельно; unknown scope считается конфликтующим;
- `UNKNOWN`/Kaggle status 5xx не должен превращаться в вечный lock: recovery обязан пробовать matching output (`guide_excursions_results.json` с тем же `run_id`); если output уже доступен, это terminal evidence, результат импортируется, а registry очищается без запуска новой Telethon/Kaggle session;
- guide digest publish-time fallback для media запрещён: bot-side `forward -> file_id`, Telethon download и public-web scraping не считаются каноническим путём; если materialized assets не доехали из Kaggle/import path, publish должен останавливаться явно, а не деградировать до text-only поста. Для уже импортированных старых VK rows допустим только server-side recovery из `attachments[].photo`, и он обязан выполняться в общем `publish_guide_digest` до Telegram fanout, чтобы Telegram и VK публиковались из одного `guide_digest_issue.media_items_json`, а не расходились по media state.
- server-side guide Gemma path (`enrich`, `dedup`, `digest_writer`) тоже обязан идти как fixed-key consumer: runtime резолвит `candidate_key_ids` из `google_ai_api_keys.env_var_name` и сначала пытается зарезервировать именно `GOOGLE_API_KEY2` / `GOOGLE_API_KEY_2`, а на `GOOGLE_API_KEY` откатывается только если для primary guide key metadata ещё не заведена в Supabase;
- server-side dedup/editorial тоже сидят на Gemma-конфигах;
- `4o` для guide pipeline не используется.
- mixed-region sources не дают “автоматического доверия по региону”: generic/out-of-region travel calendars должны отсеиваться ещё в Kaggle extraction и не materialize'иться как guide occurrence.
- first-pass `base_region_fit` теперь относится к screen/extraction LLM layer, а не к смысловым regex в local fallback.
- `base_region_fit` считается полностью Gemma-owned semantic decision: Kaggle runtime больше не держит deterministic keyword fallback по городам; если LLM не заполнил поле, результат остаётся `unknown` и post не отбрасывается по regex, а обрабатывается server-side enrichment стадиями с LLM-first ownership.
- `Gemma 4` structured stages в guide path должны использовать native `response_schema` / `response_mime_type`, а не только prompt-level "верни JSON" contract.
- guide Kaggle runtime теперь использует тот же `GoogleAIClient` не только для text-only stages, но и для multimodal OCR/vision calls:
  - poster/image OCR идёт через `guide_scout_ocr` consumer на том же guide key/runtime;
  - `ocr_chunks` подмешиваются в `trail_scout.screen.v1`, `trail_scout.*extract*` и `route_weaver.enrich.v1`, чтобы operational facts могли приходить не только из caption/body, но и из карточек/афиш;
  - OCR остаётся fail-open: если image pass не удался, post всё равно может пройти по text-only path, а ошибка должна оставаться видимой в result payload; runtime imports required for media hashing are part of the OCR contract, so missing imports must be treated as production regressions, not harmless OCR noise;
  - OCR logs must be operator-debuggable at post/media level: success, empty, retry and error lines include `source`, source `message_id`, media message id, image index, short media hash and OCR signal summary, so a Kaggle run can be audited without opening the JSON result file first;
- runtime обязан фильтровать `parts[].thought = true` до JSON parsing и materialization, чтобы guide fact-pack и admin surfaces не протаскивали hidden reasoning text.
- guide migration остаётся строго `LLM-first`: semantic screen/extract decisions не должны переезжать в regex/keyword shortcuts даже если конкретный `Gemma 4` stage ведёт себя хуже baseline; в таких случаях исправляется prompt/stage contract, а не вводится deterministic bypass по смыслу текста.
- после `INC-2026-04-23-guide-digest-extraction-loss` prompt-contract для `Gemma 4` дополнительно закрепляет multi-date расписания как отдельные occurrence на каждую датированную строку: доступная будущая строка с общим контактом/записью и без явного `sold out/full/cancelled` должна оставаться `status=available`, `availability_mode=scheduled_public`, `digest_eligible=true`; sold-out строки остаются `digest_eligible=false`; no-date/on-demand офферы не становятся digest-ready без конкретной даты; волонтёрские субботники/cleanup/work-day события не считаются экскурсиями, пока guided walk/tour/route не является основным публичным предложением.
- schedule-anchor detection may normalize Telegram keycap emoji digits (`3️⃣ мая`, `1️⃣3️⃣ мая`) into plain numerals only to keep `occurrence_blocks` from losing multi-date schedule lines; this is a syntax aid for block splitting and Gemma prompt context, not a deterministic semantic extractor. Multi-announce extraction gives Gemma a normalized `schedule_blocks` index, runs one bounded full-post extraction first, then uses block rescue for uncovered lines; if schedule blocks are already available, that broad full-post call has its own shorter timeout budget (`GUIDE_MONITORING_ANNOUNCE_MULTI_FULL_TIMEOUT_SEC`, default `45s`) and no same-prompt timeout retry, so long schedules split quickly into smaller LLM calls. Block extraction and enrichment must fail open per occurrence: one Gemma timeout/provider failure may drop or under-enrich that block with an explicit warning, but must not erase already extracted schedule lines from the same post. Explicitly tentative/preliminary/free-date schedule lines stay out of digest readiness until the source confirms them.
- if Gemma returns internally inconsistent eligibility fields, disqualifying `digest_eligibility_reason` values (`tentative_or_free_date`, `sold_out`, `cancelled`, `missing_date`, `not_scheduled_public`, `non_target`) win over `digest_eligible=true`; this is a schema-consistency guardrail around LLM output, not a semantic extractor.
- `title_normalized` в extraction prompt считается stable route identity core: без имён гидов, source labels, дат, времени, маркетинговых суффиксов и availability-слов, чтобы один source post не создавал дубли одной и той же экскурсии под разными идентичностями.
- regression evidence для guide digest completeness должен включать не только run-level `Новых выходов`, а occurrence-level проверку: какие raw outputs были digest-ready, какие были исключены как sold-out/no-date/non-target/duplicate, и какие still-future missed cards были компенсирующе опубликованы.

## Retention persistent guide media

`GUIDE_MEDIA_STORE_ROOT` находится на том же ограниченном volume, что и core SQLite, поэтому каждый server import выполняет bounded retention **до** materialization и ещё раз после commit. Ошибка retention не теряет уже скачанный Kaggle result (import остаётся fail-open), но попадает в runtime log `guide_media_retention ...` и в `import_summary.media_retention`.

Safety contract:

- никогда не удаляются файлы, на которые ссылаются occurrence с `date >= today` в `Europe/Kaliningrad`, source posts за recent grace window, все `preview`/`partial` digest issues и последний `published` issue каждого family;
- общий prune рассматривает только обычные файлы внутри store, которые не входят в protected set и старше retention floor; symlink, special/non-file entry и lexical path за пределами store не удаляются и не follow-ятся;
- unprotected candidates удаляются от старых к новым, но один pass ограничен числом и суммарным размером удалений. `max total` и `min free` — явные post-run targets: если их нельзя достигнуть без protected/recent файлов или из-за per-run cap, результат возвращает `policy_satisfied=false` / `bounded=true`, а не расширяет область удаления;
- source assets из исторических постов после успешного unlink удаляются из `guide_monitor_post.media_assets_json` вместе только с position-aligned `media_refs_json`; тот же repair pass убирает paths, отсутствие которых доказано через filesystem. Dry-run DB не меняет. Ошибка DB transaction не маскируется, а следующий pass повторяет healing по уже отсутствующим paths;
- generated `_digest_carousel/<issue_id>` является восстанавливаемым cache: `preview`/`partial` slides получают отдельный floor 24 часа, `published` — 7 суток. Source media из current issue при этом остаётся protected;
- пустые каталоги удаляются только после apply-pass.

Defaults (все thresholds меняются env без изменения кода):

| Env | Default | Назначение |
|---|---:|---|
| `GUIDE_MEDIA_RETENTION_ENABLED` | `1` | автоматический pre/post import prune |
| `GUIDE_MEDIA_RETENTION_DAYS` | `14` | минимальный возраст unprotected source/unknown regular file |
| `GUIDE_MEDIA_RECENT_POST_GRACE_DAYS` | `14` | защита media недавно увиденных source posts независимо от occurrence link |
| `GUIDE_MEDIA_RETENTION_MAX_TOTAL_BYTES` | `402653184` | target не более 384 MiB guide store после pass |
| `GUIDE_MEDIA_RETENTION_MIN_FREE_BYTES` | `268435456` | target не менее 256 MiB свободного места на volume |
| `GUIDE_MEDIA_RETENTION_MAX_DELETE_FILES` | `500` | верхняя граница unlink за pass |
| `GUIDE_MEDIA_RETENTION_MAX_DELETE_BYTES` | `536870912` | верхняя граница reclaim за pass |
| `GUIDE_MEDIA_CAROUSEL_PREVIEW_RETENTION_HOURS` | `24` | floor preview/partial carousel cache |
| `GUIDE_MEDIA_CAROUSEL_PUBLISHED_RETENTION_HOURS` | `168` | floor published carousel cache |
| `GUIDE_MEDIA_RETENTION_TZ` | `Europe/Kaliningrad` | граница current/future occurrence |

Operator path сначала всегда dry-run; apply требует явного флага и печатает полный JSON inventory/repair report:

```bash
python scripts/prune_guide_media_store.py --db /data/db.sqlite --root /data/guide_media
python scripts/prune_guide_media_store.py --db /data/db.sqlite --root /data/guide_media --apply --reason incident_cleanup
```

Production override удерживает store около `256 MiB` и требует после pass не менее `350 MiB` свободного места (`GUIDE_MEDIA_RETENTION_MAX_TOTAL_BYTES=268435456`, `GUIDE_MEDIA_RETENTION_MIN_FREE_BYTES=367001600`). Если protected/recent media не позволяют достигнуть этих targets, retention не расширяет deletion set, а сигнализирует `policy_satisfied=false`.

## Что уже мигрировано

- отдельный guide-track в основной SQLite;
- seed-пак Telegram-источников из casebook;
- seed-пак guide-источников теперь также включает `@art_from_the_Baltic` как provisional `guide_project` source, `@jeeptours39` как branded off-road / jeep-tour source, `@murnikovaT` и `@progulki_s_katey` как personal guide sources for Kaliningrad excursions, `@kaliningradlibrary` как institutional `organization_with_tours` source для экскурсионных/краеведческих прогулок библиотеки, а также VK publics `vk.ru/balticsyndicate`, `vk.com/konb39`, `vk.com/ruin.keepers` и `vk.com/narodexcursovod` через тот же Kaggle guide-monitoring runtime;
- guide-specific Kaggle runtime: [kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py](/workspaces/events-bot-new/kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py);
- secure Kaggle push/poll/download через тот же split-secrets pattern, что и в Telegram Monitoring;
- guide Kaggle transport теперь повторяет продовый Telegram Monitoring pattern: kernel push содержит нужный `google_ai/` код сразу, а secrets по-прежнему идут только через два отдельных datasets (`cipher + key`), без третьего payload dataset;
- канонический guide kernel должен оставаться одним и тем же (`zigomaro/guide-excursions-monitor`) и жить через Kaggle versioning; `GUIDE_MONITORING_KERNEL_SLUG` оставлен только как ручной аварийный override, а не как обычный E2E path;
- server import с materialization `fact_pack_json` и `GuideFactClaim.claim_role/provenance/observed_at/last_confirmed_at`;
- server-side digest eligibility теперь остаётся fact-first: undated / cancelled / private occurrences fail-closed, но `limited`-объявления с реальной будущей датой и достаточным набором публичных фактов (`time/city/meeting/route/price/booking/summary`) могут быть повышены до digest-ready вместо немого выпадения из daily выпуска;
- digest/editorial, где `fact_pack` считается primary truth source, а исходный post text используется только как secondary evidence;
- guide-specific `Lollipop Trails` fork для digest copy: Gemma batch-пишет `title`, `digest_blurb` и короткие public lines (`Гид` / `Организатор`, `Цена`, `Что в маршруте`, `Запись`, house-line про local-vs-tourist fit) из materialized fact pack + guide profile rollup;
- migration `2026-04-19`: guide path переведён на `Gemma 4` defaults с quality-first split (`26b-a4b` для screen, `31b` для extract/enrich/writer), а Kaggle/server structured stages теперь передают native `response_schema` вместо чисто prompt-only JSON режима;
- migration `2026-05-12`: после успеха Smart Update Gemini-lite перевода тот же узкий принцип применён к guide-track — `trail_scout.announce_extract_tier1.v1` (плюс `tier1_extract_block` rescue) и server-side `guide_excursions_digest_batch` (Lollipop Trails writer) теперь по умолчанию идут на `gemini-3.1-flash-lite` с прозрачным fallback на `gemma-4-31b-it` через `GOOGLE_AI_FALLBACK_MODELS`; screen, status_claim/template extract, route_weaver enrich, occurrence enrich (`main_hook` + `audience_region_fit`), profile enrich, dedup pair-judge и OCR/vision стадии сознательно остаются на Gemma 4, чтобы Lite RPD-бюджет тратился только на bottleneck'и extraction-полноты и user-facing writer copy; rollback через env (`GUIDE_MONITORING_EXTRACT_MODEL=models/gemma-4-31b-it`, `GUIDE_DIGEST_WRITER_MODEL=gemma-4-31b`) без редеплоя;
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

- OCR теперь закрывает основной MVP gap: candidate posts с image media проходят Gemma vision/OCR pass, а extracted `ocr_chunks` участвуют в screen/extract/enrich. До backlog-parity всё ещё остаются follow-up зоны вроде richer media fingerprinting, cross-image assignment и более глубокого OCR-debug/operator UX;
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
- `/guide_digest_vk` — publish последнего успешного `new_occurrences` digest issue в VK target через отложенный wall-post.

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
- Транзиентные ошибки Kaggle API на polling (`SSL`, сеть, timeout, HTTP 5xx от `GetKernelSessionStatus`) не должны валить guide run сразу: бот продолжает опрос и показывает в status-update, что это временная ошибка Kaggle API.
- Shared remote Telegram session guard перед новым запуском остаётся fail-closed для fresh/unknown Kaggle status, но stale registry entries с транзиентной ошибкой status lookup перестают блокировать новый run после `REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES` (default `390`). Это не разрешает ручную очистку fresh `UNKNOWN`: до cutoff запись считается live-lock для `TELEGRAM_AUTH_BUNDLE_S22`; после cutoff guard логирует/помечает meta `stale_transient_status_lookup_failure` и пропускает запуск, чтобы старый `GetKernelSessionStatus` 5xx не ломал ежедневный full slot бесконечно.
- Если `kaggle_registry` содержит `guide_monitoring` job, а статус Kaggle не удаётся надёжно прочитать (`UNKNOWN`, `GetKernelSessionStatus` HTTP 5xx, network/timeout), эта запись считается **активной remote Telethon session**, а не stale. Её нельзя удалять вручную и нельзя запускать новый guide Kaggle run с тем же `TELEGRAM_AUTH_BUNDLE_S22`, пока не появится terminal Kaggle evidence, fresh output с ожидаемым `run_id` не будет импортирован, либо пользователь явно подтвердит abandon старой сессии после замены auth bundle. Нарушение этого правила может запустить два Kaggle kernel параллельно и инвалидировать Telegram auth key (`AuthKeyDuplicatedError`).
- Для VK guide sources нормальный Kaggle scan обязан разбирать `attachments[].photo`, выбирать лучший `sizes[].url`, скачивать фото в output bundle `guide_media/...` и отдавать их как `media_refs/media_assets`; иначе digest carousel потеряет исходную картинку и будет вынужден строить hook-only slide. Server-side `recover_missing_vk_media_assets_for_occurrences()` допустим только как repair-страховка для уже импортированных старых rows, где `media_refs_json/media_assets_json` пустые.
- При скачивании output сервер дополнительно валидирует `run_id` внутри `guide_excursions_results.json`; stale output от предыдущей версии kernel не должен импортироваться как свежий scan.
- Перед polling сервер теперь дополнительно проверяет shape канонического Kaggle kernel: `zigomaro/guide-excursions-monitor` обязан оставаться `kernel_type=notebook` с notebook `code_file`. Если remote kernel внезапно стал `script`, run должен падать сразу с явной инструкцией пересоздать канонический notebook, а не зависать на stale output.
- Сам guide notebook runner теперь тоже fail-closed по auth boundary: если `TELEGRAM_AUTH_BUNDLE_S22` отсутствует, а в окружении есть только `TELEGRAM_AUTH_BUNDLE_E2E`, `_resolve_auth_bundle()` обязан упасть с явной ошибкой вместо тихого borrow чужой сессии; non-`S22` auth допустим только через явный low-level override `GUIDE_MONITORING_ALLOW_NON_S22_AUTH=1`.

## Recovery после рестарта бота

- `guide_monitoring` регистрирует pushed kernel в общем `kaggle_registry` сразу после успешного `push`.
- После успешного download сервер копирует весь output bundle в persistent store `GUIDE_MONITORING_RESULTS_STORE_ROOT` (по умолчанию `/data/guide_monitoring_results`) и пишет `results_path` в recovery meta, чтобы рестарт во время server-import или scheduled auto-publish не терял уже готовый результат.
- Persistent store живёт на том же Fly `/data`, что и SQLite, поэтому перед и после копирования нового bundle сервер чистит старые `guide-excursions-*` директории по guard-параметрам:
  - `GUIDE_MONITORING_RESULTS_STORE_RETENTION_DAYS=2`;
  - `GUIDE_MONITORING_RESULTS_STORE_MAX_RUNS=6` (включая текущий run);
  - `GUIDE_MONITORING_RESULTS_STORE_MAX_MB=256`;
  - `GUIDE_MONITORING_RESULTS_STORE_MIN_FREE_MB=256`.
  Эти defaults можно расширять только вместе с явным disk budget; иначе Guide recovery artifacts могут повторно заполнить `/data` и сломать SQLite-backed scheduler/video jobs.
  Production использует более строгий override: `MAX_RUNS=2`, `MAX_MB=128`, `MIN_FREE_MB=350`. Текущий persist target всегда передаётся в `exclude`, поэтому guard удаляет только предыдущие recovery bundles и не может удалить импортируемый run.
- Scheduler `kaggle_recovery` проверяет и `guide_monitoring`, так же как остальные Kaggle jobs:
  - если kernel ещё работает, запись остаётся в реестре;
  - если output уже был скачан до рестарта, recovery сначала поднимает import из сохранённого `results_path` без повторного запроса в Kaggle;
  - если kernel завершился `complete`, а локального persisted bundle ещё нет, бот заново скачивает `guide_excursions_results.json` из Kaggle и запускает обычный server-import;
  - для scheduled `full` run с `ENABLE_GUIDE_DIGEST_SCHEDULED=1` recovery должен дотягивать не только import, но и тот же auto-publish `new_occurrences`, если процесс упал между этими фазами;
  - если kernel завершился `failed/error/cancelled`, запись удаляется из реестра и оператор получает уведомление.
- Источником истины для recovery по-прежнему остаётся Kaggle output; локальный persisted bundle считается лишь durable-копией уже скачанного canonical output и нужен только для того, чтобы рестарт не обнулял фазу import/publish.
- Critical scheduler watchdog не должен немедленно повторять один и тот же дневной `guide_excursions_full` после recent terminal `error`/`crashed` или `remote_telegram_session_busy`: persisted `ops_run` state ставит retry-hold на `GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS` (default `3600`), чтобы Fly restart/secret rotation не запускали параллельный Kaggle/Telethon прогон на той же S22-сессии. Это не отключает следующий штатный слот и не считается заменой ремонта первопричины.
- Пока канонический guide kernel `zigomaro/guide-excursions-monitor` находится в `RUNNING`, запрещено делать manual/prod Telethon smoke на той же `TELEGRAM_AUTH_BUNDLE_S22`; сначала дождаться terminal status или recovery output.

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
- local guide Gemma 4 stages used while building digest previews (`GUIDE_OCCURRENCE_ENRICH_MODEL`, `GUIDE_EXCURSIONS_DEDUP_MODEL`, `GUIDE_DIGEST_WRITER_MODEL`) must be bounded by per-call timeouts (`GUIDE_OCCURRENCE_ENRICH_LLM_TIMEOUT_SEC`, `GUIDE_EXCURSIONS_DEDUP_LLM_TIMEOUT_SEC`, `GUIDE_DIGEST_WRITER_LLM_TIMEOUT_SEC`). On timeout they fall back to existing deterministic rows/heuristics instead of hanging the scheduled digest.
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

- в Telegram-render заголовок экскурсии кликается и ведёт на исходный Telegram/VK-пост;
- строка с каналом остаётся plain text без ссылки, чтобы не было двух соседних tap-target;
- отдельная строка `🔗 Анонс: исходный пост` больше не публикуется: source link живёт только в title, чтобы карточка не дублировала один и тот же tap-target дважды;
- между карточками ставится пустая строка + горизонтальный разделитель, чтобы длинный digest легче сканировался глазами в Telegram;
- booking link, если он извлечён, публикуется как кликаемая ссылка;
- placeholder-значения вроде `Не определено` в публичной карточке не показываются;
- booking/contact normalization предпочитает один лучший contact endpoint: сначала явный booking факт, затем мобильный телефон, затем `@username`, затем сайт/форма; публичная строка не должна копировать raw-instruction prose вроде `по телефону с 08:30...`, а `tel:` контакт должен быть кликабельным;
- media delivery публикует album только из materialized assets, которые приехали из Kaggle scan/import и сохранены в `guide_monitor_post.media_assets_json`;
- если выбранный digest issue ещё не имеет usable materialized media для VK-source occurrence, `publish_guide_digest` сначала делает server-side VK media recovery и обновляет `guide_digest_issue.media_items_json`; только после этого разрешена отправка Telegram/VK. Recovery после Telegram-фазы считается regression, потому что приводит к text-only Telegram post и карточкам только в VK.
- multi-target publish должен отправлять один и тот же digest payload в каждый configured target channel; issue-level storage при этом хранит legacy primary target в `target_chat` и per-target message maps в `published_targets_json`, чтобы можно было безопасно backfill-ить новый канал без text-only костылей;
- backfill нового target channel должен копировать исторические media albums как album-group, а затем перевязывать caption первого media-сообщения на текстовые части уже в целевом канале, чтобы copied album ссылался на локальные digest posts, а не на исходный канал;
- caption media album должен компактно показывать временной охват найденных экскурсий: одна дата (`12 апреля`), короткий список редких дат (`12, 14 и 16 апреля`) или диапазон (`11-15 апреля`), после чего той же строкой можно указывать `карточки 1-8`; если валидных дат нет, runtime откатывается к старому нейтральному `В альбоме карточки ...`;
- если весь digest укладывается в один безопасный caption (`<=1000` символов), runtime публикует materialized album одним сообщением без отдельной текстовой части;
- после публикации текстовых частей digest runtime редактирует caption первого сообщения media album и добавляет короткие понятные down-links вида `Подробнее: Описание` или `Подробнее: Часть 1 · Часть 2` на все связанные текстовые посты ниже в том же канале, чтобы пересланная медиагруппа оставалась связанной с описаниями экскурсий без односимвольных tap-targets;
- если несколько published occurrences приходят из одного multi-announce source post, digest должен распределять разные `media_refs` по карточкам этого поста, а не повторять одну и ту же фотографию 3-4 раза подряд;
- media selection не должна останавливаться на первых нескольких карточках без фото: runtime добирает media дальше по digest rows, пока не соберёт доступный album pack (до Telegram cap).
- если preview выбрал карточки с `media_refs`, но usable materialized files для них отсутствуют, publish обязан завершаться с явной ошибкой; silent text-only fallback для guide digest не допускается.
- service-only фразы вроде `Новых экскурсионных находок пока нет.` или `Сигналов last call пока нет.` не считаются публичным digest payload: при пустом наборе candidates runtime не публикует их в target channels и оставляет такие сообщения только для operator-facing surfaces.

## VK digest MVP

Цель ближайшего запуска: публиковать тот же `new_occurrences` fact-pack в VK-паблик `https://vk.com/uhtykaliningrad` отдельным от Telegram surface. VK-пост должен быть одним wall-постом, без Telegram-style split на несколько частей, пока итоговый plain-text payload проходит лимит VK. Если однажды payload не помещается, runtime должен явно остановить публикацию и показать оператору причину, а не резать выпуск молча.

Формат первой строки важен для ленты VK: она должна сразу сообщать количество и точные даты найденных новых выходов, например `Новые экскурсии: 3 выхода, 30 мая, 2 июня и 7 июня` или диапазон для длинного выпуска. Даты считаются по `guide_occurrence.date` после финальной dedup/editorial фильтрации, то есть по тем карточкам, которые реально попали в VK-пост. Следующие строки могут давать короткий редакционный lead, но первая строка не должна начинаться с приветствия, hashtag'ов, служебной метки или длинной интро-фразы.

VK-render отличается от Telegram-render:

- используется plain text, без HTML `<a>` и без Telegram media album/caption mechanics;
- карточки остаются fact-first и берут те же поля, что Telegram digest: дата, локация, гид/организатор, маршрут, цена, места, запись;
- исходная ссылка на анонс выводится отдельной короткой строкой в карточке, потому что заголовок в plain VK text не может быть кликабельным;
- Telegram booking/source links (`t.me/...`, `https://t.me/...`) перед выводом в VK должны проходить через existing VK shortener (`utils.getShortLink` / `vk.cc`); если shortener недоступен, публикация не падает, но сохраняет исходную ссылку и пишет warning в operator/runtime evidence;
- если экскурсия пришла с личной VK-стены, public attribution должен показывать VK user mention/link на автора (`https://vk.com/<screen_name>` или `[id...|Имя]`, если resolve доступен), а не только обезличенное `Источник: VK`;
- если экскурсия пришла из VK-сообщества, attribution использует community label/source link, но не притворяется персональным гидом без occurrence-level evidence;
- VK-пост должен идти через общий community wall publishing contract из [VK Publishing](../vk-publishing/README.md): `owner_id=-<group_id>`, `from_group=1`, `signed=0`, `publish_date` минимум через 10 минут.
- VK-пост должен публиковаться с VK photo attachments: либо production carousel slides, сгенерированными из `guide_digest_issue` fact-pack, либо теми же materialized guide media assets из `guide_digest_issue.media_items_json`. Текст и картинки идут одним `wall.post`, без Telegram-style разделения на media album + отдельный текст. Если ни carousel, ни materialized assets не дали ни одного `photo...` attachment, VK fanout должен fail-closed вместо text-only поста.

Минимальный rollout для `uhtykaliningrad`:

1. Добавить env/настройку целевой группы для guide VK digest отдельно от `VK_EVENTS_GROUP_ID`, чтобы не смешивать `uhtykaliningrad` с `klgdevents`.
2. Добавить VK-render поверх уже существующего `build_guide_digest_preview(..., family="new_occurrences")`, чтобы dedup, writer, repeat-policy и published marks не расходились между Telegram и VK.
3. Сохранять VK publication evidence в `guide_digest_issue.published_targets_json` или совместимом per-target поле, чтобы повторный запуск видел, что выпуск уже ушёл в VK, и не дублировал тот же digest.
4. После deploy взять последний успешный `new_occurrences` guide digest issue и поставить его в отложку `uhtykaliningrad` на ближайший допустимый слот через `post_to_vk` с photo attachments из `media_items_json`; verify через VK API должен подтвердить URL, `from_id=-<uhtykaliningrad_group_id>`, `publish_date >= now+600s`, `attachments[].type=photo`.

Runtime flags:

- `ENABLE_GUIDE_DIGEST_VK=1` включает scheduled VK fanout после успешной Telegram-публикации scheduled `new_occurrences`;
- `GUIDE_DIGEST_VK_TARGET=uhtykaliningrad` задаёт screen name целевого паблика; если `GUIDE_DIGEST_VK_TARGET_GROUP_ID` пуст, runtime resolve'ит group id через `utils.resolveScreenName`;
- `GUIDE_DIGEST_VK_MAX_CHARS=15000` задаёт fail-closed лимит одного VK-поста;
- ручной catch-up после deploy: `/guide_digest_vk` публикует последний успешный digest issue в VK-отложку и не пересобирает candidates.

Подводные камни, которые считаются blocking для запуска:

- нельзя переиспользовать Telegram split text как есть: VK должен получить один связный plain-text пост с собственной первой строкой;
- нельзя публиковать VK guide digest без VK photo attachments: если carousel не собрался и preview/materialization не дали usable assets, отсутствие uploaded `photo...` является blocking ошибкой;
- нельзя отмечать occurrence опубликованным только из-за VK-failure/partial: published mark ставится только после успешного wall.post/postponed result;
- shortener failure не блокирует digest, но должен быть видимым, потому что Telegram-ссылки в VK без сокращения часто выглядят длинно и плохо меряются;
- personal VK source требует owner/user awareness. `vk.com/ivsguide`, `vk.ru/natakkaz` и будущие личные страницы нельзя обрабатывать как negative group wall id;
- добавление `vk.com/ruin.keepers` и `vk.com/narodexcursovod` расширяет duplicate surface: Route Matchmaker должен сравнивать VK announcements с Telegram `ruin_keepers` и aggregator mirrors по same-date route/booking anchors, иначе один и тот же выход появится в VK digest второй карточкой;
- если VK resolve screen_name/group id недоступен из-за token/permission/rate limit, запуск должен остановиться до production-поста и показать оператору, какой source/target не был resolved.

### Отдельный визуальный VK-дайджест расписания

Помимо текстового/афишного guide digest есть отдельный VK-first выпуск
`visual_schedule`: одна вертикальная карточка 1080×1350 с расписанием до 5
будущих экскурсий. Это не замена основного дайджеста, а отдельная публикация:
зритель должен за несколько секунд понять `когда · куда/откуда · с кем · как`
и перейти в текст поста только если маршрут ему интересен.

Production contract:

- источник данных — уже сохранённые `guide_occurrence` из мониторинга; рендер не
  придумывает названия, время, места старта или транспорт, а берёт факты из
  `canonical_title`, `date/time`, `guide_names/organizer_names`,
  `meeting_point`, `route_summary`, `seats_text`, `fact_pack_json`;
- один production-выпуск содержит максимум 5 экскурсий (`GUIDE_VISUAL_DIGEST_MAX_CARDS=1`);
  production-пост теперь однокартиночный; ручной review может рендерить
  несколько карточек для сравнения, но scheduled daily публикует только первую
  карточку с максимум 5 строками;
- окно отбора начинается с завтрашней даты в timezone мониторинга
  (`GUIDE_EXCURSIONS_TZ`, по умолчанию `Europe/Kaliningrad`): сегодняшние
  экскурсии в визуальный дайджест не попадают, потому что зрителю уже слишком
  поздно принимать решение по daily-карточке;
- idempotency отдельная от старого Telegram/VK digest: после успешного VK
  `wall.post` occurrence получает `published_visual_digest_issue_id`, а
  `guide_digest_issue.media_items_json.item_states` хранит снимок визуально
  важных фактов. Повторное попадание разрешено только если occurrence ещё ни
  разу не была в `visual_schedule` или если изменился viewer-facing факт:
  дата/время/статус, название, место/маршрут/точка встречи, запись/цена,
  `last_call` или реально заканчиваются места (`≤4` и стало меньше, чем в
  сохранённом снимке). Одного `updated_at` без существенной разницы недостаточно;
- карточка показывает номер выпуска в компактном number-only бейдже (без `№`),
  с центрированным лейблом/цифрой и стабильными внутренними отступами; также
  показывает крупный период, полный месяц (`июля`), день
  недели/время (если времени нет — полный день недели), полное имя гида или
  название организации, локацию/старт/маршрут, короткие впечатления, аватар/логотип
  известного гида/организации, иконку формата и места только когда они есть;
  для `@progulki_s_katey` публичное имя показывается как `Катя Костюгова`, а
  аватар берётся из Telegram-канала (с подтверждением профиля
  `@katerinakostiugova`);
- бренд-локап `Ух ты, Калининград!` не перерисовывается Pillow-текстом: renderer
  использует committed PNG `guide_excursions/assets/visual_digest_brand/uhty_kaliningrad_v18.png`,
  полученный из принятого v18 SVG-прототипа, чтобы не менять наклон, толщину,
  stroke/glow и пропорции букв между выпусками;
- даже при ручной много-карточной review-карусели каждая карточка должна быть
  самодостаточным мини-дайджестом: не показываем техническую пагинацию вида
  `1–5 из 15`, не добавляем строку «остальные даты ниже», а заголовки строк
  остаются главным визуальным якорем;
- Telegram-публикация идёт в те же каналы, что обычный guide digest
  (`GUIDE_VISUAL_DIGEST_TARGET_CHATS` или fallback `GUIDE_DIGEST_TARGET_CHATS`):
  одна картинка + HTML-caption, где названия экскурсий являются ссылками
  (`<a href="...">название</a>`), без отдельных URL рядом и без VK shortener;
  если запись/контакт представлен телефоном, Telegram caption не дублирует номер
  рядом с названием: название экскурсии само становится ссылкой на исходный
  пост/запись (`<a href="...">название</a>`). Отдельные кнопки, redirect-страницы,
  `tel:` HTML-обёртки и хвосты вида `— +7...` в Telegram visual digest запрещены;
  VK-текст оставляет телефон человекочитаемым номером;
  после хештегов caption добавляет пустую строку и социальный футер
  `Подписаться · Max · Вконтакте`: `Подписаться` ведёт на текущий Telegram target,
  Max — на `https://max.ru/join/-aoufdeeRIfMctMnRNYgdTe3CC6tHIqE75xaVYTT7Ec`,
  Вконтакте — на `https://vk.ru/uhtykaliningrad`;
- VK-текст: первая строка `Дайджест экскурсий №…`, короткая вводная, затем строки
  `название — ссылка`. VK-ссылки оформляются кликабельным названием (`[vk-url|title]`),
  внешние URL сокращаются через `utils.getShortLink`, телефонные контакты не
  сокращаются;
- primary link safety: если future occurrence пришла из preliminary/multi-event schedule, а `booking_url` указывает на другой wall-post того же VK-источника без явной актуальной записи, visual digest должен демотировать такой исторический route/detail link и использовать current `source_post_url`/`channel_url`;
- хештеги в Telegram/VK включают базовые `#экскурсии #Калининград
  #УхтыКалининград` плюс упомянутые города/узнаваемые локации карточки
  (`#Зеленоградск`, `#Балтийск`, `#КуршскаяКоса`, `#Амалиенау` и т.п.);
- публикация идёт через `post_to_vk(..., carousel=True)`, чтобы VK оставил
  листалку, а не принудительную медиагруппу/grid. Scheduled VK wall post
  ставится в отложку на 10 минут (`GUIDE_VISUAL_DIGEST_VK_DELAY_SECONDS=600`);
  после выхода из отложки due-job публикует VK Story примерно через 15 минут
  (`GUIDE_VISUAL_DIGEST_VK_STORY_DELAY_SECONDS=900`) с той же карточкой,
  помещённой на 1080×1920 без уменьшения текста, и ссылкой на wall post.

Код: `guide_excursions/visual_digest.py`, ручной CLI
`scripts/guide_visual_digest.py`, scheduler gate
`ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED=1`. На Fly отдельный slot:
`GUIDE_VISUAL_DIGEST_TIME_LOCAL=10:30`, `GUIDE_VISUAL_DIGEST_TZ=Europe/Kaliningrad`,
`GUIDE_VISUAL_DIGEST_MAX_CARDS=1`, VK target `uhtykaliningrad`, Telegram targets
из `GUIDE_DIGEST_TARGET_CHATS`. Старый Telegram/VK guide digest остаётся
отдельным и не отменяется. Визуальный QA перед первым тестом был проведён через
`agy --model "Gemini 3.1 Pro (High)"`; Gemini verdict: `PASS` для contact sheet
production renderer.

### Hook-карточки (engagement cards)

Поверх afisha-вложений VK-пост дополняется 1–3 «крючковыми» карточками (1080×1080), на которых крупно вынесена одна цепляющая маркетинговая фраза-вопрос — чтобы в сетке VK пост сразу зацеплял взгляд. Это редакторские текст-карточки в инстаграм-стиле, а не афиши.

Контракт:

- **Карточки дополняют, а не заменяют афиши.** Слотовая математика: `available_slots = max(0, 9 − число_afisha_attachments)`, целевое число карточек 1–3 (`HOOK_CARD_MAX_CARDS`), итог в сетке ≤ 9 изображений. Если афиш уже ≥ 9, карточки не добавляются. Афиши никогда не вытесняются карточками.
- **Карточки идут ПЕРВЫМИ в сетке** (самое цепляющее впереди), афиши — следом: итоговый `attachments = карточки + афиши`.
- **Best-effort и additive.** Любая ошибка генерации/рендера/загрузки карточек логируется и проглатывается — дайджест всё равно публикуется с афишами. Отдельного on/off флага нет: карточки — часть публикации дайджеста (меньше точек отказа).
- **Хук — это ВОПРОС.** Всегда ровно один маркетинговый вопрос-крючок, заканчивающийся «?» (тот же принцип, что и первая фраза-крючок рерайта анонса, `ask_4o`/GPT-4o), а не утверждение/заголовок. Санитайзер отбрасывает любой крючок без «?» или с более чем одним «?». Существующий grounded `main_hook` из enrich используется только как фактура для grounding, его текст на карточку дословно не выносится.
- **Отбор через LLM:** один GPT-4o вызов (`response_format=GuideHookCards`) по фактам экскурсий генерирует крючок для перспективных выходов и возвращает только самые сильные и **разнотипные** (разные темы/форматы) `cap` штук, отсортированные по `strength`. Не обязательно показывать крючок для каждой экскурсии.
- **Подпись на карточке (footer) = дата + гид(ы)** (`«7 июня · Анна Иванова»`, `«7 июня · Кирилл и Марго»`, `«7 июня · 3 гида»`), детерминированно из occurrence (`date` + `guide_names`), максимально кратко; совместный выезд не должен схлопываться до первого гида. Если данных нет — footer опускается.
- **Санитайзер карточки (fail-closed на карточку):** без эмодзи, без URL/usernames/телефонов/цен/дат/времени и призывов «записаться/купить билет»; 3–14 слов, ≤ 90 символов; сохраняется доминирующий термин (прогулка/экскурсия/джип-тур, см. Terminology Policy). Неестественные штампы вроде `за горизонтом` отбрасываются вместо публикации на картинке. Нарушившие крючки отбрасываются, карточка не строится.
- **Цвет — один на публикацию, разный по дням.** Все карточки одного поста используют ОДНУ палитру (`select_post_palette(seed=issue_id)`); разные выпуски/дни ротируют палитру, поэтому один день выглядит цельно, а разные дни — по-разному. Палитры из валидированного набора `guide_excursions/assets/vk_hook_card_palettes.json` (контраст ≥ 7:1).
- **Типографика:** текст центрируется в safe-zone `x 150–930 / y 160–920` с большими полями, чтобы VK-обрезка краёв сетки не задевала смысловой текст. **Единый кегль главного крючка на все карточки одной публикации** (наименьший подходящий из 56–104 px, считается по самому длинному хуку и применяется ко всем — чтобы соседние карточки не выглядели как разная жирность/ширина); подпись — фиксированные 46 px. Шрифт главного текста — `assets/fonts/Cygre-ExtraBold.ttf`, подпись — `Cygre-SemiBold.ttf` (кириллический гротеск).
- Код: `guide_excursions/hook_cards.py` (LLM-пайплайн + отбор) и `guide_excursions/hook_card_render.py` (Pillow-рендер). Покрыто `tests/test_guide_hook_cards.py`.

### Карусель (продакшн-формат)

`publish_latest_guide_digest_to_vk` публикует VK-дайджест **каруселью** (листалкой) — вертикальные слайды 4:5, которые VK показывает целиком (без обрезки до квадрата). Тип каждого слайда выбирается **по картинке**:

- медиа, которое уже является **афишей** (постер со своим текстом — определяется vision-классификатором GPT-4o «АФИША/ФОТО»), показывается целиком + номер `i/N` + бейдж «листай» (`render_afisha_slide`);
- **фото без текста** получает маркетинговый вопрос-крючок в нижнем «ломаном» блоке + дата·гид + «листай» + номер (`render_carousel_slide`);
- для самых сильных крючков событий **без пригодного фото** добавляется 1–2 текстовые карточки-крючки (`render_hook_only_slide`);
- последний слайд — CTA со стрелкой вниз к тексту поста (`render_cta_slide`).

Сборка — `guide_excursions/hook_carousel.py::build_carousel_slides` (одна палитра на публикацию, ротация по дню; фото-слайды по силе крючка; ≤9 слайдов + CTA). Slides готовятся один раз на digest issue и сохраняются в `GUIDE_MEDIA_STORE_ROOT/_digest_carousel/<issue_id>/slide_*.jpg`: Telegram и VK должны переиспользовать эти же файлы, а не генерировать два разных набора. Если в Telegram digest ровно один фото-кандидат, публикуется generated card (`slide_0.jpg`) вместо raw source photo; если фото несколько, Telegram остаётся на старой media-album схеме. VK-пост идёт через `post_to_vk(..., carousel=True)` (без `primary_attachments_mode=grid`, чтобы VK рендерил листалку); `repair_existing` edit тоже сохраняет carousel mode, чтобы отложенный пост можно было заменить без удаления и повторной публикации. Carousel build/upload выполняется **до** требования materialized afisha assets: если в выпуске нет пригодных исходных картинок, но есть строки дайджеста, runtime может опубликовать hook-only карточки + CTA как полноценные VK photo attachments. Если carousel slides построены, но VK upload вернул меньше двух `photo...` attachments, публикация должна fail-closed/retry и не имеет права откатываться на plain source-image/afisha-grid fallback; fallback допустим только когда carousel вообще не был построен.

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
- перед финальным выбором `new_occurrences` свежие candidates сравниваются тем же LLM-first dedup слоем с уже опубликованными future occurrences из окна digest. Если новый пост/агрегаторный репост оказывается той же экскурсией, что уже была в выпуске, такая строка не занимает место в новом digest и не вытесняет действительно новую карточку;
- dedup-проход имеет общий time budget `GUIDE_EXCURSIONS_DEDUP_TOTAL_TIMEOUT_SEC`: LLM-first сравнение остаётся включённым, но серия pair-judge запросов не должна блокировать весь digest/catch-up;
- digest-writer polish имеет общий time budget `GUIDE_DIGEST_WRITER_TOTAL_TIMEOUT_SEC`: если финальная Gemma-редактура не успевает, публикация fail-open идёт по уже собранным grounded facts и editorial fallback, а не ждёт polish бесконечно;
- candidate date должен быть ISO-днём `YYYY-MM-DD`; текстовые recurring-значения вроде `every Thursday` не участвуют в daily digest candidate query, пока не материализованы в конкретную дату;
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
- Если critical-watchdog догоняет вечерний full-slot уже после локальной полуночи,
  его persisted cooldown учитывает `remote_telegram_session_busy` и другие
  terminal-попытки до текущего времени. Занятая S22 не должна порождать новый
  запуск и три сообщения в админский чат на каждом watchdog tick.
- если после scheduled `full` scan у `new_occurrences` нет candidates, scheduled publish должен завершаться bot-only служебным сообщением оператору (`новых экскурсионных находок нет`) без публикации пустого поста в каналы;
- scheduled `full` slot считается critical daily slot: если первичный APScheduler fire пропущен или записался как `ops_run(... status='skipped', skip_reason='heavy_busy')`, startup catch-up и live watchdog обязаны догонять тот же scheduled `full` path в пределах lookback окна, а catch-up-dispatch ждёт освобождения heavy gate вместо тихого пропуска дня;
- same-day `light` runs не считаются подтверждением доставки daily `full` slot: recovery должен искать materialized `guide_monitoring` именно с `details.mode='full'`, иначе вечерняя автопубликация может быть ложно признана “уже выполненной”.
- same-day `recovery_import` со статусом `success`/`partial` и `details.mode='full'` считается подтверждением доставки daily `full` slot, если он импортирует уже завершённый scheduled Kaggle output; после такого импорта watchdog не должен запускать новый full scan.
- если catch-up `full` run снова завершается `status='skipped'` только из-за занятого shared remote Telegram/Kaggle session (`remote_telegram_session_busy`), watchdog не должен считать такой dispatch завершением суточного слота, но и не должен стучаться каждую минуту: тот же scheduled `full` path откладывается до `GUIDE_MONITORING_REMOTE_BUSY_RETRY_SECONDS` (default `3600`) и пробуется снова только после cooldown, пока не materialize-ится не-skipped `full` run или не истечёт lookback окно.
- `/healthz` обязан раскрывать статусы `guide_excursions_light` и `guide_excursions_full` вместе с generic scheduler status; green health without guide job visibility is insufficient evidence for this feature. Production closure after a missed daily guide slot requires deploy evidence plus same-day catch-up/digest evidence, not just a code fix.

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
