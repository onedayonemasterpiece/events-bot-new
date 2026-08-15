# Smart Event Update (Интеллектуальный импорт)

## P0 automatic identity and source-decision contract

После `INC-2026-08-10-smart-update-identity-terminal-loss` Smart Update —
child-resolution boundary, а не второй eventness classifier. Для кандидата с
`source_disposition=EVENTS_FOUND|MIXED` regex/date/title/venue/promo/recap
детекторы могут только добавить evidence или инициировать LLM verification; они
не удаляют child, не ставят `silent` и не создают product terminal. Legacy
caller без typed source decision проходит существующий LLM eventness verifier.
Решение владельца от 2026-08-15 заменяет automatic durable retry линейным
контрактом: semantic verdict завершается в том же вызове, а provider/schema/DB/
vector failure получает видимый `FAILED_TECHNICAL` и требует явного re-drive.

Публичный результат закрыт структурно:

- accepted: `CREATED`, `MERGED`, `NOOP_EXACT_REPLAY`; только они несут
  `event_id` и разрешают downstream side effects;
- product terminal: `REJECTED_PRODUCT_POLICY` только вместе с
  `ProductExclusionReason`; free-form/unknown reason превращается в
  `FAILED_TECHNICAL`;
- technical terminal: `FAILED_TECHNICAL` + typed diagnostic reason, без
  фоновой очереди; `RETRY_SCHEDULED` остаётся только provisional/legacy value
  для одноразового post-deploy drain;
- distinct create: `IdentityDistinctReason` (`RELATED_BUT_DISTINCT`,
  `FESTIVAL_CONTEXT_SIBLING`, `UNSAFE_TO_MERGE`, explicit occurrence conflict,
  incoherent merge rollback или semantic unknown after bounded adjudication);
- provenance action: `LifecycleReason` для `ATTACH_CONTEXT`.

`diagnostic_event_id` никогда не является accepted ID. Incoherent merge
откатывается и создаёт подготовленный distinct Event. Provider/DB/vector failure
заканчивается видимым technical terminal; semantic identity unknown делает один
inline distinct-create pass. Occurrence identity выбирается в порядке
source-native ID → vendor/ticket ID → structured schedule anchor → ordinal
только как tie-breaker; разные explicit occurrence IDs являются hard distinct
rail.

Durable authority — `smart_update_candidate_state` плюс append-only
`smart_update_attempt`; Event/EventSource/facts/terminal attempt коммитятся в
одной write transaction, а LLM не выполняется под SQLite transaction.
`EventSource` связывается с `candidate_key`/`occurrence_key`; canonical source URL
может быть carrier нескольких независимых children. Legacy/structural
candidate без optional in-memory conflict set означает «нет доказанных
explicit conflicts», а не technical failure в duplicate probe. Полный
контракт:
[`identity-state-machine.md`](identity-state-machine.md).

Автоматический мердж события из разных источников без ручной модерации, с сохранением списка источников и защитой якорных полей.

## Transport-scoped duration forecast

If an event has no extracted end/duration, Smart Update may persist a separate
`event.duration_forecast_minutes` value for return-transport planning:

- the eligibility gate runs before the provider call and accepts only a valid
  single-day event with a non-default start that already matches an implemented
  rail city, an exact bus city+venue+start tuple, or exact KAUP venue policy;
- unrelated events make no duration LLM request, which bounds cost and avoids
  generating unused predictions;
- `_ask_gemma_json(..., label="duration_forecast")` uses the normal configured
  Smart Update production key/model path and only event-scoped source/OCR
  material; it is not an `agy` consultation and is not run by Astro/Kaggle;
- output must be an integer `15…720` minutes with confidence at least `0.5`;
  insufficient evidence, invalid output and provider failure fail closed;
- explicit source duration/end always wins and clears a stale forecast;
- the nullable forecast never changes canonical public timing. Static export
  may pass it to the transport helper, which labels it internally and exposes
  only a neutral approximate-end caveat.

Schema/bootstrap support lives in `models.py` and `db.py`; create and update
paths both invoke the same helper. Regression coverage is in
`tests/test_smart_update_duration_forecast.py` and
`tests/test_static_site_preview_duration.py`.

## Изображения: единый automatic gate

Все новые event images входят через `_apply_posters()` этого pipeline. Второе и
последующие logical media fail-closed остаются `pending_review`; exact raw/pixel
SHA решается детерминированно, остальные пары — bounded one-pair VLM job с
feature budgets и автоматическим retry. Только `approved` проецируется в
`Event.photo_urls` и читается Telegraph/TG/VK/static. Каноника, все ingestion
paths и rollout: [Event media](../event-media/README.md).
В production тот же gate до approval обязан materialize каждый source poster в
`static.kenigevents.ru`; existing-event ticket-status fast path не является
исключением и также передаёт текущие parser photos в `_apply_posters()`.
Даже если поступило одно сразу approved изображение и pair-review не нужен,
`_apply_posters()` явно ставит durable geometry follow-up. Worker асинхронно
сохраняет bbox всех лиц и viewer-value region в versioned pixel cache; никакого
provider-вызова и скачивания картинки внутри транзакции Smart Update нет.
Последняя картинка, повышенная до `approved` после pair review, получает такой
же follow-up. Display URL и visual fingerprints образуют один exact-pixel
contract: смена URL/path или пикселей инвалидирует старые role/focal/safe-crop/
geometry evidence. Новые managed writes адресуются точным SHA-256 закодированных
WebP-байтов и не перезаписывают perceptual-dHash URL.

## Fact-first (внедрено)

Smart Update по умолчанию строит публичный текст (`description`, `short_description`, `search_digest`) по схеме **sources → facts → text** (вариант C+D из dry‑run), то есть **строго из извлечённых фактов**, а не из “сырых” текстов источников.

## Production staged contracts (INC-2026-07-13)

Production sets `SMART_UPDATE_FORCE_STAGED_GEMINI=0` and
`SMART_UPDATE_G4_SPLIT_CREATE=1`. Only the bounded facts/writer stages plus
rare occurrence/location/anchor grounding reviews use the configured Gemini
Lite facts/writer models; core merge, coverage, revise, short-description and digest
contracts remain on the higher-RPD Gemma lane. `SMART_UPDATE_FORCE_STAGED_GEMINI=1`
is diagnostic/emergency-only: a real two-event replay consumed 20 Lite requests
(10 per event), which would exhaust the defensive 450-RPD project lane after
roughly 45 similar events. Recent production added 19–42 rows per complete
day (856 in 30 days), before counting merge-only updates, so the forced route
had no safe daily headroom. The steady-state split normally spends one Lite writer call on a matched
update and about two Lite calls on a create (facts + writer); rare grounding
reviews can add one bounded call, while core merge/derived contracts remain on Gemma. This is not a
deterministic semantic fallback: vectors/reference matching only retrieve
candidates, while the LLM still decides meaning from exact source/OCR evidence.

The hosted Gemma 4 API rejected the older numeric `thinking_budget`, but now
documents `thinking_level=minimal` as the supported off switch. The shared
Google AI client applies that default to bounded Smart Update calls so their
small output caps cannot be consumed entirely by private thought tokens;
explicit consumer thinking configuration remains possible. Create uses
native-schema split stages; the legacy generated bundle cannot fall through to
an unreviewed 4o response.

Before merge/create, social candidates now receive targeted LLM-first checks:

- the eventness reviewer receives bounded poster OCR together with source text
  and raw excerpt. If a caption merely refers readers to an attached poster,
  concrete date/program/venue evidence from that poster is valid source
  evidence and must not be rejected as an LLM hallucination;

- occurrence scope must support the target date and target city/venue together;
- doors/guest gathering/opening times are distinguished from the public start;
- an opening time is not copied to every day of a multi-day range;
- every generated public field is source/OCR-grounded, otherwise the candidate
  fails closed;
- dynamic OpenAI schema names are provider-safe, but this fallback remains
  disabled for create bundles.

Managed VK publication idempotency includes postponed posts: when
`wall.getById` cannot see a stored managed URL, the publisher checks the
authenticated `wall.get(filter=postponed)` collection before the legacy `all`
fallback. A present postponed item is edited/reused, never recreated.
VK may also assign a different public wall id during the postponed-to-live
transition. Before any hash/idempotency decision, the worker then scans only a
bounded recent managed-wall window and accepts a replacement id only for one
unique item whose generated title and date header both match exactly. Zero or
multiple matches fail closed; semantic/vector similarity is deliberately not
used for this transport-identity repair. The recovered live URL is persisted
on both the canonical event and its managed `event_source` row before the
worker can publish again.
Existence is not sufficient when canonical `photo_urls` are non-empty: a stored
text-only live/postponed item is treated as an incomplete projection and is
edited with media rather than skipped by the content-hash fast path.
If only part of a late media set can still be uploaded, a text-only projection
receives the successfully materialized subset; an already illustrated post
keeps its existing attachments on the same partial failure. This prevents the
old "preserve nothing" branch from completing a repair job without a photo.
An actual Smart Update merge also re-arms the existing managed VK projection:
the idempotent worker edits the resolved live/postponed post even when the prior
`vk_sync` job is already `done`. A no-change replay keeps the older duplicate
prevention contract and does not requeue a complete managed post. This keeps
canonical date/time/location/text repairs consistent across VK without turning
routine source rescans into repeated public edits. The managed VK content hash
also includes the canonical ICS URL: adding or removing a calendar projection
must re-render the calendar line even when the event body itself is unchanged.

Переключатель:
- `SMART_UPDATE_FACT_FIRST=1` (default) — fact‑first включён.
- `SMART_UPDATE_FACT_FIRST=0` — rollback на прежний rewrite/merge‑first путь.
- `SMART_UPDATE_FORCE_STAGED_GEMINI=0` — steady-state quota-safe routing. `1`
  must not be left enabled in production; it is only a bounded diagnostic override.
- `EVENT_PARSE_GEMMA_MODEL=gemma-4-31b-it` (default) — upstream VK/TG draft extraction uses Gemma 4 before Smart Update receives candidates. Legacy parser forcing remains explicit via `EVENT_PARSE_LLM=4o`; automatic parser fallback to 4o is disabled unless `EVENT_PARSE_ENABLE_4O_FALLBACK=1`.
- `EVENT_PARSE_GEMMA_TPM_RESERVATION_TARGET=14500` keeps Gemma 4 event-parse
  input+output+reserve-extra below the canonical 15K TPM cap. The configured
  `EVENT_PARSE_GEMMA_MAX_TOKENS` remains the upper bound, while the per-request
  output allowance is reduced only when the estimated prompt would otherwise
  make the reservation larger than one whole minute. The quality floor is
  `EVENT_PARSE_GEMMA_MIN_OUTPUT_TOKENS=2400`. If the full prompt still cannot
  fit, event-parse retries once locally with the global 100+ row venue catalogue
  omitted, while retaining every semantic extraction rule, holiday/festival
  hints, source text and poster OCR. Venue canonicalisation still runs after
  JSON decoding against the same catalogue. If that compact reservation also
  cannot fit, parsing fails before any provider call instead of waiting across
  minute boundaries or sending unaccounted traffic.
  The default output ceiling is `6000`, because the production replay showed
  Gemma 4 consuming the former 4000-token ceiling entirely in its thought
  channel. The same TPM planner still reduces that ceiling when necessary.
  Event-parse permits exactly one application/provider-SDK attempt per queue
  execution; retries are explicit queue executions and therefore separately
  visible and controllable in the shared ledger. Automatic model fallback is
  disabled for this stage. `EVENT_PARSE_GEMMA_PROVIDER_TIMEOUT_SEC=210`
  leaves bounded headroom inside the 240-second stage wall clock while allowing
  the 6000-token Gemma response to finish; the former inherited 120-second
  provider timeout truncated the production replay before a terminal response.
- Smart Update не владеет пулом Google-ключей: он использует общий gateway
  normal pool из `GOOGLE_AI_NORMAL_KEY_ENVS`. Round-robin, shared reserve и
  переход на другой допущенный lane после provider `429` выполняются в
  `GoogleAIClient`; feature-код не ждёт quota window и не перебирает ключи
  самостоятельно. Missing registry member или shared limiter закрывают вызов
  fail-closed. `GOOGLE_AI_RESERVE_OVERFLOW_KEY_ENVS` остаётся отдельным
  emergency-only механизмом.
- `GEO_REGION_GEMMA_MODEL=gemma-4-31b-it` (default) — Smart Update's region-filter LLM fallback also stays on Gemma 4.
- `SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE=1` — off-by-default production-candidate create path для Gemma 4 migration: тяжёлый `create_bundle` остаётся ответственным за извлечение фактов, а сопутствующая работа делится на лёгкие Gemma 4 native-schema стадии `lollipop.bucket_facts`, `lollipop.prioritize.weight`, `lollipop.prioritize.lead`, `lollipop.editorial.layout` и финальный writer (`smart_update.g4_lollipop_light.final_writer.v3`). Writer выводит `- item` bullets только когда соответствующая секция writer_pack содержит non-empty `literal_items`; для остальных секций требует непрерывной прозы и сливает несколько фактов, отличающихся только именем (композитор/исполнитель/произведение/роль), в одно компактное предложение, чтобы не получалась одно-словная россыпь bullets. Чтобы убрать ложно-положительный `infoblock.leak:LG03` (когда короткое имя площадки естественно встречается в narrative как «в «Сигнале»»), `_g4_lollipop_light_normalize_bucket_payload` помечает `location_name` без признаков адреса (без улицы/цифр/двух запятых) как `narrative_policy=suppress` для writer-pack infoblock-валидатора; при адресоподобном `location_name` (`ул.`, цифры, две запятые) leak-guard сохраняется. `SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE=gemma4|4o|adaptive` выбирает writer lane; в adaptive dense packs (`facts_text_clean` больше `SMART_UPDATE_G4_LOLLIPOP_LIGHT_4O_FACT_THRESHOLD`, default `14`) идут в один явный final `4o` writer call, остальные остаются на Gemma 4. Production canary должен ставить `SMART_UPDATE_G4_LOLLIPOP_LIGHT_WRITER_LANE=gemma4`, если `FOUR_O_TOKEN` намеренно не выдан, иначе writer попытается dispatch 4o call, который без авторизации сразу падает (token не тратится, но trace покажет `writer.final_4o failed` и работа уйдёт в legacy fact-first fallback вместо новой v3-prose-prompt дорожки). Лёгкие native stages ограничены `SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_TIMEOUT_SEC` (default `70`) и имеют короткий retry на transient provider failures (`SMART_UPDATE_G4_LOLLIPOP_LIGHT_STAGE_RETRIES`, default `2`).
- `SMART_UPDATE_G4_SPLIT_CREATE=1` — экспериментальный вариант `g4-split-create-v2-rich-facts` для миграции на Gemma 4: create-path не вызывает тяжёлый `create_bundle`, а делает quality-critical `rich_facts_extract` с секциями фактов, ответственный `split_description_writer` для основного Telegraph/body текста и лёгкий `split_derived_fields` для `short_description` / `search_digest`. По умолчанию выключен; `rich_facts_extract` остаётся тяжёлым fact-ledger этапом и может откатиться с native schema на prompt-schema. Основной writer не запускает повторный prompt-schema вызов, пока явно не задан `SMART_UPDATE_G4_SPLIT_CREATE_PROMPT_FALLBACK=1`; лёгкий `split_derived_fields` может откатиться на prompt-schema. Split-create стадии отключают legacy 4o fallback, чтобы Gemma 4 benchmark оставался model-clean. Основной writer ограничен `SMART_UPDATE_G4_DESCRIPTION_WRITER_TIMEOUT_SEC` (default `90`), derived stage — `SMART_UPDATE_G4_DERIVED_FIELDS_TIMEOUT_SEC` (default `20`). Если description writer недоступен после успешного извлечения фактов, create использует bounded fact-ledger fallback вместо старого многошагового fact-first/rewrite/reflow каскада. Для лекций/паблик-токов `rich_facts_extract` обязан сохранять каждый блок `ИМЯ` + роль как отдельный named-roster fact; `split_description_writer` обязан включать такие факты в narrative и не сворачивать участников в категории вроде «краеведы/учёные/эксперты». Если draft writer попал под logistics reject, Smart Update делает LLM-pass `split_description_writer_remove_logistics`, чтобы убрать повтор даты/адреса/билетов без потери фактов, и только потом переходит к fallback.
- `SMART_UPDATE_IDENTITY_GATE=off|shadow|enforce` and
  `SMART_UPDATE_MERGE_IDENTITY_GATE=off|shadow|enforce` keep their existing
  create/merge semantic evidence stages. In `enforce`, a same/source decision
  merges, a distinct/sibling/unsafe or explicit occurrence conflict creates a
  distinct Event, and provider/schema/vector/DB uncertainty closes visibly as
  `FAILED_TECHNICAL` without a background retry. There is no hidden terminal
  veto or human-review queue. Production runs
  both gates in `enforce`. Vector recall remains controlled by
  `SMART_UPDATE_IDENTITY_VECTOR_RECALL`, `SMART_UPDATE_IDENTITY_VECTOR_TOP_K`,
  `SMART_UPDATE_IDENTITY_VECTOR_MIN_SIMILARITY`,
  `SMART_UPDATE_IDENTITY_VECTOR_TIMEOUT_SECONDS`,
  `SMART_UPDATE_IDENTITY_EMBEDDING_MODEL`,
  `SMART_UPDATE_IDENTITY_EMBEDDING_DIM`, and
  `SMART_UPDATE_IDENTITY_GOOGLE_KEY_ENV`.
- After switching the identity gate to `enforce`, production should also enable the read-only `/vystavki/` acceptance audit with `ENABLE_EXHIBITION_DUPLICATE_AUDIT=1`; it records `ops_run(kind='exhibition_duplicate_audit')` and fails/alerts on high-confidence public exhibition duplicate pairs during the 14-day rollout window.

### Canonical source identity and automatic terminals

The canonical five-terminal result, `UPSERT_EVENT` / `ATTACH_CONTEXT` intents,
stable producer-child and occurrence keys, candidate-key/occurrence-scoped
EventSource uniqueness, durable attempt ledger, retry rules, caller/queue
contract, funnel balance, recovery, migration, and rollback are specified in
[`identity-state-machine.md`](identity-state-machine.md).

Exact replay means the same canonical source, candidate/occurrence key, and
packet fingerprint. It performs zero provider or Event/EventSource/fact/poster/
outbox work; only its durable attempt receipt may be written. A changed packet
with the same key proceeds as an update, while a different key in the same
carrier may create a distinct Event.

For read-only rollout evidence independent of the scheduler, run
`python3 scripts/inspect/audit_identity_gate_rollout.py --db /data/db.sqlite --since-days 14 --format both`.
It reports decision/gate/vector-error evidence
without printing secrets; candidate and attempt balance is reported separately
by the state-machine audit.

Канонический контракт и детали реализации:

- `docs/features/smart-event-update/fact-first.md`

Дополнительный guardrail для fact-first:

- чувствительные LLM-факты из linked-source merge/create (возрастные ограничения, лимиты группы/мест, длительность, концертно-музыкальные утверждения и т.п.) попадают в `event_source_fact` и публичный fact-first narrative только если подтверждаются `source_text` / `raw_excerpt` / OCR этого же кандидата; неподтверждённые факты отбрасываются как hallucination-prone.


## VK/source parse quality boundary

Smart Update получает уже типизированный source verdict и полный evidence
manifest от producer. Source/OCR-grounded normalization ссылок, placeholder
очистка и reference venue normalization допустимы; подозрительная площадка,
weak title, date/OCR conflict, collapsed occurrences или incomplete evidence
маршрутизируются в existing LLM verifier/retry. Ни один такой сигнал не удаляет
положительный child или siblings. Media assignment остаётся отдельным
условным media task и не владеет eventness.

## Что реализовано

- Матчинг кандидата с существующими событиями:
  - быстрые shortlist-сигналы по `ticket_link` и `poster_hash`;
    - `ticket_link` сам по себе больше не считается достаточным основанием для broad auto-merge: recurring/multi-event кейсы могут делить один и тот же билетный URL;
  - soft city match для shortlist/idempotency: при `candidate.city=<город>` в матчинг также попадают legacy-события с пустым `event.city`, чтобы повторный импорт того же события (уже с нормализованным городом) сходился в merge, а не создавал дубль;
  - детерминированный матч по строгим якорям (дата + начало времени/пустое время + площадка + нормализованный `title`) до LLM, чтобы одинаковые репосты из разных источников не создавали дубль;
    - для сравнения заголовков `ё` нормализуется в `е` (только в match-слое, публичный `title` не меняется);
    - если `location_name` расходится как короткий алиас vs официальное длинное название, shortlist/match может сойтись по нормализованному `location_address`, чтобы одинаковое событие не дублировалось только из-за формы записи площадки;
  - дополнительный детерминированный матч по `date + explicit time + location + related title` до LLM: если заголовок отличается только уточняющим хвостом (`Гегель` vs `Гегель: философия истории`), Smart Update старается слить событие без зависимости от LLM; особенно важно для `parser:*` источников, где shortlist может быть шире из-за разрешённой коррекции времени;
  - поверх базовых safe-guards действует узкий Stage 04 deterministic layer для очевидных дублей и false-friend блокеров:
    - `same_post_exact_title`: один и тот же `source_url`, exact normalized title, та же дата и без явного time conflict;
    - `same_post_longrun_exact_title`: то же для long-running событий при совпадающем `end_date`, даже если в одном источнике время `doors/start`, а в другом время экскурсии/слота;
    - `broken_extraction_address_title`: same-source duplicate rescue, когда один `title` выглядит как адрес/артефакт extraction bug, но текст источника одинаковый;
    - `specific_ticket_same_slot`: merge только для narrow case `same date + same slot + same venue + same specific ticket`, и только если есть общий source/text anchor;
    - `copy_post_ticket_same_day`: merge для cross-source repost/copy cases, когда совпадают `date + ticket_link`, тексты источников практически одинаковые, а `title` имеет сильное пересечение; правило специально позволяет пережить шумный `city/location_name` у одного из repost-источников;
    - `doors_start_ticket_bridge`: merge для пар вроде `19:30 doors / 20:00 start`, когда в тексте/OCR явно присутствует оба времени и прочие якоря совпадают;
    - `copy_post_same_day_text`: merge для same-day repost/copy cases с почти идентичным `source_text` и тем же venue даже тогда, когда один источник несёт shortlink, а другой generic ticket URL/button-only CTA;
    - `doors_start_text_bridge`: отдельный bridge для copy-post family без ticket anchor, когда один источник ошибочно заякорился в `сбор гостей 19:00`, а другой в `начало 20:00`, но текст явно содержит оба времени и это всё ещё один и тот же слот;
    - `prose_location_same_slot_text`: если extractor протащил в `location_name` очевидный prose-фрагмент, Smart Update не использует этот текст как venue anchor; он может смержить только при совпадении `date + explicit time + related title + near-identical source_text`, иначе кандидат fail-closed как `invalid:prose_location` и не создаёт публичную карточку с prose-venue;
    - `cross_source_exact_match`: exact-title cross-source merge при одинаковых `date + time + venue`; это осознанно узкий rescue, а не общая cluster heuristic;
    - `city_noise_exact_title_shortlist`: если из-за `event.city` shortlist потерял exact-title duplicate, Smart Update точечно добирает city-mismatch событие обратно только при совпадении `date + venue + exact title` и без time conflict;
    - `city_noise_copy_post_shortlist`: отдельный city-noise rescue для multi-event repost families; Smart Update может добрать событие из другого города обратно в shortlist только при совпадении `date + ticket_link` и почти идентичном `source_text`, чтобы не терять merge из-за шумного venue override;
    - `generic_ticket_false_friend`: blocker против ложного merge, когда два события делят generic ticket URL и слот, но заголовки семантически разные;
    - `multi_event_source_blocker`: blocker для same-source schedule/program постов, если на одном `source_url` уже висит несколько active child events;
    - `cross_sibling_redirect`: если deterministic/LLM path выбрал sibling из той же source family (`vk wall owner` / `tg channel`), но его `title` не связан с кандидатом, Smart Update перенаправляет merge на другого sibling в shortlist только при совпадении `same date + same venue + exact/related title`;
    - venue-normalization в этом слое намеренно узкая: форматный/translit шум (`Bar`/`Бар`, служебные префиксы вроде `бар`) помогает сравнению строк, но не считается самостоятельным доказательством merge;
  - LLM‑матчинг (JSON‑ответ с `match_event_id`, `confidence`).
    - в prompt LLM `time=00:00` и `time_is_default=true` трактуются как слабый/placeholder якорь времени;
    - LLM отдельно инструктируется склеивать события при совпадении дата+площадка+контекст (участники/афиша/OCR), даже если заголовки сформулированы по-разному (общее vs конкретное название постановки).
    - если LLM с высокой уверенностью (`confidence >= 0.95`) нашёл совпадение и жёсткие якоря не конфликтуют (дата, площадка, явное время), deterministic title guard не имеет права отменять merge только по `unrelated_titles`; детерминированный слой остаётся safety rail для фактических конфликтов, но не semantic owner для harmless title drift / русских падежей (`Валерия` vs `Концерт Валерии`).
  - Telegram Monitoring помечает extracted time как `time_is_default=true`, если это время не поддержано текстом поста, linked source text или OCR. Такой кандидат может смержиться с уже существующей карточкой по дате/площадке/смыслу, не создавая дубль только из-за неподтверждённого времени.
  - rescue‑match на create‑пути: если LLM на шаге `match_or_create` выбрал `create`, но вернул осмысленный `bundle.title`, Smart Update делает дополнительную детерминированную попытку матчинга по shortlist через `bundle.title`, чтобы не создавать дубль при слабом/плохом `candidate.title`.
  - **LLM dedup adjudicator (widened recall, create-path only — INC‑2026‑05‑30 opt 1).** Самый последний рубеж: когда все детерминированные матчеры, `match_or_create` bundle, rescue‑match и `_pre_create_duplicate_probe` сказали «нет совпадения», настоящий дубль мог просто не попасть в anchor‑gated shortlist (дрейф строки площадки или времени doors/start). Поэтому Smart Update делает **отдельный широкий recall** (дата ±1 день + soft‑city, БЕЗ фильтра по точной площадке/времени), сужает его дешёвым blocking‑ключом (`_titles_look_related` OR совпадение площадки OR паритет `ticket_link` OR пересечение poster‑hash, топ‑8) и спрашивает LLM (`_llm_dedup_adjudicator`, decision‑only JSON: `action/match_event_id/confidence/reason_code/reason`) — это дубль или отдельное событие. Промпт явно: doors‑vs‑start и алиас/касса/билетный‑оператор площадки — НЕ признак различия; два разных вендора билетов на один слот — это всё ещё один ивент; но несколько сеансов из одного поста, утренник+вечер, разные шоу и `allow_parallel` площадки — РАЗНЫЕ события. Решение проходит детерминированный guard‑ladder `_dedup_adjudicator_accept_merge` (порог `confidence ≥ 0.80`, для `allow_parallel` `≥ 0.90`; veto по конфликту времени кроме `doors_start_skew ≤ 90 мин`; unrelated‑title overrule кроме `junk_location_same_venue`; `generic_ticket_false_friend`; и **жёсткий инвариант**: один и тот же `source_url` + конфликт времени ⇒ всегда create — это легитимный multi‑session split вроде `5426/5427` из `t.me/gusmuseum/4509`). Адъюдикатор работает ТОЛЬКО на create‑пути (никогда не переопределяет уже найденный match), не запускается для `parser:*` и под `anchor_forced`, и управляется флагом `SMART_UPDATE_DEDUP_ADJUDICATOR` (default ON). Он не противоречит детерминированным rescue‑веткам выше — запускается строго после них.
  - **Vector identity gate evidence.**
    `event_identity.py` defines the `identity_candidate_v1` compact candidate
    document format for embedding recall. It uses provenance-labelled canonical
    semantic fields aligned with `related_v1`, bounds/truncates large fields,
    and carries a stable SHA-256 hash. The
    Smart Update create-path gate can generate an ephemeral Gemini embedding
    (not stored), call the service-role-only Supabase RPC
    `event_identity_candidates_by_embedding_v1` over existing `related_v1` and
    `search_v3` vectors, hydrate the nearest SQLite event by id, and feed that
    vector evidence into deterministic create-veto checks. The ephemeral
    embedding provider call must go through `GoogleAIClient.embed_content_async()`
    and `google_ai_reserve`/`google_ai_finalize`; direct `embedContent` calls are
    disabled unless an explicit local/debug bypass is set. Raw source excerpts,
    poster OCR, URLs, ticket logistics and perceptual hashes remain structured
    evidence for later identity gates and are excluded from the semantic vector.
    `search_v3` is broad recall evidence only; high vector similarity alone is not enough to block
    recurring/single-slot events with different explicit dates.
  - create-bundle title guard теперь жёстче:
    - prompt прямо запрещает редакционные/идеологические заголовки вместо фактического имени события;
    - если явного имени нет, LLM должен предпочесть нейтральный формат вроде `<event_type> в <venue>`, а не промо-слоган;
    - deterministic grounding для `bundle.title` теперь требует не случайного одного токена, а более широкого пересечения с source text / raw_excerpt / OCR, чтобы не проходили fabricated titles вроде `8 марта — ...`, когда из источника реально извлекаются только дата и общий повод.
  - для multi-day событий используется пересечение диапазонов дат (`event.date..event.end_date` vs `candidate.date..candidate.end_date`), чтобы апдейты текущих выставок не создавали дублей.
  - VK intake теперь прокидывает `post_id/group_id` из `wall-...` URL в `source_message_id/source_chat_id`, чтобы same-post idempotency могла сойтись ещё на раннем anchor-path даже до полного materialize `event_source`.
- Мердж:
  - перед любыми side effects может запускаться `SMART_UPDATE_MERGE_IDENTITY_GATE`; он защищает уже выбранный match от ошибочной склейки родственных, но разных событий (например выставка той же площадки/кампании и отдельная лекция). Gate не создаёт replacement-rows и не делает repair; он только разрешает merge или безопасно пропускает side effects с логом решения.
  - якорные поля (`date/time/location_name/location_address/end_date`) в целом не меняются автоматически;
    - исключение: `parser:<site>` может уточнять якоря при мердже;
    - исключение: время может быть заполнено/уточнено, если оно было пустым/placeholder или помечено как низкоприоритетное (`event.time_is_default=1`).
  - исключение для длинных событий (`выставка`, `ярмарка`): продление `end_date` допускается по trust (если trust кандидата не ниже накопленного trust события);
  - для длинных событий (`выставка`, `ярмарка`) later exact announcement может исправить legacy start date только если существующий диапазон выглядит синтезированным (`end_date_is_inferred=true`), а новая дата явно grounded в `source_text` / `raw_excerpt` / OCR; это не разрешает произвольное переписывание source-grounded anchor fields.
  - описание и необязательные поля обогащаются через LLM (Gemma): **журналистский рерайт, не дословно** (не только для Telegram — для всех внешних источников);
    - при create-bundle `short_description` теперь дополнительно ограничивается на factual framing: модель должна описывать, что происходит (формат/участники/жанр), а не добавлять символические/идеологические интерпретации;
    - create-bundle facts теперь могут возвращать до 24 атомарных фактов и явно приоритизируют организаторов/институции, цель события, методологию/background, точные числа/статистику, участников/ведущих/модераторов/гидов/исполнителей, программу/примеры и условия участия. Это LLM-first tightening для Gemma 4 migration: если источник плотный, модель должна сокращать декоративные оценки, а не терять организатора, цель, числа или функцию модератора/ведущего/гида.
    - rich-facts/create-bundle prompts трактуют организатора, сообщество, площадку и источник вдохновения (`Плоский мир Терри Пратчетта` и т.п.) как identity facts: их нужно сохранять из явного source evidence, не заменяя тематическими догадками или названием другого сообщества.
  - для всех LLM-запросов Smart Update (rewrite/merge) действует орфографическое правило: при любой модификации текста нужно **сохранять/использовать букву `ё`** в словах, где она нормативна (не заменять её на `е`, если слово в источнике или по норме пишется через `ё`);
  - конфликты фиксируются в логах (`added_facts`, `skipped_conflicts`).
  - Telegraph страница события рендерится из `event.description` (смёрженного/отрерайченного текста), чтобы новые факты из дополнительных источников не терялись.
  - если LLM-рерайт временно недоступен при **первичном** импорте, Smart Update **не публикует** полный `source_text` дословно в `description`:
    - `source_text` сохраняется отдельно (для логов/дедупа/повторных проходов);
    - в `description` используется `raw_excerpt` (если есть) или краткий fallback‑фрагмент/дайджест, чтобы Telegraph не выглядел пустым, но и не превращался в «копипаст».
  - если при **первичном** импорте LLM вернул слишком короткий “дайджест” при богатом источнике (в т.ч. когда фактология лежит в OCR афиш), Smart Update делает второй проход `rewrite_full`, чтобы на Telegraph оставался осмысленный основной текст, а не 1–2 предложения.
  - `search_digest` (по смыслу это `search_description`) хранит **краткий** дайджест для поиска/карточек и не заменяет `description`.
    - `search_digest` теперь **пересобирается** при существенном изменении `description` (LLM, 1 предложение), чтобы короткое резюме оставалось согласованным с полным набором фактов после каждого нового источника.
  - `short_description` для публичных списков (`/daily`, страница фестиваля, месяц/выходные) формируется LLM как **ровно 1 законченное предложение на 12–16 слов** (без даты/времени/адреса/ссылок и без `...`/`…`); при merge/create Smart Update переобновляет поле, если текущий текст не проходит этот формат.
  - длина `description` ограничена сверху (по умолчанию 12000 символов) для защиты от раздувания текста; настраивается через `SMART_UPDATE_DESCRIPTION_MAX_CHARS`.
  - опционально можно включить **умеренные эмодзи** в полном тексте `description` (для читабельности, без перегибов):
    - `SMART_UPDATE_DESCRIPTION_EMOJI_MODE=light` (по умолчанию `light`; отключить: `off`);
    - лимит: `SMART_UPDATE_DESCRIPTION_MAX_EMOJIS` (по умолчанию 3);
    - опционально: `SMART_UPDATE_DESCRIPTION_EMOJI_ALLOWLIST` (строка со списком допустимых эмодзи, через пробел; если не задано, используется дефолтный набор).
    - `search_digest` и `title` остаются без эмодзи (по prompt-правилам), чтобы списки/дайджесты не превращались в «ёлку».
  - для коротких Telegram‑источников (типичный случай: 1–2 строки расписания) действует доп. anti-overexpand guard:
    - и в create‑bundle, и в rewrite: итоговое описание не должно быть длиннее доступного объёма источников (текст поста + OCR афиши + связанные источники);
    - если LLM всё же “раздула” описание (hallucination-prone вода/клише), Smart Update делает дополнительный LLM‑pass `shrink_desc` (без добавления новых фактов), чтобы привести объём к бюджету источников.
  - лимит генерации рерайта Gemma настраивается через `SMART_UPDATE_REWRITE_MAX_TOKENS` (важно: это не “краткий тизер”, а полный текст для Telegraph), а сколько исходного `source_text` попадает в prompt рерайта — через `SMART_UPDATE_REWRITE_SOURCE_MAX_CHARS` (полезно для длинных страниц/программ).
  - лимиты мерджа (LLM JSON merge) настраиваются отдельно: `SMART_UPDATE_MERGE_MAX_TOKENS`, `SMART_UPDATE_MERGE_EVENT_DESC_MAX_CHARS`, `SMART_UPDATE_MERGE_CANDIDATE_TEXT_MAX_CHARS`.
  - legacy non-event/promo/recap/date detectors сохранены только как diagnostic hints. Для automatic VK/TG они не имеют terminal authority: semantic product exclusion приходит из complete-evidence structured source verdict, а конфликт или uncertainty планирует verification/retry.
  - в Telegram UI длинный `description` показывается усечённо (по умолчанию до 900 символов) чтобы не упираться в лимит 4096 символов сообщения; настраивается через `EVENT_DESCRIPTION_TELEGRAM_PREVIEW_CHARS`. Полный текст публикуется на Telegraph.
- Санитаризация текста:
  - хештеги на страницах события запрещаются инструкциями LLM (rewrite/merge/facts/reflow); детерминированный regex-strip `#...` в event-pipeline не используется.
  - Telegram custom emoji (PUA / `<tg-emoji>`) вычищаются из текста перед публикацией.
  - JSON-ish escape-последовательности, случайно попавшие в `description`/Telegraph body (`\n`, `\r`, `\t`, `\"`), разворачиваются обратно в нормальный текст до сохранения/рендера, чтобы на публичной странице не появлялись буквальные `\n`.
  - промо‑фрагменты (скидки/промокоды/«акция») должны игнорироваться/удаляться **внутри LLM** по инструкциям промпта (детерминированного regex‑стрипинга нет), при этом факты события сохраняются.
  - маркеры списков из Telegram (`·`, `•` и т.п.) нормализуются в Markdown (`- ...`), чтобы списки не “схлопывались” при рерайте.
  - Markdown‑списки (`- ...`) в `event.description` должны рендериться на Telegraph как `<ul>/<li>` (иначе пункты превращаются в абзацы и между ними появляются лишние `&#8203;`-спейсеры).
  - для event/source Telegraph‑страниц перед блоком списка (`<ul>/<ol>`) не добавляется дополнительный `&#8203;`‑spacer; прочие межблочные пустые строки сохраняются.
  - разделители `---` / `<hr>` в описании считаются **внутренними** (body dividers): они не должны “обрывать” основной текст при вставке месячной навигации в футер Telegraph.
  - для коротких Telegram-списков (2–6 пунктов) есть safety‑net: если рерайт потерял список целиком, он добавляется в `description` как отдельный блок (без ссылок/контактов).
  - для коротких Telegram-сниппетов (примерно до 350 символов) рерайт дополнительно ограничивается по длине (≈ исходник + 100) и вычищает типовые нейро‑клише (например «это создаёт ...»), чтобы не раздувать текст и не “допридумывать”.
- Semantic filters:
  - giveaway/promo/recap wording передаётся source LLM как evidence. Complete-evidence typed verdict может подтвердить no-event/product exclusion; положительный event child не отменяется повторным regex-классификатором Smart Update.
  - даты без явного года теперь якорятся к дате публикации источника с окном `recent past`: если пост вышел `12 марта 2026`, то `11 марта` трактуется как `2026-03-11`, а не `2027-03-11`; на следующий год дата переносится только когда anchor-year вариант действительно слишком далеко в прошлом.
  - Детерминированный `possible_past_event` — только semantic hint/verification fact. Он не может удалить positive typed source child или запустить `skipped_past_event`; конфликт publish date/source/OCR/event date разрешает source LLM/verifier.
  - посты‑расписания (несколько событий в одном Telegram сообщении): LLM инструктируется писать описание только про **конкретное** событие и не переносить “чужие” строки расписания. Детерминированного разрезания/удаления расписания после LLM сейчас нет.
  - строки вида `DD.MM | Название` считаются **низкосигнальным шумом** и должны не попадать в итоговый narrative‑текст по инструкции LLM.
  - LLM отдельно инструктируется держать narrative-структуру: короткие абзацы (обычно 1-2 предложения), без «стены текста», с логичным порядком фактов.
  - LLM не должен дублировать в основном тексте карточки строковые якоря (`Дата/Время/Локация/Билеты`), потому что они уже выводятся в summary-блоке Telegraph.
  - защита от ложной «платности»: суммы в контексте `компенсация/выплата/вознаграждение/гонорар/приз/подарок` не трактуются как цена билета; донорские акции автоматически помечаются как бесплатные, чтобы summary‑блок не показывал «🎟 Билеты … руб.».
  - для читабельности `event.description` может содержать **лёгкую Markdown-разметку**, которая корректно рендерится на Telegraph:
    - заголовки `###`
    - цитаты блоком `> ...` (например цитата режиссёра)
    - редкое выделение `**...**`
    - при этом запрещены Markdown-ссылки `[текст](url)` и таблицы.
  - заголовок события (`event.title`) при создании/мердже может быть улучшен LLM с учётом заголовка афиши (`poster_titles`/OCR),
    чтобы не терять ключевые смысловые маркеры (например «Масленица») и не превращать title в “кусок описания”.
  - если входной `title` выглядит как generic-fallback вида `<event_type> — <площадка>` (например «Концерт — Янтарь холл»)
    или как category-only заголовок без отличительного собственного имени при наличии такого имени в `source_text`/OCR
    (например `Городской фестиваль` при source headline `Городской фестиваль «ВЕЛОДЕНЬ»`), Smart Update сначала просит LLM
    найти формальное собственное название/бренд события в `source_text`/OCR/фактах; если такого имени нет, запускается второй
    LLM-first recovery pass для короткого публичного заголовка из grounded темы, программы, участника/коллектива, фестиваля/проекта,
    праздника или центрального произведения/объекта события. Это закрывает афишные источники без официального нейминга
    (`Pianissimo: Илья Папоян`, `Розовый натюрморт`, `День защиты детей в Юности`) без детерминированного смыслового угадывания.
    Детерминированный guard только маршрутизирует слабый заголовок к LLM и не выбирает replacement сам; результат принимается только
    если он не является тем же `<тип> — <площадка>` шаблоном и все смысловые токены заголовка подтверждаются тем же source/OCR/fact corpus.
  - цитаты должны быть устойчивыми при мердже: если в предыдущей версии описания уже была релевантная цитата (blockquote),
    то при добавлении новых источников (например `/parse`) Smart Update старается **сохранить цитату** (и не заменить её
    целиком на “косвенную речь”).
  - прямые цитаты в «ёлочках» (например `Цитата: «Можно прийти слушать…»`) best-effort промотируются в blockquote, чтобы Telegraph рендерил их как `<blockquote>`.
  - отзывы зрителей вида `- Лариса: ...` в блоках с контекстом “отзывы/зрители/мнения” детерминированно промотируются в blockquote (`>`): текст становится цитатой, а автор переносится в строку атрибуции `— Лариса` для более “журнального” оформления.
  - технические заголовки/термины вроде `Facts/Added Facts` / `Факты` не должны попадать в публичное описание:
    LLM явно инструктируется их не писать, а safety-net удаляет только *самостоятельные* строки-заголовки (без вырезания содержимого),
    а также вычищает префиксы `Facts:`/`Факты:` как артефакт форматирования (в т.ч. `**Facts:**`/`**Факты:**` внутри абзаца).
  - safety-net для разметки: если LLM случайно оформил абзац как подзаголовок (`### ...`) и получился “гигантский заголовок”,
    Smart Update демотирует такой heading обратно в обычный абзац (форматирование без изменения смысла).
  - safety-net для разметки: “осиротевшие” подзаголовки (например `### Подробности` без текста, после которого сразу идёт следующий `### ...`)
    удаляются, чтобы на Telegraph не появлялись пустые секции.
  - если в blockquote “случайно” попало много повествовательного текста (несколько предложений), Smart Update
    разрезает его: **в цитате оставляет первую фразу**, а остальное выносит в обычный абзац.
  - дополнительно: повторяющиеся абзацы удаляются **без потери форматирования** (сравнение нормализованное, с игнором
    хвостовой пунктуации и zero-width символов).
  - детерминированный анти-дубль: если в описании появляются обрезанные/усечённые фрагменты (например текст оборвался на середине слова) или предложения-дубликаты как подстрока более полного предложения, они вычищаются.
  - source-grounded spelling guard для узких известных proper-name регрессий может вернуть написание из источника после LLM writer, если источник явно содержит корректную форму; `INC-2026-05-08-vk-quality-false-skips` фиксирует кейс `Симуран`, где публичный writer изменил фамилию на `Симюран`.
- Источники:
  - таблица `event_source` хранит все источники события;
  - idempotency по `telegram_scanned_message`;
  - URL сам по себе не является idempotency verdict: exact replay требует совпадения stable candidate/occurrence identity и fingerprint и возвращает typed `NOOP_EXACT_REPLAY`; изменённый packet проходит update/match pipeline;
  - если `check_source_url=False` (переобработка разрешена), Smart Update всё равно пытается **сойтись** в уже созданное событие по якорям источника, чтобы ретраи/повторные импорты не плодили дубли:
    - Telegram: `(source_message_id + source_url)`,
    - VK: `source_vk_post_url/source_post_url`,
    - fallback: поиск по `event_source.source_url`, если источник уже был сохранён у события.
    - форс‑матч применяется только когда он безопасен: если на один `source_url` приходится несколько событий (schedule‑посты), матч делается только при одинаковой “сигнатуре дубля” (дата + начало времени/пустое время + площадка + нормализованный `title`).
  - для Telegram `group/supergroup` постов, где автор публикации — пользователь Telegram, мониторинг может использовать автора как fallback‑контакт для `ticket_link`, если явная ссылка/handle на запись не найдена и в тексте нет phone/email контакта; приоритет: `https://t.me/<username>`, fallback — `tg://user?id=<id>`.
- Identity/schema foundation:
  - `event.identity_status` хранит статус идентичности карточки (`canonical` по умолчанию; будущий merge/gate сможет помечать merged/review states без смешивания с `lifecycle_status`);
  - `event.merged_into_event_id` хранит каноническую карточку, если текущая строка позже будет сведена как дубль;
  - `event.date_is_inferred`, `date_provenance`, `date_confidence`, `end_date_provenance`, `end_date_confidence` добавлены как first-class provenance/trust поля для будущих решений о датах; существующий `end_date_is_inferred` сохраняется и остаётся отдельным durable marker;
  - `event_identity_decision_log` — append-only foundation для решений identity gate/adjudicator: участвующие event/source ids, решение, причина, confidence, actor и JSON payload;
  - `event_identity_lock` — per-event lock для временной или ручной защиты identity от автоматического merge/update.
- Trust‑логика:
  - хранится `event.ticket_trust_level` и применяется при обновлении ticket‑полей.
  - для даты есть явная лестница provenance/trust: `missing` → `ungrounded` → `source_text` → `poster_ocr` → `canonical_source` → `operator`;
    обычный merge не переписывает `event.date` только потому, что новый источник принёс другую дату. Разрешённые автоматические случаи остаются узкими:
    canonical `parser:*`/site source может исправить якорь, а grounded дата из source text/OCR может исправить старый inferred range у long‑running события при совпадающей площадке.
  - регулярные/сезонные события с `end_date` не считаются той же публичной карточкой, что и свежая точная occurrence внутри диапазона:
    LLM match/merge prompts должны выбирать новую occurrence для источников вида `10 июля 20:00`, если существующая строка означает сезон/серии (`1 мая — 30 сентября`, `каждую пятницу`).
    Узкий deterministic guard только поддерживает это fail-closed: общий ticket/title/place/poster не должен заставлять Smart Update мутировать сезонную карточку свежей афишей одного слота.
  - афиши при merge дедуплицируются не только по `poster_hash`, но и по `supabase_path`, точному `phash`, а затем по точному URL как weak fallback;
    если новый storage URL относится к уже сохранённой афише, строка `eventposter` обновляется без добавления визуального дубля в `event.photo_urls`.

## Где используется

- Telegram Monitoring (`source_parsing/telegram/handlers.py` → `smart_event_update.py`).
- VK ingestion (`vk_intake.persist_event_and_pages`).
- VK auto-import очереди (`vk_inbox`) (`vk_auto_queue.run_vk_auto_import`).
  `FAILED_TECHNICAL` переводит carrier в terminal `failed_technical`, сохраняет
  typed reason в packet/ops report и явно пишет «без автоматического повтора»;
  он не превращается обратно в `PERSIST_ERROR`/`deferred`.
- Ручной импорт (`add_events_from_text`, `/addevent_raw`).
- `/parse` (site parsers): источник сайта добавляется/мерджится через Smart Update, когда у события ещё нет этого `parser:<site>` источника.
- Outgoing post jobs: `schedule_event_update_tasks()` ставит Telegraph rebuild, VK event post и Telegram event post ([Telegram Event Publishing](../tg-publishing/README.md)) для активных будущих/текущих событий.
  Для managed VK event posts `vk_sync` считается идемпотентным и по отложенным постам тоже: если `wall.getById`
  не видит stored URL, но `wall.get` находит запись по тому же postponed/live id или единственный live-пост с
  точным generated title + date header, Smart Update сохраняет актуальный live id и не создаёт второй VK-пост
  для того же `event_id`. Неоднозначный поиск всегда fail-closed.

## Фестивали и фестивальная очередь

Smart Update содержит детекцию фестивалей как часть основного LLM‑разбора (Gemma) — отдельного запроса нет.

Ожидаемые поля LLM:
- `festival_context` = `festival_post` | `event_with_festival` | `none`.
- `festival` (короткое имя серии) и при наличии `festival_full` (полное название выпуска).
- ссылки/сигналы фестиваля (сайт/соцсети/программа), если есть.

Поведение:
- `festival_post`: событие **не создаётся**, источник ставится в фестивальную очередь.
- `event_with_festival`: событие создаётся/обновляется как обычно, выставляется `event.festival`.
- если источник или LLM ошибочно помечает как `festival_post` один сильный event draft (дата + площадка/маршрут + время/тип/билет), festival detection понижает его до `event_with_festival`/`none`, чтобы одиночные мастер-классы, лекции, показы или спортивные старты внутри цикла не терялись; bullet-списки материалов/условий без нескольких дат или времён не считаются самостоятельной программой фестиваля;

Дополнительно:
- псевдо‑фестивали из `docs/reference/holidays.md` участвуют в матч‑логике;
- «День <…>» + признаки программы/многодневности рассматриваются как фестивальный сигнал (см. `docs/features/festivals/README.md`);
- «программа/расписание/план мероприятий» усиливает `festival_post`, если есть несколько дат/времён или список активностей;
- Smart Update сообщает оператору, что источник попал в очередь, показывает команду ручного запуска и статус автозапуска.

Каноническое описание серии/выпусков и очереди: `docs/features/festivals/README.md`.

## Единый генератор Telegraph (инвариант)

Для всех источников событий используется один и тот же финальный рендер события в Telegraph:

- `JobTask.telegraph_build` в `main.py` всегда привязан к `update_telegraph_event_page`.
- Эта функция строит страницу из `event.description` (fallback `source_text`) через `build_source_page_content`.
- `telegram monitoring`, `/parse`, `vk_auto_import` и ручной VK review не имеют отдельного "своего" рендерера страницы события: они сходятся в этот же handler через JobOutbox или явный вызов `_ensure_telegraph_url` (который вызывает `main.update_telegraph_event_page`).
- Рендер поддерживает lightweight Markdown (например, `**bold**`, `_italic_`) и делает best-effort балансировку HTML-тегов (`b/i/a/...`), чтобы `telegraph.utils.html_to_nodes` не падал на mis-nesting (часто встречается при смешивании `**...` и `_...`).
- Для подзаголовков внутри `event.description` (`h3/h4`) рендер не вставляет дополнительный пустой spacer перед первым текстовым блоком, но сохраняет обычные отбивки между последующими абзацами.

Важно: если `telegraph_edit_page` возвращает `PAGE_ACCESS_DENIED`, создаётся новый path (`telegraph_create_page`), поэтому старый URL может остаться неизменным. Канонический URL всегда хранится в `event.telegraph_url` и показывается в отчётах Smart Update.

Разделение ответственности:
- `/parse` решает, нужен ли Smart Update для существующего события (source-aware проверка `parser:<site>`).
- Smart Update выполняет сам merge, лог источников (`event_source`, `event_source_fact`) и правила приоритетов фактов.

## Важные файлы

- `smart_event_update.py` — основная логика матчинга/мерджа.
- `docs/reference/location-flags.md` — `allow_parallel_events`.
- `models.py` / `db.py` — `event_source`, `event_identity_*`, `eventposter.phash`, `telegram_*` таблицы.

## Примечания

- Smart Update защищает якорные поля и дополняет данные из разных источников.
- Legacy `skipped_non_event:*` strings are compatibility inputs only and cannot authorize a terminal without a closed `ProductExclusionReason`. Unknown/free-form reasons fail closed to visible `FAILED_TECHNICAL`.
- Каждый вызов Smart Update должен иметь заполненные `source_type` и `source_url` (они влияют на лог источников и счётчик в Telegraph).
- Для ручного добавления через бота (`/addevent`, `/addevent_raw`) источник фиксируется как **bot**.
  - Для источника `bot` deterministic region filter не блокирует создание при пустом `city`.
  - Для источника `bot` сохраняется операторский `title` (LLM может обогащать `description`, но не должен “переименовывать” событие).
- В operational логах Smart Update при каждом LLM вызове фиксируются `label` и **конкретная модель** (`SMART_UPDATE_MODEL`) для трассировки качества рерайта/мерджа.
  - Надёжность LLM:
    - Smart Update делает мягкие ретраи запросов в Gemma (`SMART_UPDATE_GEMMA_RETRIES`, по умолчанию 3) с экспоненциальной паузой (`SMART_UPDATE_GEMMA_RETRY_BASE_SEC`, по умолчанию 1.0s).
    - Если Gemma не отвечает/падает после ретраев, Smart Update переключается на fallback **4o** (требует `FOUR_O_TOKEN`) и сообщает об этом в чат оператора (LLM incident), чтобы было видно, что использовалась другая модель.
  - Выбор модели:
    - Smart Update использует **Gemma как primary**; переключение на 4o возможно **только** как fallback при ошибках/недоступности Gemma.
    - `SMART_UPDATE_LLM` поддерживает значения: `gemma` (default) и `off` (отключить LLM в Smart Update для offline/E2E).
    - `SMART_UPDATE_MODEL` задаёт конкретную модель Gemma (по умолчанию `gemma-4-31b-it`).
    - Smart Update использует общий `GoogleAIClient` с `default_env_var_name=GOOGLE_API_KEY`; reserve не должен самовольно выбирать `GOOGLE_API_KEY2`, если это не отдельный explicitly-scoped consumer.

## allow_parallel_events и hall_hint (защита от ложных мерджей)

Для площадок с параллельными событиями (например, «Научная библиотека») матчинг учитывает **hall_hint**
(зал/аудитория/лекторий/этаж и т.п.). Hall_hint извлекается не только из текста, но и из OCR афиш,
чтобы не склеивать разные события в один слот.

## Канонизация локаций (алиасы)

- В Smart Update есть канонизация части частых алиасов на входе кандидата:
  - `Научная библиотека` / `Калининградская областная научная библиотека` -> `Научная библиотека, Мира 9, Калининград` (без смешивания с БФУ).
  - Для VK-источника КОНБ (`wall-30777579_*`/`konb39`) room/floor labels вроде
    `читальный зал`, `2 этаж`, `4 этаж лекционный зал` не считаются публичной
    площадкой: при source-grounding и адресе `Мира 9` они нормализуются в
    `Научная библиотека, Мира 9, Калининград`, а room/hall остаётся только
    вспомогательным hall_hint для дедупликации.
  - `Дом китобоя` + вариации адреса (`пр-т Мира 9` и т.п.) -> `Дом китобоя, Мира 9, Калининград`.
  - `Закхайм*` / `Ворота галерея` / `Арт-пространство Ворота` -> `Закхаймские ворота, Литовский Вал 61, Калининград`.
  - `Фридланд*` / `Дзержинского 30` -> `Фридландские ворота, Дзержинского 30, Калининград`.
  - `Железнодорожн*` / `Гвардейский проспект 51А` / `Генерала Буткова` -> `Железнодорожные ворота, Гвардейский проспект 51А, Калининград`.
- Важно: голое имя `Ворота` больше не считается безопасным canonical alias, потому что в Калининграде это неоднозначно как минимум между Закхаймскими, Фридландскими и Железнодорожными воротами.
- Общая канонизация известных площадок теперь идёт через reference-слой:
  - `docs/reference/locations.md` — канонические площадки;
  - `docs/reference/location-aliases.md` — data-driven алиасы/опечатки без venue-specific regex веток.
- Практические кейсы, которые теперь нормализуются в один canonical venue:
  - `Bar Sovetov` / `Бар Советов` / `Баре Советов` -> `Бар Советов, Мира 118, Калининград`;
  - `Суспирия` / `Сусппирия` + `Коперника 21` -> `Бар Суспирия, Коперника 21, Калининград`.
  - `Третьяковская галерея` / `Третьяковская галерея, Калининград` -> `Филиал Третьяковской галереи, Парадная наб. 3, Калининград`.
  - `Русский центр искусства` / `РЦИ` / `Русский центр искусства, Рыбная деревня` -> `Русский центр искусства, Октябрьская 10, Калининград`.
- После этих узких алиасов Smart Update дополнительно сверяет `location_name/location_address` с `docs/reference/locations.md`.
  Если запись матчится на одну каноническую площадку, Smart Update подставляет reference `location_name`
  и использует город из справочника как авторитетный, даже когда extractor записал в `city` шумный токен вроде названия площадки.
- Reference-слой умеет также искать единственную известную площадку в свободном тексте по каноническому имени, алиасу или адресу; Telegram Monitoring использует это только как recovery после пустой или явно прозоподобной `location_name`, чтобы не закреплять фрагменты описания как площадку.
- Curated aliases in `docs/reference/location-aliases.md` force canonical `location_name/location_address/city` from `docs/reference/locations.md` when the alias matches exactly. This is intentionally a reference-normalization guardrail, not a semantic matcher: broad/ambiguous room labels such as bare `Кинозал` still need source context or the Gemma 4 venue-review stage.
- Для нормализации по `docs/reference/locations.md` действует guardrail: неизвестная площадка не должна схлопываться в известную запись по общему токену вроде `школа`; если в источнике есть явный конфликтующий адрес, сохраняем raw `location_name/location_address/city`, а не создаём гибрид из справочника и текста поста. Fuzzy address matching и duplicate recall сравнивают границы нормализованных токенов, включая полный номер дома: `Советский 1` не совпадает с `Советский 12`, но `Советский 1, 2 этаж` остаётся допустимым уточнением канонического `Советский 1` (`INC-2026-07-27-icae-casting-wrong-venue`).
- Географические слова `остров` и `озеро` также не являются identity-токенами площадки: `Верхнее озеро, остров Шайба` не может fuzzy-схлопнуться в `Остров Канта` только по слову `остров`.
- После reference-нормализации подозрительные social-source локации проходят отдельный LLM-first `location_grounding_review`. Детерминированный слой только маршрутизирует high-risk случаи; LLM обязан выбрать закрытое финальное действие `keep|repair|reject_missing_location`. Для `keep` и `repair` требуется дословная source/OCR quote и source-grounded имя либо адрес. Numeric confidence сохраняется как диагностика, но больше не отменяет валидный grounded `keep`: именно этот fallthrough зацикливал `wall-32547811_11187`. Если источник подтверждает только город/посёлок, название фестиваля/праздника или программу, LLM возвращает `reject_missing_location`, и candidate сразу получает typed product terminal без semantic retry. Malformed/provider/invalid-grounding response заканчивается `FAILED_TECHNICAL`, также без фонового повтора. После `llm_repair` reference-normalization может привести только source-grounded эквивалент; если она снова подставляет неподтверждённое имя, сохраняется LLM-repaired venue. Это предотвращает подмены парк `Юность` → одноимённый дворец спорта, `остров Шайба` → `Остров Канта`, `Советский 12` → `ИЦАЭ` и programme/occasion label → venue.
- Цель: стабильный dedup/merge и единообразный summary-блок Telegraph.
- Legacy eventness detectors in this layer are warning/verification routers only.
  Positive typed source children are never converted to `skipped_non_event`;
  complete contradictory cases receive one closed LLM decision; incomplete or
  technical cases end as visible `FAILED_TECHNICAL`.


## Summary-блок для выставок

- Для `event_type=выставка` в summary-блоке Telegraph используется формат периода:
  - `10-20 февраля`
  - `с 10 февраля по 28 марта`
  - `по 28 марта` (для уже идущей выставки).
- Если известно время работы, оно добавляется в ту же строку периода.

## Выставки через Smart Update

- Выставки (`event_type=выставка`) обрабатываются тем же Smart Update-пайплайном для `telegram`, `vk` и `/parse`.
- Действующая выставка с периодом обновляется в существующем событии **только если** новый источник действительно про ту же выставку
  (название/автор/тематика/афиша/ссылка/ключевые факты совпадают или явно связаны).
- Пересечение диапазонов дат **само по себе не означает дубль**: в одном музее/галерее может идти несколько выставок параллельно.
- Поэтому Smart Update **не делает** deterministic auto-match для длинных событий только по `date..end_date` + локации при несвязанных `title` —
  в сомнительных случаях используется LLM match/create (или создаётся новый ивент при отключённом LLM).
- Если для выставки в источнике есть только дата открытия (без даты закрытия), Smart Update ставит `end_date` по умолчанию как `date + 1 календарный месяц`.
- Для `ярмарка` такого fallback нет: без явной даты закрытия/периода ярмарка остаётся однодневной, потому что месячный inferred `end_date` даёт ложные ongoing-события и ломает подборки `/v`.
- Важно: посты вида **«Акция» / «билет действует только на указанную дату» / «только сегодня/завтра»** не должны превращаться в месячный период по умолчанию даже при ошибочной классификации в `выставка`.
- Продление `end_date` для выставки/ярмарки разрешено по trust; при более низком trust изменение фиксируется как конфликт и не применяется.
- Отображение в `/exhibitions` и секции `Постоянные выставки` на страницах месяца/выходных использует те же поля `event_type` и `end_date`, заполненные Smart Update.

## Отложенное обновление страниц (debounce)

После создания/обновления события запускается пайплайн задач (JobOutbox): Telegraph страница события, публикация ICS, синхронизация в VK и т.д.

Важно про устойчивость:
- JobOutbox выполняет каждый handler с ограничением по времени (per‑task max runtime: `JOB_MAX_RUNTIME`, иначе `DEFAULT_JOB_MAX_RUNTIME`).
  Если задача зависла на сети/провайдере или слишком долго ждёт внутренних семафоров, она помечается как `error` и будет повторена по backoff,
  не блокируя остальные операции (например, `/tg` импорт).

Чтобы не «долбить» Telegraph и не спамить уведомлениями при серии апдейтов (например Telegram Monitoring импортирует много событий подряд), **обновление навигационных страниц делается отложенно и накопительно**:

- `month_pages` и `weekend_pages` ставятся в JobOutbox с `next_run_at = now + 15 минут`.
- Ключ коалесинга:
  - `month_pages:YYYY-MM` (один job на месяц),
  - `weekend_pages:YYYY-MM-DD` (один job на конкретные выходные).
- При повторных изменениях в тот же период:
  - job не дублируется,
  - `next_run_at` «сдвигается вправо» (остается **15 минут после последнего изменения**).
- Страницы месяца/выходных пересобираются **только** debounced задачами JobOutbox; `telegraph_build` обновляет только страницу события.

Отключение debounce (для ручной отладки): `EVENT_UPDATE_SYNC=1` — тогда month/weekend страницы обновляются сразу, без ожидания 15 минут.

## Инциденты / риски

### OCR может ошибаться в названии локации

Инцидент: OCR (Gemma) ошибочно распознал «Калининградская» как «Каминская», что может увести `location_name` при импорте из Telegram.

Митигируемо:
- Для Telegram‑источников с `default_location` используется **проверка на конфликт**:
  - по умолчанию `default_location` остаётся сильным prior и раскладывается в структурные поля (`location_name/location_address/city`);
  - если extractor нашёл явно подтверждённую текстом off-site площадку/адрес, candidate сохраняет её вместо слепой подмены на `default_location`.
- В сценариях E2E есть проверка, что локация события из @kaliningradlibrary остаётся «Научная библиотека», даже если OCR даёт шум.

### Дальние даты и конфликт с афишей

Date/OCR mismatch — contradiction evidence for source verification. Smart Update
может записать diagnostic note, но не ставит accepted child в `event.silent=1` и
не отменяет его публикацию. Complete-evidence semantic ambiguity receives one closed LLM decision;
технически недоступная verification даёт `FAILED_TECHNICAL`.

### Free / Location / Duplicate Fail-Closed Guards

LLM остаётся владельцем смысловых решений, но Smart Update теперь ставит fail-closed инварианты поверх всех автоматических источников:

- `is_free=true` не выводится из `ticket_price_min=0`; free-флаг принимается только как явный LLM/source факт. Положительная цена, розыгрыш билетов или формулировка «входит во входной билет» снимают free-флаг.
- VK intake больше не делает библиотечные события бесплатными только по площадке: нужна явная формулировка вроде `вход свободный`/`бесплатно` или LLM `is_free=true`.
- rental/booking wording is a semantic hint; only the typed source decision may confirm a product exclusion.
- duplicate matching до LLM учитывает normalized specific ticket URL + same date/place даже при пустом времени и rewritten title, а также same date/time/place + near-identical source text без требования одинакового заголовка.
- Telegram `default_location` остаётся prior, но не подставляется вместо неподтверждённой off-site/prose локации: в event row попадает known/reference/text-grounded площадка или candidate fail-closed.

### LLM инциденты должны быть видны оператору

Требование: ошибки LLM (rate-limit, provider errors, missing Supabase RPC routes) должны быть видны не только superadmin, но и оператору, который инициировал действие через Telegram UI.

Реализация:
- инциденты отправляются в **чат оператора** (контекст текущего update) и дополнительно в superadmin;
- для scheduled/background задач без UI-контекста остаётся только superadmin.
- если Supabase RPC `google_ai_reserve` отсутствует, но включён fallback на прямой `GOOGLE_API_KEY`,
  инцидент маркируется как `warning` (а не `critical`), потому что обработка продолжается.

## Прозрачность источников (требование)

Чтобы оператор понимал, как сформирована карточка события:

1. **Telegraph footer** (в конец страницы события):
   - строка `Источников: N` (без перечисления самих источников);
   - строка `Последнее обновление: YYYY-MM-DD HH:MM (TZ)` — время последнего Smart Update (локальный TZ, Калининград).
   - после служебных строк выводится компактный редакционный подвал:
     `Полюбить Калининград`, `Telegram: Анонсы · Афиша`,
     `ВК: Анонсы · Афиша · канал Афиши`, `Max: Анонсы`. Ссылки ведут соответственно на
     `https://t.me/kenigevents`, `https://t.me/kldevents`,
     `https://vk.com/kenigeventsofficial`, `https://vk.com/klgdevents`,
     VK-канал Афиши `https://vk.ru/im/channels/-239844596` и
     Max `https://max.ru/channel_kenigevents`.
2. **Лог фактов (added_facts) по источникам**:
   - доступен из `/events -> Edit` через отдельную кнопку, а также шорткатом командой `/log <event_id>`;
   - формат — журнал с датой/временем, источником и фактами, что именно было добавлено/смёрджено и что было проигнорировано.
   - для пары `(event_id, source_url)` хранится один актуальный batch фактов: при повторной обработке того же поста факты источника заменяются, а не накапливаются отдельными историческими блоками.
   - у каждого факта есть `status` (хранится в `event_source_fact.status`) и человекочитаемая иконка:
     - `✅` — **added**: факт **новый для события (глобально)**, применён к событию и должен быть отражён на Telegraph странице;
     - `↩️` — **duplicate**: факт был в источнике, но уже был в событии, поэтому **не добавлен**;
     - `⚠️` — **conflict**: конфликт фактов или якорей. Конфликты по фактам выявляет LLM (Gemma) на основе сравнения нового источника с уже известными фактами;
       при противоречиях LLM выбирает версию по уровню trust (`candidate.trust_level` против максимального trust среди уже добавленных источников).
     - `ℹ️` — **note**: служебная заметка (фильтры/действия с афишами/технические пометки).
   - для афиш логирует конкретные URL (`Добавлена афиша: https://...`).
   - заметку `Текст дополнен: ...` больше не используем: изменения в тексте должны быть объяснимы через добавленные факты (`✅`) и дубли (`↩️`).
   - если лог содержит и `Афиша в источнике: ...`, и `Добавлена афиша: ...` с одним и тем же URL — отображаем/записываем только `Добавлена афиша` (без дублирования).
   - если одна и та же афиша попала в событие под разными URL (например, VK CDN и Catbox/Supabase), Telegraph-рендеринг должен показывать только один вариант (предпочитая Catbox/Supabase) — без визуальных дублей.
   - порядок афиш в `event.photo_urls` должен начинаться с **самой релевантной афиши** (по OCR совпадению с `title/date/time`), чтобы в Telegraph/превью первой была именно афиша события, а не “самая шумная” картинка из альбома.
   - Smart Update не должен подмешивать в `event.description` “сырые” фрагменты без LLM: если LLM недоступна, сохраняем прошлое описание и пишем `ℹ️` заметку в лог источников.

## Унифицированный отчёт Smart Update (UI)

Требование: после каждого создания/обновления события оператор должен видеть компактный блок, одинаковый по смыслу для разных источников (`/tg` мониторинг, `/parse`, ручной импорт).

Минимальный состав блока на событие:

- название (кликабельно ведёт на Telegraph, если ссылка уже создана) + `id` + дата/время
- `Источник: ...`
- `Источники:` (все ранее использованные источники события, компактно `DD.MM HH:MM <url>`)
- `Telegraph: ⏳ ...` / `Telegraph: ❌ ...` (только если ссылка ещё не готова или есть ошибка)
- `Лог: /log <id>`
- `ICS: ics` (короткая кликабельная ссылка, либо `—/⏳`)
- `Посты: VK пост/⏳ · TG пост/⏳` (кликабельные ссылки на управляемый `klgdevents` VK-анонс и Telegram event post, если они уже опубликованы; для VK рядом может быть пометка `соавторство: @... предложено`, когда publish-flow распознал известного источника-соавтора)
- `Факты: ✅N ↩️M ⚠️K ℹ️L` (сколько фактов добавлено/проигнорировано/в конфликте/служебных заметок на текущей итерации)
- `Иллюстрации:` и `Видео:` (где доступно: delta `+N` и `всего M`)

Также, если обнаружено: строки про добавление в очереди `festival_queue` и/или `ticket_site_queue`.

### Двухфазная проверка telegram-first → /parse (обязательная для E2E)

Чтобы отлавливать реальные регрессии мёржа, для сценариев `telegram-first` проверка должна идти в **двух этапах** и сохранять оба состояния:

1. После Telegram Monitoring (`telegram-first`):
   - зафиксировать snapshot карточки/лога источников/Telegraph страницы;
   - убедиться, что в telegram-секции лога есть не только служебные поля (`Дата/Время/Локация/Афиша`), но и минимум один **текстовый факт** события (`✅`).
2. После `/parse` (`after-parse`):
   - зафиксировать второй snapshot;
   - убедиться, что источников стало больше (прибавился `parser:<site>`);
   - убедиться, что текст Telegraph не “схлопнулся” по объёму относительно `telegram-first`.

Требования к качеству результата после мёржа:

- все смысловые факты из лога источников должны быть отражены на Telegraph странице события;
- в Telegraph тексте не должно быть строк расписания/названий других событий из multi-event поста;
- в `description` не дублировать логистику из инфоблока (дата/время/площадка/адрес/билеты/цена/контакты): это уже показывается сверху. Решается правилом в промпте рерайта/мёржа; safety-net — детектор + LLM-editor pass (best-effort). Детерминированное regex-«вырезание» логистики не используется, чтобы не ломать грамматику/смысл.
- в `description` и `Факты` не включать промо-упоминания «где следить за анонсами» (например ссылки на Telegram-канал афиши/анонсов), если это не относится к самому событию;
- текст должен оставаться структурированным (абзацы + форматирование), нейтрально-профессиональным по стилю;
- избегать нейросетевых клише и необоснованных оценок/прогнозов (например «обещает стать заметным событием»), если это не цитата из источника;
- для публичной проверки в Telegram у страницы события должен собираться web preview (`cached_page` + `photo`).
  - Важно: операторские отчёты (например `Smart Update (детали событий)`) обычно отправляются с `disable_web_page_preview=True`,
    поэтому сами по себе они **не прогревают** Telegram preview. Если нужно прогревать автоматически после публикации Telegraph,
    включите `TELEGRAPH_PREVIEW_WARMUP=1` (см. `.env.example`).

## /3di: инвалидация 3D-превью при изменении иллюстраций

Если у события уже есть `preview_3d_url`, но в процессе мерджа добавились/изменились иллюстрации (`photo_urls`), то 3D-превью становится устаревшим.

- Smart Update **сбрасывает** `preview_3d_url` (ставит `NULL`) при изменении набора иллюстраций.
- Такое событие попадает в список “🆕 Только новые” в `/3di` и будет пересобрано ближайшим scheduled запуском `/3di` (при условии `photo_count >= 2`).

## Acceptance (Gherkin)

Канонические сценарии:

- `tests/e2e/features/smart_event_update.feature` (пограничные кейсы матчинга/мерджа).
- `tests/e2e/features/telegram_monitoring.feature` (обогащение событий из Telegram Monitoring).

### 2026-06-30 incident guard: campaign actions and short prose locations

Smart Update must treat campaign/discount/action-shaped candidates as semantic high risk and route them to the LLM eventness reviewer before create. Examples include discount campaigns and Pushkin-card mechanics: a long validity period is not enough to make the source a concrete attendable event.

Operational date-role ambiguity is handled the same LLM-first way. A narrow deterministic detector may route source shapes such as visitor/cash-desk hours or `билет действителен до <date>` to eventness review, but it must not decide meaning itself. An explicit source-grounded LLM `non_event` verdict at confidence `>=0.90` closes immediately as typed `REJECTED_PRODUCT_POLICY/non_event`; lower-confidence, malformed, unavailable or `uncertain` output closes as visible `FAILED_TECHNICAL`, never as a product guess or background retry. Identity/vector `allow_create` is never a quality approval.

Historical anniversary/interview prose with several explicit old years is likewise only routed, never classified, by the deterministic layer. The LLM eventness reviewer must reject museum chronology (opening, acquisition, employment or memoir dates) unless the source separately announces a future attendee-facing programme; a real future anniversary lecture remains valid when its date and venue are explicit.

### 2026-07-12 occurrence-role and roundup scope gates

Social sources that mix a completed-event recap with a short future invitation
are reviewed before venue defaults, inferred exhibition duration, identity
vectors or writes. Deterministic past/future markers only route the candidate;
the LLM assigns occurrence roles. A future candidate whose attendee-facing
location is not present in the source fails closed, and exhibition words in a
past section cannot trigger the one-month duration fallback for the future
occurrence.

Multi-event roundups receive a separate LLM scope pass. It selects verbatim
event-local excerpts for the target title/date/time plus only genuinely shared
logistics. Rich-fact extraction, the match/create bundle and description writer
use that scoped evidence, while the complete source remains stored for
provenance. Non-verbatim output, a missing target date, uncertainty or provider
failure blocks automatic creation. This prevents performer/program facts from
sibling dates being written into `event_source_fact`, descriptions, search
digests and public projections.

The generated public bundle has a second source-grounding review. A `grounded`
decision is accepted only at confidence `>=0.9` with no unsupported fields. An
`uncertain` decision remains fail-closed. An explicit `ungrounded` decision does
not authorize replacement prose: the importer mechanically removes the fields
named by the reviewer and falls back to the already scoped candidate evidence.
If the reviewer omits its per-field diagnosis, all populated generated public
fields are removed; this is a conservative reduction, not a semantic guess.
Verbatim reviewer evidence is required before either outcome is trusted.

A VK roundup row is not atomic with its child Smart Update writes. If an early
child succeeds and a later child is rejected or fails, the successful event ids
remain linked in `vk_inbox_import_event`, while the carrier closes in the same
batch with the exact child outcomes and a visible `FAILED_TECHNICAL` reason for
any unresolved child. The row receives no `next_attempt_at` and is not silently
reprocessed by a later batch; committed child links are never discarded.

### Legacy retry drain visibility

The old `smart_update_retry_worker` is disabled by default and is permitted only
as a controlled one-time drain of pre-existing `RETRY_SCHEDULED` rows. After a replay has durably reached
`CREATED` or `MERGED`, the worker sends one bounded, HTML-escaped batch report
to the resolved superadmin chat with event ids, safe titles, and create/update
counts. Every claimed legacy row must finish accepted, product-rejected or
`FAILED_TECHNICAL`; no new scheduled retry is produced.

The report is observability only: a Telegram notification failure is logged but
must never roll back, reclassify, or replay the already accepted Smart Update
result. Interactive Telegram/VK callers keep their existing unified reports;
only the explicitly enabled legacy drain uses this callback, avoiding duplicate
success messages.

### Vector-first future quality audit contract

The future-event quality audit is separate from identity deduplication:

1. select every active canonical future event from Fly SQLite and assert exact id coverage;
2. retrieve/compute versioned claim vectors plus incident-prototype and nearest-event candidates;
3. give every row, its linked source bundle and vector-retrieved context to the LLM source-grounding verifier;
4. persist/report `pass | repair | remove | needs_review | indeterminate` without treating similarity as evidence or mutating production automatically;
5. fail closed when any catalog id, vector, source bundle or validated LLM verdict is missing.

Supabase pgvector remains a retrieval sidecar and may be stale; Fly SQLite remains canonical. A future enforce-mode publication gate must key a current quality decision to both the event hash and source-bundle hash, so any logistics/media/source change invalidates the pass before Telegram/VK/Telegraph/static fanout.

For a reproducible read-only vector-first catalog pass, freeze a compact JSON/JSON.GZ export with `events`, linked `event_source(s)` and `eventposter(s)`, then run:

```bash
python scripts/inspect/audit_future_event_vectors.py \
  --export artifacts/codex/<incident>/future-events.json.gz \
  --output artifacts/codex/<incident>/vector-audit \
  --env-file .env
```

The command reuses a `related_v1` sidecar vector only when its `text_hash` exactly matches the current identity document, fills stale/missing vectors ephemerally with bounded provider retries and without sidecar writes, and emits top-K `vector_pairs.json`. Similarity is recall only: every proposed duplicate still requires source/OCR-grounded LLM or human adjudication; the tool never mutates Fly SQLite or Supabase.

Short non-location fragments that arrive as `location_name` (for example `И не забывайте` or `В программе — ...`) are fail-closed safety issues: deterministic code may reject the field, but must not invent the semantic venue. Recovery must come from source-grounded defaults, explicit address/venue evidence, or an LLM-owned review stage.

### Static-site public projection gate

The production preview/static-site exporter has an independent public projection
gate before `preview-events.json` is written.  It is not a substitute for the
LLM-owned Smart Update create/merge decisions; it is a final fail-closed public
boundary.  The gate is applied to both the normal SQL slice and explicit
`--include-ids` / control ids.

Rows are suppressed when they are not canonical identities
(`identity_status != canonical`), have `merged_into_event_id`, carry
review/quarantine/rejected lifecycle or moderation statuses, have invalid ISO
`date`/`end_date`, or expose narrow prompt/code/prose leakage patterns in
`title`, `location_name`, `location_address`, or `city`.  Missing new columns on
old SQLite snapshots are treated as absent schema, so old DB rows still export
if their required public fields are valid.

Identity gate observability is persisted in `event_identity_decision_log` for
every enabled create-path gate invocation, including allow/retry/distinct
evidence and compact vector evidence. Newly created events also store
`date_provenance`, `date_confidence`, `date_is_inferred`,
`end_date_provenance`, and `end_date_confidence` derived from source/OCR
grounding. Immediately before inserting a new event row, Smart Update reruns a
cheap duplicate probe at the final write boundary. A fresh authoritative match
is reloaded and revalidated in the same facade operation: a confirmed duplicate
receives the keyed source packet and returns `MERGED`, while a stale/disproved
match proceeds as distinct `CREATED`. A missing authoritative row or transient
storage failure rolls back and returns visible `FAILED_TECHNICAL`; the probe
never emits a veto/review terminal and does not add another LLM pass.

The `/vystavki/` enforce rollout is monitored independently from page rendering
with `scripts/inspect/audit_public_exhibition_duplicates.py`.  It scans the
canonical SQLite event inventory read-only and emits JSON/Prometheus acceptance
metrics, including
`events_public_exhibition_duplicate_pairs_since_total{confidence="high",window_days="14"}`.

### Sparse-source and fact evidence contract (2026-07-14)

Public facts extracted from a candidate, including facts returned by the merge stage, must be
LLM-selected objects `{fact, evidence_quote}`. `evidence_quote` is an exact contiguous fragment
of that candidate's text/OCR and must directly support the complete fact. A narrow verifier may
reject an invalid contract, but it does not rewrite or infer meaning. In particular, the model
must not infer an event's goal, format, benefits, regularity, or “continuation of a series” from
the project name or topic.

Thin teasers are allowed to yield only one to three facts. The fact-first writer then produces
one or two short paragraphs without forced headings or filler; sparsity is an honest state, not
a prompt to invent detail. Later concrete organizer sources can enrich the same event through
the normal LLM merge. Managed VK publication URLs are output projections and are excluded from
legacy `event_source` backfill, preventing published AI copy from becoming circular evidence.

### Возраст события

Source-native `age_restriction` сохраняется без LLM. Для text/OCR строгий
`age_decision` piggybacked в уже существующий facts/create/merge вызов, поэтому
feature не увеличивает число provider requests на событие. Declared и assessed
разделены; конфликт fail-closed, публичный default — declared-only. Полная
каноника, CPU Kaggle/BGE gate и backfill: [Event age rating](../event-age-rating/README.md).

### Collection facts for static selections (facts v3)

Static selections do not make admission, target-audience or visiting-person
decisions in Astro/BGE. Smart Update owns a nullable, independently merged
`Event.collection_decisions` container with independently grounded facts:

- `admission_decision.value = confirmed_free|confirmed_paid|unknown`;
- `child_directed_decision.value = confirmed|denied|unknown`;
- `family_suitable_decision.value = confirmed|denied|unknown`;
- `joint_family_activity_decision.value = confirmed|denied|unknown`;
- `people_appearances[]` with explicit role, `confirmed|mentioned|unknown` and
  separately evidenced origin scope.

The versioned contracts are `static-collection-facts-v3` and
`static-collection-adjudication-v2`. The three audience facts are returned by
the existing single short `collection_candidate_adjudication` JSON request;
there is no second audience request and no expansion of create/merge writers.
The primary remains `gemma-4-31b-it`. This label uses one native-schema
physical send with Google model fallback/retry disabled per call, followed by
at most the existing one GPT-4o fallback send. Gemini Lite is not a fallback
for this label. Trace records expose physical sends, actual model path and
available token usage.

Routing remains high recall: topics/BGE, existing legacy/v3 facts and narrow
audience phrases can request the one evaluation. Ticket availability and an
age rating alone do not route. None of these recall signals is evidence.
Every `confirmed` or `denied` v3 fact needs an exact continuous source quote;
`denied` additionally needs explicit negative wording. Missing proof is
`unknown`. Child authorship, a family theme/atmosphere, parents-only copy and a
bare “семейный турнир” do not prove the corresponding positive fact. A
confirmed joint activity is rejected unless both child-directed and
family-suitable are independently confirmed.

Accepted source attachment and decision application are atomic. JSON is deep
merged per decision and reassigned as a whole for SQLAlchemy persistence; manual
lock wins, then official/high/medium/low source trust and recency decide each
key independently. At apply time v3 quotes are rechecked against the persisted
same-event `EventSource.source_text`, not candidate OCR or `raw_excerpt`.
`unknown` never removes accepted truth.

A bounded `evaluation_receipts` cache is persisted by `(source_id,input_hash)`.
It retains validated all-unknown evaluations too, allowing normal Smart Update
and the bounded backfill to skip provider calls and writes on an identical warm
replay. The input hash includes policy/schema, so a policy bump invalidates
coverage. The legacy `audience_decision` is a deterministic compatibility
projection (`family`, then `kids`, explicit source-proven adults-only `none`)
marked `derived_from_facts_v3`; it is never produced by a second LLM request.

`Event.is_free` remains the compatible materialized bool and changes only from
`confirmed_free|confirmed_paid`; exporter code must not infer it from prose
`ticket_status`. Reason-filtered audience-only apply cannot change admission,
people or `is_free`; callers without a filter retain the existing all-reasons
behavior. The additive DB schema remains `20260801_static_collection_facts`. A bounded operational
backfill is implemented in `scripts/backfill_static_collection_facts.py`: plan
mode is read-only; `--apply` reuses persisted `EventSource`, the same strict
adjudicator and atomic apply contract, prefers trusted/recent source evidence,
is hash-resumable, and can be limited by reason/event/source count. It does not
re-run Smart Update identity/writer logic or scan the whole archive. Production
execution is gated by the bounded operator runbook. The 2026-08-02 primary-only
real replay stopped at Gate B: transport/call-count/quote checks passed, but
quality thresholds and several provisional review rows did not. Therefore no
copy apply, ingestion replay, Fly canary or publication is claimed. See the
[operator runbook](../../operations/static-collection-facts-v3.md),
[integration report](../../../.codex/integration/static-collection-facts-v3-INTEGRATION_REPORT.md)
and product/extraction contract
[`podborki-to-be.md`](../static-site-pages/podborki-to-be.md).

### Interest-club relation handoff

Interest-club matching is a downstream identity relation, not part of the
Smart Update match/create/merge verdict and never rewrites canonical event
fields.  A completed canonical event change may enqueue/coalesce a bounded
club candidate evaluation.  Deterministic anchors only retrieve or fail-close;
positive relation requires the feature's grounded Gemma verdict plus exact
quote.  `no`, `unclear`, invalid quote, quota/provider failure and missing
anchor do not create a relation and do not use Lite as positive fallback.
Linked occurrences and festival identity remain owned by their existing
features.  Full contract and separate production gate:
[Interest clubs](../interest-clubs/README.md) and
[release plan](../interest-clubs/release-plan.md).
